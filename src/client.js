const mqttConn = require('mqtt-connection');
const jwt = require('./auth/jwt');
const events = require('events');
const { v4: uuidv4 } = require('uuid');
const MQTT = require('./const');
const globalEvents = new events.EventEmitter();
require('src/types.js');

function noop() {}

/**
 *
 * @param stream
 * @param broker
 */
const connection = function (stream, broker) {
  /**
   *
   * @type {Connection|*}
   * @function client.connack
   * @function client.puback
   * @function client.pingresp
   * @function client.suback
   */
  const client = mqttConn(stream, {
    protocolVersion: 5,
    properties: {
      maximumPacketSize: 1024 * 1024
    }
  });

  const streamTimeoutDelay = 1000 * 60 * 5;
  const clientEvents = new events.EventEmitter();
  const logger = broker.log;

  let metaData = {
    authMeta: false,
    authFailed: false,
    will: null,
    userName: '',
    clientId: '',
    state: 'init',
    uuid: uuidv4(),
    connack: null,
    timeTail: 0
  };

  let currentMessageId = 0;
  let messagesInUse = [];
  let messageRetryTimer = [];
  let clientSubscriptions = [];
  let messageTimeTracker = [];
  let streamTimer = null;

  const getNewMessageId = function () {
    let max = 0xffff;
    while (true) {
      if (++currentMessageId > 0xffff) {
        currentMessageId = 1;
      }
      if (!messagesInUse.find((element) => element === currentMessageId)) {
        messagesInUse.push(currentMessageId);
        return currentMessageId;
      }
      if (max-- === 0) {
        logger.log('debug', 'MQTT: getNewMessageId failure');
        return 0;
      }
    }
  };

  const sessionRefresh = async function () {
    if (streamTimer) {
      clearTimeout(streamTimer);
      streamTimer = setTimeout(streamTimeout, streamTimeoutDelay);
    }
    await broker.persist.refreshSession(metaData.clientId);
  };
  const waitAuth = async function () {
    if (metaData.authMeta !== false && metaData.authFailed === false) {
      return true;
    }
    return new Promise((resolve) => {
      clientEvents.on('auth', function (data) {
        resolve(data);
      });
    });
  };

  const clientPublishPacket = async function (packet, cb) {
    if (metaData.state === 'closed') {
      return;
    }
    if (packet.messageId === -1 && packet.qos === 1) {
      let pktCpy = Object.assign({}, packet);

      pktCpy.messageId = getNewMessageId();
      const results = await broker.persist.queueMessage(
          metaData.clientId,
          pktCpy
      );
      if (results.replaced !== -1) {
        closePendingRetry(results.replaced);
      }
      pktCpy.time = results.time;
      return await clientPublishPacket(pktCpy, cb);
    }
    /* NOTE: If we did retransmissions over tcp, we would do this here
             However MQTT5 allows us to only retransmit on reconnects.  */
    client.publish(packet, function () {
      cb();
    });
  };

  const addSubscription = function (topic) {
    if (clientSubscriptions[topic]) {
      return;
    }
    clientSubscriptions[topic] = clientPublishPacket;
    broker.mq.on(topic, clientPublishPacket);
  };

  const removeSubscription = function (topic) {
    delete clientSubscriptions[topic];
    broker.mq.removeListener(topic, clientPublishPacket);
  };

  const processOutboundQueue = async function () {
    const packets = await broker.persist.retrieveMessages(
        metaData.clientId,
        metaData.timeTail
    );
    // pre-process and sort
    for (let i = 0; i < packets.global.length; i++) {
      packets.global[i].messageId = -1; //getNewMessageId();
      packets.queued.push(packets.global[i]);
    }

    const deduplicated = [];
    for (let i = 0; i < packets.queued.length; i++) {
      const pkt = packets.queued[i];
      if (deduplicated[pkt.topic]) {
        if (pkt.time > deduplicated[pkt.topic].time) {
          deduplicated[pkt.topic] = pkt;
        }
      } else {
        deduplicated[pkt.topic] = pkt;
      }
    }

    const out = [];
    const keys = Object.keys(deduplicated);
    for (let i = 0; i < keys.length; i++) {
      out[i] = deduplicated[keys[i]];
    }

    // Sort, primarily for message time tracking order
    out.sort(function (a, b) {
      return a.time - b.time;
    });

    for (let i = 0; i < out.length; i++) {
      await clientPublishPacket(out[i], noop);
    }
  };

  const closeClient = async function () {
    if (metaData.state !== 'closed') {
      if (streamTimer) {
        clearTimeout(streamTimer);
      }
      metaData.state = 'closed';
      clientEvents.removeAllListeners('auth');
      globalEvents.removeAllListeners(`new-${metaData.clientId}`);
      if (metaData.authMeta !== false && metaData.clientId !== '') {
        broker.closeClient(metaData.clientId);
        broker.persist.saveClosingInfo(
            metaData.clientId,
            currentMessageId,
            Date.now()
        );
      }
      const topics = Object.keys(clientSubscriptions);
      for (let i = 0; i < topics.length; i++) {
        removeSubscription(topics[i]);
      }

      const retries = Object.keys(messageRetryTimer);
      for (let i = 0; i < retries.length; i++) {
        clearTimeout(messageRetryTimer[retries[i]].timer);
        delete messageRetryTimer[retries[i]];
      }
      // TODO different handling for Aerlink devices
      if (metaData.authMeta !== false) {
        if (broker.clientCallbacks) {
          broker.clientCallbacks.forEach((callback) => {
            const topic = callback.topicBuilder(metaData.authMeta);
            callback.
            broker.mq.removeListener(
                topic,
                callback.handler,
                noop);
          });
        }
        logger.log(
            'debug',
            `MQTT [${metaData.clientId}:${metaData.uuid.substr(0, 8)}] Closed`
        );
      }
      client.destroy();
      metaData.authMeta = false;
    }
  };

  const closePendingRetry = function (messageId) {
    let timeTail = messageTimeTracker[messageId]
        ? messageTimeTracker[messageId]
        : metaData.timeTail;
    delete messageTimeTracker[messageId];
    const older = messageTimeTracker.find((element) => element < timeTail);
    if (older) {
      timeTail = older;
    }

    const index = messagesInUse.findIndex((element) => element === messageId);
    if (index > -1) {
      messagesInUse.splice(index, 1);
    }
    if (messageRetryTimer[messageId]) {
      clearTimeout(messageRetryTimer[messageId].timer);
      delete messageRetryTimer[messageId];
    }
    return timeTail;
  };

  client.on('connect', async function (packet) {
    // Prevent duplicate connections, FIFO
    if ((await broker.persist.setOnline(packet.clientId, true)) === false) {
      globalEvents.emit(`new-${packet.clientId}`, true);
      logger.log('debug', `MQTT [${packet.clientId}] Preempted-previous`);
      //Wait for orphaned connections to die
      for (let i = 0; i < 5; i++) {
        if ((await broker.persist.setOnline(packet.clientId, true)) === true) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
        if (i === 4) {
          logger.log(
              'debug',
              `MQTT [${packet.clientId}] Unable to close previous session`
          );
          client.destroy();
          return;
        }
      }
    }
    globalEvents.on(`new-${packet.clientId}`, function () {
      closeClient().then();
    });
    // Re-send acknowledge
    // TODO Make sure this doesn't need db
    if (
        broker.clientList[packet.clientId] &&
        broker.clientList[packet.clientId] === metaData.uuid
    ) {
      logger.log('debug', `MQTT [${packet.clientId}] already ack-ed`);
      client.connack(metaData.connack);
      return;
    }

    if (packet.protocolVersion !== 5) {
      logger.log(
          'info',
          `MQTT [${packet.clientId}] not MQTT 5.0 (${packet.protocolVersion})`
      );
      await broker.persist.setOnline(packet.clientId, false);
      client.destroy();
      return;
    }
    let authMeta;
    try {
      authMeta = jwt.verifyToken(packet.password.toString());
    } catch (error) {
      authMeta = false;
      logger.log('debug', `MQTT [${packet.clientId}]`, packet);
    }

    if (authMeta !== false) {
      try {
        //Fill metaData
        authMeta.farms = await Users.getInstallerAccess(authMeta.user_id);
        metaData.authMeta = authMeta;
        metaData.userName = packet.username;
        metaData.clientId = packet.clientId;
        broker.addClient(packet.clientId, metaData.clientId);

        if (packet.properties && packet.properties.sessionExpiryInterval) {
          metaData.sessionExpiryInterval =
              packet.properties.sessionExpiryInterval;
        } else {
          metaData.sessionExpiryInterval = 60000;
        }
        metaData.keepalive = packet.keepalive;
        metaData.clean = packet.clean;
        logger.log(
            'info',
            `MQTT [${metaData.clientId}:${metaData.uuid.substr(0, 8)}] Authorized`
        );

        /** @type {Object | boolean} sessionData */
        let sessionData = false;
        let clientSubscriptions = [];
        if (!packet.clean) {
          // TODO restore session when requested
          sessionData = await broker.persist.findSession(packet.clientId);
          if (sessionData !== false) {
            metaData.timeTail = sessionData.timeTail;
            clientSubscriptions = sessionData.subscriptions;
            currentMessageId = isNaN(sessionData.messageId)
                ? 1
                : sessionData.messageId;
            messagesInUse = sessionData.existingMessageIds;
          } else {
            await broker.persist.startSession(packet.clientId);
          }
        } else {
          await broker.persist.cleanSession(packet.clientId);
          await broker.persist.startSession(packet.clientId);
        }
        metaData.connack = {
          reasonCode: MQTT.SUCCESS,
          sessionPresent: sessionData !== false,
          protocolVersion: 5,
          properties: {
            sessionExpiryInterval: 3600,
            maximumQoS: MQTT.QOS_1,
            assignedClientIdentifier: metaData.uuid,
            userProperties: {
              subscriptions: JSON.stringify(clientSubscriptions)
            }
          }
        };

        if (broker.clientCallbacks) {
          broker.clientCallbacks.forEach((callback) => {
            const topic = callback.topicBuilder(metaData.authMeta);
            broker.mq.on(
                topic,
                callback.handler,
                noop);
          });
        }

        clientEvents.emit('auth', true);
        metaData.state = 'open';
        client.connack(metaData.connack);
        // sessionRefresh().then();
        if (sessionData !== false) {
          for (let i = 0; i < sessionData.subscriptions.length; i++) {
            addSubscription(
                sessionData.subscriptions[i].topic,
                sessionData.subscriptions[i].qos
            );
          }
          await processOutboundQueue();
        }
      } catch (error) {
        clientEvents.emit('auth', false);
        logger.log('error', 'client.on(connect) error', error);
        await broker.persist.setOnline(packet.clientId, false);
        client.connack({ reasonCode: MQTT.SERVER_UNAVAILABLE }, function () {
          closeClient();
        });
      }
    } else {
      clientEvents.emit('auth', false);
      metaData.authFailed = true;
      logger.log('info', `MQTT [${packet.clientId}] Denied`);
      client.connack({ reasonCode: MQTT.BAD_USER_PASS }, function () {
        client.destroy();
      });
      await broker.persist.setOnline(packet.clientId, false);
    }
  });

  // client --> broker published
  client.on('publish', async function (packet) {
    if (metaData.authMeta === false) {
      const result = await waitAuth();
      if (result === false) {
        logger.log('debug', 'auth failure @ publish');
        return;
      }
    }
    sessionRefresh().then();
    const messageId = packet.messageId ? packet.messageId : 0;
    const publ = broker.findPublication(packet.topic);
    if (publ.handler) {
      const access = await publ.handler.cb.access(
          publ.params,
          metaData.authMeta
      );
      if (
          access &&
          packet.qos > 0 &&
          packet.dup &&
          broker.persist.getPublishHandled(
              metaData.clientId,
              publ.index,
              messageId
          )
      ) {
        // Repeat pub-ack, but don't do anything else, we already processed this publish
        client.puback({
          messageId,
          reasonCode: MQTT.SUCCESS,
          properties: {
            reasonString: 'duplicate'
          }
        });
      } else if (access) {
        const pubs = await publ.handler.cb.publish(
            publ.params,
            metaData.authMeta,
            packet.payload
        );
        await broker.persist.setPublishHandled(
            metaData.clientId,
            publ.index,
            messageId
        );
        // Todo review return value protocol
        if (pubs === false) {
          client.publish(
              {
                topic: 'lastError',
                payload: 'Failed write to ' + packet.topic
              },
              noop
          );
          if (packet.qos > 0) {
            client.puback({
              messageId,
              reasonCode: MQTT.PAYLOAD_INVALID
            });
          }
        } else {
          if (pubs.client) {
            for (let i = 0; i < pubs.client.length; i++) {
              pubs.client[i].qos = MQTT.QOS_1;
              pubs.client[i].messageId = -1; //Individual client flag to create messageId
              await clientPublishPacket(pubs.client[i], noop);
            }
          }
          if (pubs.all) {
            for (let i = 0; i < pubs.all.length; i++) {
              // FIXME, testing
              pubs.all[i].qos = MQTT.QOS_1;
              if (pubs.all[i].subscription) {
                await broker.persist.addAllMessage(
                    pubs.all[i],
                    pubs.all[i].subscription
                );
              } else {
                await broker.persist.addAllMessage(
                    pubs.all[i],
                    pubs.subscription
                );
              }
              // Send initial publish
              pubs.all[i].messageId = -1; //Individual client flag to create messageId
              broker.publish(pubs.all[i]);
            }
          }
          if (packet.qos > 0) {
            if (pubs.puback) {
              pubs.puback.messageId = messageId;
              client.puback(pubs.puback);
            } else {
              client.puback({
                messageId,
                reasonCode: MQTT.SUCCESS
              });
            }
          }
        }
      } else {
        // TODO: remove publish in production
        client.publish(
            {
              topic: 'lastError',
              payload: 'Not authorized @ ' + packet.topic
            },
            noop
        );
        if (packet.qos > 0) {
          client.puback({
            messageId,
            reasonCode: MQTT.NOT_AUTHORIZED,
            properties: {
              reasonString: `User not authorized for "${packet.topic}"`,
              userProperties: {
                error: `User not authorized for "${packet.topic}"`
              }
            }
          });
        }
      }
    } else {
      // TODO: remove publish in production
      client.publish(
          {
            topic: 'lastError',
            payload: 'Topic not found ' + packet.topic
          },
          noop
      );
      if (packet.qos > 0) {
        client.puback({
          messageId,
          reasonCode: MQTT.TOPIC_INVALID,
          properties: {
            reasonString: `Topic "${packet.topic}" not found`,
            userProperties: {
              error: `Topic "${packet.topic}" not found`
            }
          }
        });
      }
    }
  });

  client.on('pingreq', function () {
    sessionRefresh().then();
    client.pingresp();
  });

  client.on('puback', async function (packet) {
    /*
    Packet {
      cmd: 'puback',
        retain: false,
        qos: 0,
        dup: false,
        length: 3,
        topic: null,
        payload: null,
        messageId: 41,
        reasonCode: 0
    } */
    metaData.timeTail = closePendingRetry(packet.messageId);
    // console.log(`${metaData.clientId} puback  ${packet.messageId}  TT ${metaData.timeTail}`);
    await broker.persist.deleteMessage(
        metaData.clientId,
        packet.messageId,
        metaData.timeTail
    );
  });

  client.on('unsubscribe', async function (packet) {
    // TODO complete this section
    // console.log(metaData.clientId, '@unsubscribe', packet);
    if (metaData.authMeta === false) {
      const result = await waitAuth();
      if (result === false) {
        logger.log('debug', 'auth failure');
        return;
      }
    }
    sessionRefresh().then();
    if (!packet.unsubscriptions || packet.unsubscriptions.length === 0) {
      client.unsuback({
        reasonCode: MQTT.PAYLOAD_INVALID,
        messageId: packet.messageId
      });
      return;
    }
    const granted = [];

    for (let i = 0; i < packet.unsubscriptions.length; i++) {
      const unSub = packet.unsubscriptions[i];
      const subFind = broker.findSubscription(unSub);
      if (subFind.handler) {
        granted[i] = MQTT.SUCCESS;
        broker.persist.removeSubscription(metaData.clientId, unSub);
        removeSubscription(unSub);
      } else {
        granted[i] = MQTT.NO_SUBSCRIPTION;
      }
    }

    client.unsuback({
      reasonCode: MQTT.SUCCESS,
      messageId: packet.messageId,
      granted: granted
    });
  });

  // client subscribed
  client.on('subscribe', async function (packet) {
    if (metaData.authMeta === false) {
      const result = await waitAuth();
      if (result === false) {
        logger.log('debug', 'auth failure');
        return;
      }
    }
    sessionRefresh().then();
    if (!packet.subscriptions || packet.subscriptions.length === 0) {
      client.suback({
        reasonCode: MQTT.PAYLOAD_INVALID,
        messageId: packet.messageId
      });
      return;
    }
    // Topic matching and authorization
    const granted = [];
    let success = true;
    for (let i = 0; i < packet.subscriptions.length; i++) {
      const sub = packet.subscriptions[i];
      sub.params = {};
      const subFind = broker.findSubscription(sub.topic);
      if (subFind.handler) {
        sub.params = subFind.params;
        const result = await subFind.handler.cb.access(
            subFind.params,
            metaData.authMeta
        );
        if (result) {
          if (packet.subscriptions[i].qos > MQTT.QOS_1) {
            granted[i] = MQTT.QOS_NOT_SUPPORTED;
            success = false;
          } else {
            granted[i] = packet.subscriptions[i].qos;
          }
        } else {
          granted[i] = MQTT.NOT_AUTHORIZED;
          success = false;
        }
      } else {
        granted[i] = MQTT.TOPIC_INVALID;
        success = false;
      }
    }

    // Queue to send out initial subscriptions

    if (success) {
      for (let i = 0; i < packet.subscriptions.length; i++) {
        const sub = packet.subscriptions[i];
        broker.persist.addSubscription(
            metaData.clientId,
            sub.topic,
            granted[i]
        );
        addSubscription(sub.topic);

        const fetchers = broker.retrieveFetchers(sub.topic);
        for (let f = 0; f < fetchers.length; f++) {
          const fetcher = fetchers[f];
          // console.log('FOUND matching fetcher', fetcher.matcher);
          try {
            /** @type {Object []} pkts*/
            const result = await fetcher.cb.load(sub.params, metaData.authMeta);
            const pkts = result.packets;
            for (let p = 0; p < pkts.length; p++) {
              const pkt = pkts[p];
              pkt.messageId = -1;
              pkt.qos = sub.qos;
              // await clientPublishPacket(pkt, noop);
              clientPublishPacket(pkt, noop).then();
            }
          } catch (error) {
            logger.log('error', `Error @ subscribe`, error);
          }
        }
      }
    }

    client.suback({
      granted: granted,
      messageId: packet.messageId
    });
  });

  client.on('disconnect', async function (packet) {
    logger.log('debug', `MQTT [${metaData.clientId}] Req disconnect`);
    closeClient().then();
  });
  // connection error handling
  client.on('close', function () {
    closeClient().then();
  });
  client.on('error', function (error) {
    const ignored = ['EPIPE', 'ECONNRESET', 'ECONNABORTED'];
    if (!ignored.find((element) => element === error.code)) {
      logger.log('info', `MQTT [${metaData.clientId}] Error`, error);
    } else {
      logger.log(
          'info',
          `MQTT [${metaData.clientId}] pipe failed ${error.code}`
      );
    }
    if (metaData.state !== 'closed') {
      closeClient().then();
    }
  });

  const streamTimeout = function () {
    logger.log('info', `MQTT [${metaData.clientId}] Timed out`);
    closeClient().then();
  };

  // stream timeout
  stream.on('timeout', function () {
    streamTimeout();
  });

  // timeout idle streams after 5 minutes
  if (stream.setTimeout) {
    stream.setTimeout(streamTimeoutDelay);
  } else {
    streamTimer = setTimeout(streamTimeout, streamTimeoutDelay);
  }
  //
};

module.exports = connection;

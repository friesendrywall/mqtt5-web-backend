const mqttConn = require('mqtt-connection');
const events = require('events');
const { v4: uuidv4 } = require('uuid');
const MQTT = require('./const');
const globalEvents = new events.EventEmitter();
require('./typedefs.js');

function noop() {}

const UNASSIGNED_MSG_ID = -1;

/**
 * @typedef client_opt_t
 * @type object
 * @property {boolean} deduplicate
 */

/**
 *
 * @param stream
 * @param broker
 * @param {object | undefined} opt
 */
const connection = function (stream, broker, opt = undefined) {
  /**
   *
   * @type {Connection|*}
   * @property {function} connack
   * @property {function} puback
   * @property {function} unsuback
   * @property {function} pingresp
   * @property {function} suback
   */
  const client = mqttConn(stream, {
    protocolVersion: 5,
    properties: {
      maximumPacketSize: 1024 * 1024
    }
  });

  const streamTimeoutDelay = 1000 * 60 * 5;
  const clientEvents = new events.EventEmitter();
  const log = broker.log;

  let metaData = {
    authMeta: false,
    authFailed: false,
    will: null,
    userName: '',
    clientId: '',
    state: 'init',
    uuid: uuidv4(null, null, null),
    connack: null
  };

  let currentMessageId = 0;
  let clientSubscriptions = [];
  let callbackTopics = [];
  let streamTimer = null;

  const getNewMessageId = function () {
    if (++currentMessageId > 0xffff) {
      currentMessageId = 1;
    }
    return currentMessageId;
  };

  const sessionRefresh = async function () {
    if (metaData.state === 'closed') {
      return;
    }
    if (streamTimer) {
      clearTimeout(streamTimer);
      streamTimer = setTimeout(streamTimeout, streamTimeoutDelay);
    }
    broker.persist.refreshSession(metaData.clientId);
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
    if (packet.messageId === UNASSIGNED_MSG_ID) {
      let pktCpy = Object.assign({}, packet);

      pktCpy.messageId = getNewMessageId();
      if (pktCpy.qos === MQTT.QOS_1) {
        broker.persist.updateMessageId(metaData.clientId, pktCpy);
      }
      return await clientPublishPacket(pktCpy, cb);
    }
    /* NOTE: If we did retransmissions over tcp, we would do this here
             However MQTT5 allows us to only retransmit on reconnects.  */
    client.publish(packet, function () {
      cb();
    });
  };

  const bridgePublishPacket = async function (packet, cb) {
    const pktCpy = Object.assign({}, packet);
    if (pktCpy.qos > 0) {
      pktCpy.messageId = UNASSIGNED_MSG_ID;
    }
    cb();
    clientPublishPacket(pktCpy, noop).then();
  };
  const addSubscription = function (topic) {
    if (clientSubscriptions[topic]) {
      return;
    }
    const handler = broker.findSubscription(topic).handler;
    if (handler == null) {
      return;
    }
    clientSubscriptions[topic] = clientPublishPacket;

    if (handler && handler.cb && handler.cb.bridge) {
      broker.aedes_handle.subscribe(topic, bridgePublishPacket, noop);
      // Duplicate listener for internal repeat channels
      broker.mq.on(topic, clientPublishPacket);
      /* We have to manually find and send persisted items */
      const persistence = broker.aedes_handle.persistence;
      const stream = persistence.createRetainedStream(topic);
      stream.on('data', (packet) => {
        bridgePublishPacket(packet, noop).then();
      });
    } else {
      broker.mq.on(topic, clientPublishPacket);
    }
  };

  const removeSubscription = function (topic) {
    delete clientSubscriptions[topic];
    const handler = broker.findSubscription(topic).handler;
    if (handler && handler.cb && handler.cb.bridge) {
      broker.aedes_handle.unsubscribe(topic, bridgePublishPacket, noop);
      // Duplicate listener for internal repeat channels
      broker.mq.removeListener(topic, clientPublishPacket);
    } else {
      broker.mq.removeListener(topic, clientPublishPacket);
    }
  };

  const processOutboundQueue = async function () {
    const packets = await broker.persist.getOfflineMessages(metaData.clientId);

    if (opt && opt.deduplicate) {
      const topicExists = [];
      for (let i = packets.length - 1; i >= 0; i--) {
        if (topicExists[packets[i].topic]) {
          // Delete duplicated packet
          packets.splice(i, 1);
          broker.persist.deleteMessage(metaData.clientId, packets[i]);
        } else {
          // Save newest packet
          topicExists[packets[i].topic] = true;
        }
      }
    }

    for (let i = 0; i < packets.length; i++) {
      if(!packets[i].messageId){
        packets[i].messageId = UNASSIGNED_MSG_ID;
      }
      await clientPublishPacket(packets[i], noop);
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
        broker.persist.saveSession(
            metaData.clientId,
            currentMessageId
        );
      }
      const topics = Object.keys(clientSubscriptions);
      for (let i = 0; i < topics.length; i++) {
        removeSubscription(topics[i]);
      }
      broker.persist.setOnline(metaData.clientId, false);
      if (metaData.authMeta !== false) {
        if (broker.clientCallbacks) {
          broker.clientCallbacks.forEach((callback) => {
            const topic = callback.topicBuilder(metaData.authMeta);
            delete callbackTopics[topic];
            broker.mq.removeListener(
                topic,
                clientCallbackHandler,
                noop);
          });
        }
        log(
            'debug',
            `MQTT [${metaData.clientId}:${metaData.uuid.substring(0, 8)}] Closed`
        );
      }
      client.destroy();
      metaData.authMeta = false;
    }
  };

  const clientCallbackHandler = function (packet, cb) {
    if (callbackTopics[packet.topic] && callbackTopics[packet.topic].handler) {
      callbackTopics[packet.topic].handler(packet, metaData.authMeta, cb);
    } else {
      log('debug', `MQTT [${metaData.clientId}] Unhandled clientCallback ${packet.topic}`);
      cb();
    }
  }

  client.on('connect', async function (packet) {
    // Prevent duplicate connections, FIFO
    if ((broker.persist.setOnline(packet.clientId, true)) === false) {
      globalEvents.emit(`new-${packet.clientId}`, true);
      log('debug', `MQTT [${packet.clientId}] Preempted-previous`);
      //Wait for orphaned connections to die
      for (let i = 0; i < 5; i++) {
        if ((broker.persist.setOnline(packet.clientId, true)) === true) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
        if (i === 4) {
          log(
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
    if (
        broker.clientList[packet.clientId] &&
        broker.clientList[packet.clientId] === metaData.uuid
    ) {
      log('debug', `MQTT [${packet.clientId}] already ack-ed`);
      client.connack(metaData.connack);
      return;
    }

    if (packet.protocolVersion !== 5) {
      log(
          'info',
          `MQTT [${packet.clientId}] not MQTT 5.0 (${packet.protocolVersion})`
      );
      await broker.persist.setOnline(packet.clientId, false);
      client.destroy();
      return;
    }
    let authMeta;
    try {
      if (broker.auth_func) {
        authMeta = broker.auth_func(packet.username, packet.password.toString());
      } else {
        authMeta = false;
        console.error('No auth_func defined');
      }
      if (broker.metadata_func) {
        await broker.metadata_func(authMeta);
      }
    } catch (error) {
      authMeta = false;
      log('debug', `MQTT [${packet.clientId}]`, packet);
    }

    if (authMeta !== false) {
      try {
        //Fill metaData
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
        log(
            'info',
            `MQTT [${metaData.clientId}:${metaData.uuid.substring(0, 8)}] Authorized`
        );

        /** @type {session_data_t | boolean} sessionData */
        let sessionData = false;
        let clientSubscriptions = [];
        if (!packet.clean) {
          sessionData = await broker.persist.findSession(packet.clientId);
          if (sessionData !== false) {
            clientSubscriptions = sessionData.subs;
            currentMessageId = isNaN(sessionData.messageId)
                ? 1
                : sessionData.messageId;
          } else {
            broker.persist.startSession(packet.clientId);
          }
        } else {
          broker.persist.deleteSessions([packet.clientId]);
          broker.persist.startSession(packet.clientId);
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
            callbackTopics[topic] = {
              handler: callback.handler
            };
            broker.mq.on(
                topic,
                clientCallbackHandler,
                noop);
          });
        }

        clientEvents.emit('auth', true);
        metaData.state = 'open';
        client.connack(metaData.connack);
        if (sessionData !== false) {
          for (let i = 0; i < sessionData.subs.length; i++) {
            addSubscription(
                sessionData.subs[i].topic,
                sessionData.subs[i].qos
            );
          }
          await processOutboundQueue();
        }
      } catch (error) {
        clientEvents.emit('auth', false);
        log('error', 'client.on(connect) error', error);
        broker.persist.setOnline(packet.clientId, false);
        client.connack({ reasonCode: MQTT.SERVER_UNAVAILABLE }, function () {
          closeClient();
        });
      }
    } else {
      clientEvents.emit('auth', false);
      metaData.authFailed = true;
      log('info', `MQTT [${packet.clientId}] Denied`);
      client.connack({ reasonCode: MQTT.BAD_USER_PASS }, function () {
        client.destroy();
      });
      broker.persist.setOnline(packet.clientId, false);
    }
  });

  // client --> broker published
  client.on('publish', async function (packet) {
    if (metaData.authMeta === false) {
      const result = await waitAuth();
      if (result === false) {
        log('debug', 'auth failure @ publish');
        return;
      }
    }
    sessionRefresh().then();
    const messageId = packet.messageId ? packet.messageId : 0;
    const publication = broker.findPublication(packet.topic);
    if (publication.handler) {
      const access = await publication.handler.cb.access(
          publication.params,
          metaData.authMeta
      );
      if (access) {
        const pubs = await publication.handler.cb.publish(
            publication.params,
            metaData.authMeta,
            packet.payload
        );
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
              pubs.client[i].messageId = UNASSIGNED_MSG_ID; //Individual client flag to create messageId
              pubs.client[i].brokerCounter = broker.counter++;
              pubs.client[i].brokerId = broker.serverId;
              broker.persist.queueClientMessage(metaData.clientId, pubs.client[i]);
              await clientPublishPacket(pubs.client[i], noop);
            }
          }
          if (pubs.all) {
            for (let i = 0; i < pubs.all.length; i++) {
              pubs.all[i].qos = MQTT.QOS_1;
              pubs.all[i].messageId = UNASSIGNED_MSG_ID; //Individual client flag to create messageId
              pubs.all[i].brokerCounter = broker.counter++;
              pubs.all[i].brokerId = broker.serverId;
              broker.persist.queueMessage(pubs.all[i]);
              // Send initial publish
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
        // Debug helper
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
      // Debug helper
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
    broker.persist.deleteMessage(metaData.clientId, packet);
  });

  client.on('unsubscribe', async function (packet) {
    if (metaData.authMeta === false) {
      const result = await waitAuth();
      if (result === false) {
        log('debug', 'auth failure');
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
        log('debug', 'auth failure');
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
        broker.persist.addSubscription(metaData.clientId, sub.topic);
        addSubscription(sub.topic);

        const fetchers = broker.retrieveFetchers(sub.topic);
        for (let f = 0; f < fetchers.length; f++) {
          const fetcher = fetchers[f];
          try {
            const result = await fetcher.cb.load(sub.params, metaData.authMeta);
            const pkts = result.packets;
            for (let p = 0; p < pkts.length; p++) {
              const pkt = pkts[p];
              pkt.messageId = UNASSIGNED_MSG_ID;
              pkt.qos = sub.qos;
              pkt.brokerCounter = broker.counter++;
              pkt.brokerId = broker.serverId;
              if (sub.qos > 0) {
                broker.persist.queueClientMessage(metaData.clientId, pkt);
              }
              clientPublishPacket(pkt, noop).then();
            }
          } catch (error) {
            log('error', `Error @ subscribe`, error);
          }
        }
      }
    }

    client.suback({
      granted: granted,
      messageId: packet.messageId
    });
  });

  client.on('disconnect', function () {
    log('debug', `MQTT [${metaData.clientId}] Req disconnect`);
    closeClient().then();
  });
  // connection error handling
  client.on('close', function () {
    closeClient().then();
  });
  client.on('error', function (error) {
    const ignored = ['EPIPE', 'ECONNRESET', 'ECONNABORTED'];
    if (!ignored.find((element) => element === error.code)) {
      log('info', `MQTT [${metaData.clientId}] Error`, error);
    } else {
      log(
          'info',
          `MQTT [${metaData.clientId}] pipe failed ${error.code}`
      );
    }
    if (metaData.state !== 'closed') {
      closeClient().then();
    }
  });

  const streamTimeout = function () {
    log('info', `MQTT [${metaData.clientId}] Timed out`);
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
module.exports.UNASSIGNED_MSG_ID = UNASSIGNED_MSG_ID;

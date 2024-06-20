const Redis = require('ioredis');
const logger = require('lib/winston.js');

const SessionCacheExpiry = 10000;
const cleanupTimePeriod = 60000;
const willCheckInterval = 15000;

const sessionDb = function (serverId, maxSession, globalProcessId) {

  if (!(this instanceof sessionDb)) {
    return new sessionDb(serverId, maxSession, globalProcessId);
  }

  this.redis = new Redis({
    host: process.env.API_REDIS_HOST,
    port: process.env.API_REDIS_PORT,
    password: process.env.API_REDIS_PW,
    db: process.env.API_REDIS_DB,
    connectionName: 'MQTT-SESSION-' + serverId,
    TLS: process.env.API_REDIS_TLS,
    family: 4           // 4 (IPv4) or 6 (IPv6)
  });

  const refreshCache = [];
  const that = this;

  const subscriptionKey = function(sessionId){
    return 'subscription:' + sessionId;
  };

  const subscriptionRevKey = function (topic) {
    return 'subscription-rev:' + topic;
  };

  const messageOutKey = function (sessionId, topic) {
    return `msgs-out:${sessionId}:${topic}`;
  };

  const messageOutIndexKey = function (sessionId) {
    return `msgs-out-index:${sessionId}`;
  };

  const messageOutIdKey = function (sessionId){
    return `msgs-out-id:${sessionId}`;
  };

  const globalMessageKey = function (topic) {
    return `msgs-global:${topic}`;
  };

  const globalMessageIndexKey = function (subscription) {
    return `msgs-table:index:${subscription}`;
  };

  const messagesGlobalListKey = function () {
    return 'msgs-table:list';
  };

  const sessionKey = function (sessionId) {
    if(sessionId === ''){
      return `sessions:active:blank`;
    }
    return `sessions:active:${sessionId}`;
  };

  const sessionListKey = function () {
    return 'sessions:list';
  };

  const publishHandledKey = function (sessionId) {
    return `publishHandled:${sessionId}`;
  };

  const activeSessionKey = function (globalProcessId) {
    return `online:${globalProcessId}`;
  };

  const willKey = function (sessionId) {
    return `wills:${sessionId}`;
  };

  const willIndexKey = function () {
    return `wills-index`;
  };

  const databaseCleanup = async function () {

    await that.redis.multi()
      .sadd(activeSessionKey(globalProcessId), 'broker:' + serverId)
      .expire(activeSessionKey(globalProcessId), maxSession)
      .exec();

    const sessionResults = await that.redis.sort(
      sessionListKey(),
      'BY',
      sessionKey('*->time'),
      'GET',
      '#',
      'GET',
      sessionKey('*->time'));

    for (let i = 0; i < sessionResults.length; i += 2) {
      if (!sessionResults[i + 1]) {
        await that.cleanSession(sessionResults[i]);
        await that.redis.srem(sessionListKey(), sessionResults[i]);
      }
    }

    const globalMessageList = await that.redis.smembers(messagesGlobalListKey());

    for (let i = 0; i < globalMessageList.length; i++) {
      const sub = globalMessageList[i];
      const exists = await that.redis.exists(globalMessageIndexKey(sub));
      if (exists === 1) {
        await that.redis.zremrangebyscore(
          globalMessageIndexKey(sub), 0, Date.now() - (maxSession * 1100) // * 1.1, generous time
        );
      } else {
        await that.redis.srem(messagesGlobalListKey(), sub);
      }
    }

    setTimeout(databaseCleanup, cleanupTimePeriod);

  };
  // Only if master eventually
  //databaseCleanup().then();
  setTimeout(databaseCleanup, 250);

  this.saveClosingInfo = async function (sessionId, messageId, time) {
    await that.redis.multi()
      .hset(sessionKey(sessionId), 'messageId', messageId)
      .hset(sessionKey(sessionId), 'time', time)
      .hset(sessionKey(sessionId), 'status', 'offline')
      .expire(sessionKey(sessionId), maxSession)
      .exec();
  };

  this.refreshSession = async function (sessionId, will) {
    if (refreshCache[sessionId]) {
      return;
    }
    setTimeout(function () {
      delete refreshCache[sessionId];
    }, SessionCacheExpiry);
    refreshCache[sessionId] = true;
    if (will) {
      await that.redis.multi()
        .expire(sessionKey(sessionId), maxSession)
        .zadd(willIndexKey(), Date.now() + will.expiration, sessionId)
        .exec();
    } else {
      await that.redis.expire(sessionKey(sessionId), maxSession);
    }

  };

  /**
   *
   * @param sessionId
   * @returns {Object|boolean} Subscription topics
   */
  this.findSession = async function (sessionId) {
    try {
      const results = await that.redis.multi()
        .expire(sessionKey(sessionId), maxSession)//Refresh session if it exists
        .hget(sessionKey(sessionId), 'time')
        .hget(sessionKey(sessionId), 'messageId')
        .hgetall(subscriptionKey(sessionId))
        .hget(sessionKey(sessionId), 'timeTail')
        .exec();

      const time = parseInt(results[1][1]);
      const messageId = parseInt(results[2][1]);
      const list = results[3][1];
      let timeTail = parseInt(results[4][1]);

      const subKeys = Object.keys(list);
      if (subKeys.length === 0 || !time) {
        await that.cleanSession(sessionId);
        return false;
      }

      const subscriptions = [];
      for (let i = 0; i < subKeys.length; i++) {
        subscriptions.push({
          topic: subKeys[i],
          qos: list[subKeys[i]]
        });
      }

      timeTail = isNaN(timeTail) ? 0 : timeTail;

      await that.redis.multi()
        .hset(sessionKey(sessionId), 'time', Date.now())
        .hset(sessionKey(sessionId), 'status', 'online')
        .expire(sessionKey(sessionId), maxSession)
        .exec();

      return {
        subscriptions,
        messageId,
        existingMessageIds: await that.getUsedMessageIds(sessionId),
        timeTail
      };
    } catch (error) {
      logger.log('error', 'db.findSession', error);
      await that.cleanSession(sessionId);
      return false;
    }
  };

  this.cleanSession = async function (sessionId) {

    const resultSet = await that.redis.multi()
      .zrange(messageOutIndexKey(sessionId), 0, -1)
      .hkeys(subscriptionKey(sessionId))
      .exec();

    const messages = resultSet[0][1];
    const subscriptions = resultSet[1][1];

    const multi = that.redis.multi();

    for (let i = 0; i < messages.length; i++) {
      multi.del(messageOutKey(sessionId, messages[i]));
    }
    multi.del(messageOutIndexKey(sessionId));
    multi.del(messageOutIdKey(sessionId));
    for (let i = 0; i < subscriptions.length; i++) {
      multi.hdel(subscriptionRevKey(subscriptions[i]), sessionId);
    }
    multi.del(subscriptionKey(sessionId));
    multi.del(publishHandledKey(sessionId));
    await multi.exec();
  };

  this.startSession = async function (sessionId) {
    const t = Date.now();
    await that.redis.multi()
      .hset(sessionKey(sessionId), 'messageId', 1)
      .hset(sessionKey(sessionId), 'time', t)
      .hset(sessionKey(sessionId), 'timeTail', t)
      .hset(sessionKey(sessionId), 'status', 'online')
      .sadd(sessionListKey(),sessionId)
      .expire(sessionKey(sessionId), maxSession)
      .exec();
  };

  this.getPublishHandled = async function (sessionId, handlerIndex, messageId) {
    const results = await that.redis.multi()
      .zremrangebyscore(publishHandledKey(sessionId), 0, Date.now() - (maxSession / 4 * 1000))
      .zscore(publishHandledKey(sessionId), `${handlerIndex}:${messageId}`)
      .exec();
    return results[1][1] !== null;
  };

  this.setPublishHandled = async function (sessionId, handlerIndex, messageId) {
    await that.redis.multi()
      .zremrangebyscore(publishHandledKey(sessionId), 0, Date.now() - (maxSession / 4 * 1000))
      .zadd(publishHandledKey(sessionId), Date.now(), `${handlerIndex}:${messageId}`)
      .exec();
  };

  this.getOnline = async function (sessionId) {
    const result = await that.redis.sismember(activeSessionKey(globalProcessId), sessionId);
    return parseInt(result) === 1;
  };

  this.setOnline = async function (sessionId, value) {
    if (value) {
      const result = await that.redis.sadd(activeSessionKey(globalProcessId), sessionId);
      return parseInt(result) === 1;
    } else {
      const result = await that.redis.srem(activeSessionKey(globalProcessId), sessionId);
      return parseInt(result) === 1;
    }
  };

  this.addSubscription = async function (sessionId, topic, qos = 1) {
    await that.redis.multi()
      .hset(subscriptionKey(sessionId), topic, qos)
      .hset(subscriptionRevKey(topic), sessionId, qos)
      .exec();
  };

  this.removeSubscription = async function (sessionId, topic) {
    await that.redis.multi()
      .hdel(subscriptionKey(sessionId), topic)
      .hdel(subscriptionRevKey(topic), sessionId)
      .exec();
  };

  this.getSubscriptions = async function (sessionId){
    const list = await that.redis.hgetall(subscriptionKey(sessionId));
    const subKeys = Object.keys(list);

    const subscriptions = [];
    for (let i = 0; i < subKeys.length; i++) {
      subscriptions.push({
        topic: subKeys[i],
        qos: list[subKeys[i]]
      });
    }
    return subscriptions;
  };

  this.addAllMessage = async function (packet, subscription) {

    const time = Date.now();
    await that.redis.multi()
      .hset(globalMessageKey(packet.topic), 'packet', JSON.stringify(packet))
      .hset(globalMessageKey(packet.topic), 'time', time)
      .expire(globalMessageKey(packet.topic), maxSession + 250) //  + 250ms to prevent message disappearing
      .zadd(globalMessageIndexKey(subscription), time, packet.topic)
      .zremrangebyscore(globalMessageIndexKey(subscription), 0, (Date.now() - (maxSession * 1100)))
      .sadd(messagesGlobalListKey(), subscription)
      .exec();

  };

  that.redis.defineCommand('queueMessage', {
    /**
     * @function that.redis.queueMessage
     * @param {string} messageOutKey      - KEYS[1] messageOutKey(sessionId, topic)
     * @param {string} messageOutIdKey    - KEYS[2] messageOutIdKey(sessionId)
     * @param {string} messageOutIndexKey - KEYS[3] messageOutIndexKey(sessionId)
     * @param {string} packet    - ARGV[1]
     * @param {string} topic     - ARGV[2]
     * @param {number} messageId - ARGV[3]
     * @param {number} time      - ARGV[4]
     */
    numberOfKeys: 3,
    lua:
      `
    local messageId = redis.call('hget', KEYS[1], 'messageId') -- messageOutIdKey
    local ret = -1;
    if (messageId) then
      redis.call('hdel', KEYS[2], messageId) -- messageOutIdKey, messageId
      ret = messageId -- overwrite happened
    end
    redis.call('hset', KEYS[1], 'packet', ARGV[1]) -- messageOutKey, , packet
    redis.call('hset', KEYS[1], 'time', ARGV[4]) -- messageOutKey, , time
    redis.call('hset', KEYS[1], 'messageId', ARGV[3]) -- messageOutKey, , messageId
    redis.call('hset', KEYS[2], ARGV[3], ARGV[2]) -- messageOutKey, messageId, topic
    redis.call('zadd', KEYS[3], ARGV[4], ARGV[2]) -- messageOutIndexKey, time, topic
    return ret
    `
  });

  this.queueMessage = async function (sessionId, packet) {

    // console.log(sessionId, 'queueMessage', packet.messageId, packet.topic);

    const time = Date.now();
    const results = await that.redis.queueMessage(
      messageOutKey(sessionId, packet.topic),
      messageOutIdKey(sessionId),
      messageOutIndexKey(sessionId),
      JSON.stringify(packet),
      packet.topic,
      packet.messageId,
      time
    );
    return {
      time,
      replaced: parseInt(results)
    };
  };

  this.retrieveQueue = async function (sessionId) {
    const packets = [];
    const results = await that.redis.sort(
      messageOutIndexKey(sessionId),
      'BY',
      messageOutKey(sessionId, '*->time'),
      'GET',
      messageOutKey(sessionId, '*->packet'),
      'GET',
      messageOutKey(sessionId, '*->time'));

    for (let i = 0; i < results.length; i += 2) {
      const pkt = JSON.parse(results[i]);
      pkt.payload = String(pkt.payload);
      pkt.time = parseInt(results[i + 1]);
      packets.push(pkt);
    }
    return packets;
  };

  this.getGlobalMessages = async function (sessionId, timeTail) {
    const subscriptions = await that.redis.hkeys(subscriptionKey(sessionId));
    if(subscriptions.length === 0){
      return [];
    }
    const list = [];
    const scratchName = 'scratch:' + sessionId;
    list.push('zunionstore');
    list.push(scratchName);
    list.push(subscriptions.length);
    for (let i = 0; i < subscriptions.length; i++) {
      list.push(globalMessageIndexKey(subscriptions[i]));
    }

    const result = await that.redis.pipeline([list])
      .expire(scratchName, 60)
      .zremrangebyscore(scratchName, 0, timeTail)
      .sort(
        scratchName,
        'BY',
        globalMessageKey('*->time'),
        'GET',
        globalMessageKey('*->packet'),
        'GET',
        globalMessageKey('*->time')
      )
      .exec();
    const packets = [];
    const pkts = result[3][1];
    for (let i = 0; i < pkts.length; i += 2) {
      if (pkts[i]) {
        const pkt = JSON.parse(pkts[i]);
        pkt.payload = String(pkt.payload);
        pkt.time = parseInt(pkts[i + 1]);
        packets.push(pkt);
      }
    }
    return packets;

  };

  /**
   *
   * @param {string} sessionId
   * @param {number} timeTail
   * @returns {Promise<{queued: *, global: []}>}
   */
  this.retrieveMessages = async function (sessionId, timeTail) {

    const packets = await that.retrieveQueue(sessionId);
    const globalPackets = await that.getGlobalMessages(sessionId, timeTail);

    return {
      queued: packets,
      global: globalPackets // Todo add global packets here
    };
  };

  this.getUsedMessageIds = async function (sessionId) {
    const ids = await that.redis.hkeys(messageOutIdKey(sessionId));
    for (let i = 0; i < ids.length; i++) {
      ids[i] = parseInt(ids[i]);
    }
    return ids;
  };

  this.deleteMessage = async function (sessionId, messageId, timeTail) {
    const topic = await that.redis.hget(messageOutIdKey(sessionId), messageId);
    // console.log(sessionId, 'deleteMessage', messageId, topic);
    const multi = that.redis.multi();
    if (topic) {
      multi.del(messageOutKey(sessionId, topic));
      multi.hdel(messageOutIdKey(sessionId), messageId);
      multi.zrem(messageOutIndexKey(sessionId), topic);
    }
    multi.hset(sessionKey(sessionId), 'timeTail', timeTail);
    await multi.exec();
  };

  this.addWill = async function (sessionId, packet, authMeta, expiration) {
    await that.redis.multi()
      .hset(willKey(sessionId), 'expiration', expiration)
      .hset(willKey(sessionId), 'authMeta', JSON.stringify(authMeta))
      .hset(willKey(sessionId), 'packet', JSON.stringify(packet))
      .zadd(willIndexKey(), Date.now() + expiration, sessionId)
      .exec();
  };

  this.deleteWill = async function (sessionId) {
    await that.redis.multi()
      .del(willKey(sessionId))
      .zrem(willIndexKey(), sessionId)
      .exec();
  };

  this.getWills = async function () {

    const results = await that.redis.multi()
      .zunionstore('scratch:wills', 1, willIndexKey())
      .zremrangebyscore('scratch:wills', Date.now(), 'inf')
      .sort(
        'scratch:wills',
        'BY',
        willKey('*->expiration'),
        'GET',
        '#',
        'GET',
        willKey('*->packet'),
        'GET',
        willKey('*->authMeta')) // TODO consider adding index retrieval for extra time on not offline
      .del('scratch:wills')
      .exec();
    const willResults = results[2][1];
    const expiredWills = [];
    for (let i = 0; i < willResults.length; i += 3) {
      // Is it still registered online?
      if (await that.getOnline(willResults[i])) {
        //Give extra time
      } else {
        expiredWills.push({
          clientId: willResults[i],
          packet: JSON.parse(willResults[i + 1]),
          authMeta: JSON.parse(willResults[i + 2])
        });
      }
    }
    return expiredWills;
  };

};

module.exports = sessionDb;

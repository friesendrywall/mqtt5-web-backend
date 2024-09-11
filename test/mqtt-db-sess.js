require('app-module-path').addPath(__dirname + './../src/');
const sessionDb = require('./../src/session');
var assert = require('assert');
const { equal } = require("assert");

const fakeSession1 = 'fake-id-1';
const fakeSession2 = 'fake-id-2';
let that;
const maxSession = 15;

describe('mqtt-sess', function () {
  beforeEach(async function () {

    that = this;
    that.persist = new sessionDb({ ttl: maxSession });
  });

  this.timeout(maxSession * 2 * 1000);

  it('should not find session1', async () => {
    // console.log('#1');
    const results = await that.persist.findSession(fakeSession1);
    assert.equal(results,false);
  });

  it('should find session1', async () => {
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1,'test/#');
    const results = await that.persist.findSession(fakeSession1);
    assert.equal(results.messageId,1);
  });

  it('should find session1 data', async () => {
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.addSubscription(fakeSession1, 'topic/#');
    const results = await that.persist.findSession(fakeSession1);
    assert.deepEqual(results.subs, [{
      qos: 1,
      topic: 'test/#',
      nl: undefined,
      rap: undefined,
      rh: undefined
    },
      {
        qos: 1,
        topic: 'topic/#',
        nl: undefined,
        rap: undefined,
        rh: undefined
      }]);
  });

  it('should add and delete subscriptions', async () => {
    await that.persist.startSession(fakeSession1);
    that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.addSubscription(fakeSession1, 'topic/#');
    let session = await that.persist.findSession(fakeSession1);
    assert(session.subs.length, 2);
    // Test removal now
    await that.persist.deleteSessions([fakeSession1]);
    session = await that.persist.findSession(fakeSession1);
    assert(session === false);
  });

  it('should add and delete messages', async () => {
    const packet1 = /**@type AedesPacket */{
      messageId: 3,
      topic: 'test/all/out/',
      payload: 'test 1',
      brokerCounter: 1,
      brokerId: 'Random-12345',
      qos: 1
    };
    const packet2 = /**@type AedesPacket */{
      messageId: 5,
      topic: 'test/1/out/',
      payload: 'test 1',
      brokerCounter: 2,
      brokerId: 'Random-12345',
      qos: 1
    };
    const packet3 = /**@type AedesPacket */{
      messageId: 10,
      topic: 'test/2/out/',
      payload: 'test 2',
      brokerCounter: 3,
      brokerId: 'Random-12345',
      qos: 1
    };
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.queueMessage(packet1);
    await that.persist.queueClientMessage(fakeSession1, packet2);
    await that.persist.queueClientMessage(fakeSession1, packet3);
    that.persist.updateMessageId(fakeSession1, packet1);
    that.persist.updateMessageId(fakeSession1, packet2);
    that.persist.updateMessageId(fakeSession1, packet3);
    let messages = await that.persist.getOfflineMessages(fakeSession1);

    assert.equal(messages.length, 3, '#1');
    assert.equal(messages[0].messageId, 3, '#2');
    assert.equal(messages[2].messageId, 10, '#3');
    // Test removal now
    that.persist.deleteMessage(fakeSession1, packet1);
    that.persist.deleteMessage(fakeSession1, packet2);
    that.persist.deleteMessage(fakeSession1, packet3);
    messages = await that.persist.getOfflineMessages(fakeSession1, 0);
    assert.equal(messages.length, 0, '#4');
  });

  it('should queue and delete message', async () => {
    const packet = /**@type AedesPacket */{
      messageId: 5,
      topic: 'test/1/out/',
      payload: 'test 1',
      brokerCounter: 3,
      brokerId: 'Random-12345',
      qos: 1
    }
    await that.persist.startSession(fakeSession1);
    that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.queueMessage(packet);
    that.persist.updateMessageId(fakeSession1, packet);
    let messages = await that.persist.getOfflineMessages(fakeSession1);
    assert.equal(messages.length, 1, '#1');
    assert.equal(messages[0].messageId, 5, '#2');
    await that.persist.deleteMessage(fakeSession1, packet);
    messages = await that.persist.getOfflineMessages(fakeSession1);
    assert.equal(messages.length, 0, '#3');
  });

  it('should save session data', async () => {
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1, 'test/#');
    // await that.persist.deleteMessage(fakeSession1, 1, 123456);
    await that.persist.saveSession(fakeSession1, 123);
    const results = await that.persist.findSession(fakeSession1);
    assert(results.subs.length === 1, 'length error');
    assert(results.messageId === 123, 'messageId error');
  });

  it('should session timeout', async () => {
    const timeout = function (ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    };
    this.slow(maxSession * 2000);
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.saveSession(fakeSession1, 5);
    await timeout((maxSession * 1000) + 500);
    /* Call heartbeat item */
    const expiredSessions = that.persist.getExpiredSessions();
    that.persist.deleteSessions(expiredSessions);

    const results = await that.persist.findSession(fakeSession1);
    assert(results === false);
  });

  // refresh session
  it('should not session timeout', async () => {
    const timeout = function (ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    };
    this.slow(maxSession * 2000);
    await that.persist.startSession(fakeSession1);
    await that.persist.addSubscription(fakeSession1, 'test/#');
    await that.persist.saveSession(fakeSession1, 5);
    await timeout((maxSession * 1000) - 1000);
    await that.persist.refreshSession(fakeSession1);
    await timeout((maxSession * 1000) / 4);
    /* Call heartbeat item */
    const expiredSessions = that.persist.getExpiredSessions();
    that.persist.deleteSessions(expiredSessions);
    const results = await that.persist.findSession(fakeSession1);
    assert(results);
  });

  it('should set online and offline', async () => {

    let result = that.persist.setOnline(fakeSession1, true);
    assert(result === true, 'Online already err');
    result = that.persist.setOnline(fakeSession1, true);
    assert(result === false, 'Should be online');
    result = that.persist.setOnline(fakeSession1, false);
    assert(result === true, "Didn't set offline");
    result = that.persist.setOnline(fakeSession1, false);
    assert(result === false, "Wasn't offline");
  });

  it('should store retained', async () => {

    await that.persist.startSession(fakeSession1);
    let retained = await that.persist.getRetainedMessages('test/#');
    assert(retained.length === 0, 'Messages should not exist');
    await that.persist.storeRetained({
      topic: 'test/111/',
      payload: 'Retained',
      retain: true
    });
    retained = await that.persist.getRetainedMessages('test/#');
    assert(retained.length === 1, '1 Message should exist');
    assert(retained[0].payload === 'Retained', 'Message should exist');
  });

  // Database cleanup

});

require('dotenv').config({ path: './.env' });
require('app-module-path').addPath(__dirname + './../src/');
// const Redis = require('ioredis');
const assert = require('assert');
const MQTT_ERR = require('./../src/const.js');

const net = require('net');
const mqttCon = require('mqtt-connection');
const { generateToken } = require('./../example/auth/jwt.js');
const jwt = require("../example/auth/jwt");
const server = require("../src");
const { v4: uuidv4 } = require("uuid");

let that;
const maxSession = 30;
let testAdminPassword;
let testClientPassword;
let mqttBroker;

const mqtt = function () {

  const auth_func = function (username, password) {
    let authMeta;
    try {
      authMeta = jwt.verifyToken(password);
    } catch (error) {
      authMeta = false;
    }
    return authMeta;
  }

  const metadata_func = function (authMeta) {
    authMeta.specialProperty = 'Hello_world';
    authMeta.access_level = 1;
  }

  const mqttServer = new server(uuidv4(null, null, null), {
    heartbeat_time: 30,
    auth_func,
    metadata_func,
    module_dir: __dirname + '/../example/'
  });

  const net = require('net');
  const netServer = new net.Server();

  mqttServer.autoloadModules().then();
  mqttBroker = mqttServer;
  netServer.on('connection', mqttServer.connection);
  netServer.listen(1883);
};

const doStdConn = function (clean, ready, done) {
  that.conn.connect({
    clientId: 'mqtt-js-test',
    protocolVersion: 5,
    clean: clean,
    username: '****',
    password: testAdminPassword,
    keepalive: 30
  });
  that.conn.on('connack', function (packet) {
    assert.equal(packet.reasonCode, 0);
    if (packet.reasonCode !== 0) {
      console.log('\tConnack failure');
      that.conn.destroy();
      done();
    }
    if (ready) {
      ready(packet);
    }
  });
  that.conn.on('error', function (error) {
    assert.equal(false, true, error);
    console.log('\tconnection failure');
    done();
  });
};

const doStdSubConn = function (clean, ready, done, clientId = 'mqtt-js-test') {

  that.conn.connect({
    clientId: clientId,
    protocolVersion: 5,
    clean: clean,
    username: '****',
    password: testAdminPassword,
    keepalive: 30
  });

  that.conn.on('error', function (error) {
    assert.equal(false, true, error);
    done();
  });

  that.conn.on('connack', function (packet) {
    assert.equal(packet.reasonCode, 0);
    if (packet.reasonCode !== 0) {
      that.conn.destroy();
      done();
    }
    that.conn.subscribe({
      // dup: 0,
      messageId: (Date.now() & 0xFFFF),
      subscriptions: [{
        topic: 'test/#',
        qos: 1
      }]
    }, function () {
      if (ready) {
        ready(packet);
      }
    });
  });
};

describe('mqtt-test', function () {
  before(async function () {
    // This is test only
    testAdminPassword = generateToken(
      1,
      1
    );
    testClientPassword = generateToken(
      1,
      1
    );
    mqtt();
  });

  beforeEach(async function () {
    that = this;
    const stream = net.createConnection(1883, '127.0.0.1');
    that.conn = mqttCon(stream, {
      protocolVersion: 5
    });
  });

  afterEach(async function () {
    if (that.conn) {
      that.conn.destroy();
    }
  });

  this.timeout(500);

  it('should be denied', function (done) {
    that.conn.connect({
      clientId: 'mqtt-js-test',
      protocolVersion: 5,
      clean: false,
      username: '****',
      password: testAdminPassword + 'bad',
      keepalive: 30
    });
    that.conn.on('connack', function (packet) {
      assert.notEqual(packet.reasonCode, 0);
      that.conn.destroy();
      done();
    });
  });

  it('should fail acl subscription check', function (done) {
    that.conn.connect({
      clientId: 'mqtt-js-test',
      protocolVersion: 5,
      clean: false,
      username: '****',
      password: testClientPassword,
      keepalive: 30
    });
    that.conn.on('connack', function (packet) {
      assert.equal(packet.reasonCode, 0);
      that.conn.destroy();
      done();
    });
    that.conn.on('suback', function (packet) {
      that.conn.disconnect();
      assert.equal(
        packet.granted[0] === MQTT_ERR.NOT_AUTHORIZED, true,
        'Should fail acl');
      done();
    });
  });

  it('should connect', function (done) {
    doStdConn(true, function () {
      done();
    });
  });

  it('should get subscription', function (done) {
    doStdSubConn(true, function () {
      console.log('\tAwaiting packets');
    }, done);

    const packets = [];
    that.conn.on('publish', function (packet) {

      if (packet.qos === 1) {
        that.conn.puback({
          messageId: packet.messageId,
          reasonCode: 0
        });
      }
      packets.push(packet);
      if (packets.length === 2) {
        assert.equal(packets[0].topic === 'test/0/out/', true);
        assert.equal(packets[1].topic === 'test/1/out/', true);
        assert.equal(
          packets[0].payload.toString() === '0', true,
          packets[0].payload.toString() + ' !== 0'
        );
        assert.equal(packets[1].payload.toString() === '1', true,
          packets[1].payload.toString() + ' !== 1'
        );
        done();
      }
    });
  });

  it('should get broker broadcast', function (done) {
    this.timeout(1000);
    let rx = 0;
    doStdSubConn(true, null, null, 'mqtt-js-test');

    that.conn.on('publish', function (packet) {
      rx++;
      console.log(`\trx #${rx} ${packet.topic}`);
      if (rx === 1) {
        console.log(`\tSend broadcast`);
        mqttBroker.sendUpdateBroadcast('testBroadcast/1/#',
            {
              id: 1024,
              payload: 'HELLO_WORLD'
            }, 1);
      }
      if (packet.topic === 'test/1024/out/') {
        assert.equal(packet.topic, 'test/1024/out/', 'Should be correct payload');
        assert.equal(packet.payload, 'HELLO_WORLD', 'Should be HELLO_WORLD');
        done();
      }
    });

  });

  it('should get offline publishes', function (done) {

    let localConn;
    const testPayload = 'test-' + Date.now();
    this.slow(1000);
    doStdSubConn(true, null, done, 'mqtt-js-test-offline');

    that.conn.on('suback', function (packet) {
      that.conn.destroy();
      console.log('\tSubscribed to test/#');
      startSecond();
    });

    const startSecond = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdSubConn(true, function () {
        console.log('\tPublishing to test/10/out/', testPayload);
        that.conn.publish({
          topic: 'test/100/out/',
          payload: testPayload,
          qos: 1,
          messageId: 10
        });
      }, done, 'mqtt-js-test-out');
      let next = false;
      that.conn.on('puback', function (packet) {
        if (!next) {
          next = true;
          setTimeout(function () {
            that.conn.destroy();
            startTest();
          }, 250);
        }
      });
    };

    const startTest = function () {
      let complete = false;
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdSubConn(false, null, done, 'mqtt-js-test-offline');
      that.conn.on('publish', function (packet) {
        if (packet.qos === 1) {
          that.conn.puback({
            messageId: packet.messageId,
            reasonCode: 0
          });
        }
        if (packet.topic === 'test/100/out/' && !complete) {
          complete = true;
          console.log('\tReceived from test/10/out/', packet.payload.toString());
          assert.equal(packet.payload.toString(), testPayload, 'Should = ' + testPayload);
          done();
        }
      });
    };
  });

  it('should publish last of two to same topic', function (done) {

    this.slow(1000);
    let localConn;
    const testPayload = 'test-' + Date.now();

    doStdSubConn(true, null, done, 'mqtt-js-test-offline');

    that.conn.on('suback', function (packet) {
      that.conn.destroy();
      console.log('\tSubscribed to test/#');
      startSecond();
    });

    const startSecond = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdSubConn(true, function () {
        console.log('\tPublishing to test/123/out/', testPayload + 'oldest');
        that.conn.publish({
          topic: 'test/123/out/',
          payload: testPayload + 'oldest',
          qos: 1,
          messageId: 100
        });
        that.conn.publish({
          topic: 'test/123/out/',
          payload: testPayload,
          qos: 1,
          messageId: 101
        });
      }, done, 'mqtt-js-test-out');
      let next = 0;
      that.conn.on('puback', function (packet) {
        if (++next === 2) {
          // next = true;
          setTimeout(function () {
            that.conn.destroy();
            startTest();
          }, 250);
        }
      });
    };

    const startTest = function () {
      let complete = false;
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdSubConn(false, null, done, 'mqtt-js-test-offline');
      that.conn.on('publish', function (packet) {
        if (packet.qos === 1) {
          that.conn.puback({
            messageId: packet.messageId,
            reasonCode: 0
          });
        }
        if (packet.topic === 'test/123/out/' && !complete) {
          complete = true;
          console.log('\tReceived from test/123/out/', packet.payload.toString());
          assert.equal(packet.payload.toString(), testPayload, 'Should = ' + testPayload);
          done();
        }
      });
    };
  });

  it('should get invalid payload', function (done) {
    let rx = 0;
    let puback = 0;
    let pkt = {
      // dup: 0,
      messageId: (Date.now() & 0xFFF),
      topic: 'test/9/out/',
      qos: 1,
      payload: 'INVALID PAYLOAD'
    };

    doStdConn(true, function () {
      console.log('\tPublish #1', pkt.messageId);
      that.conn.publish(pkt);
    });
    that.conn.on('puback', function (packet) {
      console.log('\tReceive #1', pkt.messageId);
      assert(packet.reasonCode === MQTT_ERR.PAYLOAD_INVALID, 'Should be invalid payload');
      assert(packet.properties.reasonString === 'Invalid payload', 'Should be invalid payload');
      done();
    });

  });

  it('should connect clean = true', function (done) {
    this.slow(1000);
    this.timeout(2500);
    doStdSubConn(true, null, done, 'mqtt-js-test');

    that.conn.on('suback', function (packet) {
      that.conn.destroy();
      console.log('\tInitial subscription');
      finishTest();
    });

    const finishTest = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdConn(true, function (packet) {
        assert.equal(packet.sessionPresent, false, 'No session should be present');
        // wait for rx
        setTimeout(function () {
          done();
        }, 250);

      }, done, 'mqtt-js-test');
      that.conn.on('publish', function (packet) {
        assert.fail('No packets should be received');
        done();
      });
    };
  });

  it('should connect clean = false + remember', function (done) {
    this.slow(1000);
    this.timeout(2500);
    let packets = 0;
    doStdSubConn(true, null, done, 'mqtt-js-test');

    that.conn.on('suback', function (packet) {
      that.conn.destroy();
      console.log('\tInitial subscription');
      finishTest();
    });

    const finishTest = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdConn(false, function (packet) {
        assert(packet.sessionPresent, 'Session should be present');
        // wait for rx
        setTimeout(function () {
          assert(packets > 0, 'Some packets should be received');
          console.log('\tPackets received' ,packets);
          done();
        }, 250);

      }, done, 'mqtt-js-test');
      that.conn.on('publish', function (packet) {
        packets++;
        if (packet.qos === 1) {
          that.conn.puback({
            messageId: packet.messageId,
            reasonCode: 0
          });
        }
      });
    };
  });

  it('should delay immediate requests before auth', function (done) {
    let connAck = false;
    that.conn.connect({
      clientId: 'mqtt-js-test',
      protocolVersion: 5,
      clean: true,
      username: '****',
      password: testAdminPassword,
      keepalive: 30
    });
    that.conn.subscribe({
      // dup: 0,
      messageId: (Date.now() & 0xFFFF),
      subscriptions: [{
        topic: 'test/#',
        qos: 1
      }]
    });
    that.conn.on('connack', function (packet) {
      assert(packet.reasonCode === 0);
      if (packet.reasonCode !== 0) {
        console.log('\tConnack failure');
        that.conn.destroy();
        done();
      }
      connAck = true;
    });

    that.conn.on('suback', function (packet) {
      assert(connAck === true,'suback before authorization');
      that.conn.destroy();
      done();
    });

    that.conn.on('error', function (error) {
      assert.fail(error);
      console.log('\tconnection failure');
      that.conn.destroy();
      done();
    });
  });

  it('should unsubscribe successfully', function (done) {
    this.slow(2500);
    this.timeout(2500);
    let packets = 0;
    const messageId = (Date.now() & 0xFFFF);
    doStdSubConn(true, function () {

    }, done, 'mqtt-js-test');

    that.conn.on('publish', function (packet) {
      if (packet.topic.substring(0, 5) === 'test/') {
        packets++;
      } else {
        console.log('\tUnhandled prefix', packet.topic.substring(0, 5));
      }
      if (packet.qos === 1) {
        that.conn.puback({
          messageId: packet.messageId,
          reasonCode: 0
        });
      }
    });

    setTimeout(function () {
      assert(packets > 0, 'Some packets should have been received');
      that.conn.unsubscribe({
        messageId: messageId,
        unsubscriptions: ['test/#'],
        qos: 1
      });
    }, 500);

    that.conn.on('unsuback', function (packet) {
      console.log('\tunsuback received id:', packet.messageId);
      assert(packet.messageId === messageId, 'MessageId issue');
      finishTest(packets);
    });

    const finishTest = function (packetCount) {
      const stream = net.createConnection(1883, '127.0.0.1');
      const diffConn = mqttCon(stream, {
        protocolVersion: 5
      });

      diffConn.connect({
        clientId: 'mqtt-js-test-other',
        protocolVersion: 5,
        clean: true,
        username: '****',
        password: testAdminPassword,
        keepalive: 30
      });
      diffConn.on('connack', function (packet) {
        assert.equal(packet.reasonCode, 0);
        if (packet.reasonCode !== 0) {
          console.log('\tConnack failure');
          diffConn.destroy();
          done();
        }
        diffConn.publish({
          // dup: 0,
          messageId: 123,
          topic: 'test/3/out/',
          qos: 1,
          payload: 'RANDOM()'
        });
      });

      diffConn.on('puback', function (packet) {
        diffConn.disconnect();
        diffConn.destroy();
      });

      setTimeout(function () {
        assert(packetCount === packets, 'Packages were received :' + packetCount + ':' + packets);
        done();
      }, 500);
    };
  });

  it('should pingreq', function (done) {
    this.timeout(250);
    doStdConn(true, function () {
      that.conn.pingreq(null, function () {
        console.log('\tPing sent');
      });
    }, done);
    that.conn.on('pingresp', function (packet) {
      console.log('\tPing resp');
      assert.equal(true, true);
      done();
    });
  });

  it('should properly sequence messages race conditions', function (done) {
    this.slow(4000);
    this.timeout(5000);
    let time = Date.now();
    let pkt = {
      // dup: 0,
      messageId: (Date.now() & 0xFFF),
      topic: 'test/5/out/',
      qos: 1,
      payload: ''
    };
    const testLength = 75;
    const startPos = 10;
    let int;
    let out = startPos;
    const sendNext = function () {
      pkt.messageId++;
      pkt.topic = `test/${out}/out/`;
      pkt.payload = `payload-${out}-${time}`;
      that.conn.publish(pkt);
      out++;
      if (out === testLength + startPos + 1) {
        clearInterval(int);
      }
    };

    doStdSubConn(true, function () {
      setTimeout(function () {
        console.log('\tClosed initial subscriber connection');
        that.conn.destroy();
      }, 500);
      setTimeout(startSender, 600);
      setTimeout(startAlternate, 650);
    }, done, 'mqtt-test-sequence');

    that.conn.on('publish', function (packet) {
      if (packet.qos === 1) {
        that.conn.puback({
          messageId: packet.messageId,
          reasonCode: 0
        });
      }
    });

    const startSender = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      that.conn = mqttCon(stream, {
        protocolVersion: 5
      });
      doStdConn(true, function () {
        int = setInterval(sendNext, 2);
      });
      that.conn.on('puback', function (packet) {

      });
    };

    const startAlternate = function () {
      const stream = net.createConnection(1883, '127.0.0.1');
      const altConn = mqttCon(stream, {
        protocolVersion: 5
      });

      altConn.connect({
        clientId: 'mqtt-test-sequence',
        protocolVersion: 5,
        clean: false,
        username: '****',
        password: testAdminPassword,
        keepalive: 30
      });

      const packetBuff = [];
      let rx = 0;

      altConn.on('publish', function (packet) {
        if (packet.qos === 1) {
          altConn.puback({
            messageId: packet.messageId,
            reasonCode: 0
          });
        }
        let index = packet.topic.toString().replace('test/','');
        index = index.replace('/out/','');
        index = parseInt(index);
        packetBuff[index] = packet.payload.toString();
        if (packet.payload.toString() === `payload-${index}-${time}`) {
          rx++;
          if (rx === testLength) {
            done();
          }
        }
      });

      altConn.on('error', function (error) {
        assert.fail(error);
        done();
      });

      altConn.on('connack', function (packet) {
        assert(packet.reasonCode === 0);
        if (packet.reasonCode !== 0) {
          altConn.destroy();
          done();
        }
      });

    };

  });

});

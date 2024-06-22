require('dotenv').config({ path: './.env' });
const server = require('../src/index.js');
const { v4: uuidv4 } = require('uuid');
const jwt = require('./auth/jwt.js');
const net = require("net");
const { MQTT5_ERR } = require('../src/index.js');

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

    const clientCallbacks = [
        {
            topicBuilder: function (authMeta) {
                return `test/${authMeta.specialProperty}`;
            },
            handler: function (packet, done) {
                console.log(`Packet rx ${packet.payload.toString()}`)
            }
        }
    ];

    const mqttServer = new server(uuidv4(null, null, null), {
        clientCallbacks,
        heartbeat_time: 30,
        auth_func,
        metadata_func,
        log_func: console.log,
        module_dir: __dirname
    });

    const net = require('net');
    const netServer = new net.Server();

    mqttServer.autoloadModules().then();

    netServer.on('connection', mqttServer.connection);
    netServer.listen(1883);

    // mqttServer.log("Hello world");
    // mqttServer.unsubscribe('',null, null);

};

const testPassword = jwt.generateToken(1, 2);
console.log(testPassword);
mqtt();




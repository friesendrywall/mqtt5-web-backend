const persistence = require('aedes-persistence');

/**
 * @typedef session_opt_t
 * @type {object}
 * @property {number} ttl Seconds
 * @property {log_if_t | null} log_func
 */

/**
 * @typedef session_data_t
 * @type {object}
 * @property {number} last Last unix time
 * @property {boolean} online
 * @property {number} messageId
 * @property {[]} subs
 */

/**
 *
 * @param {session_opt_t} options
 */
const session = function (options) {
    const ttl = options && options.ttl ? options.ttl * 1000 : 3600 * 1000;
    const clients = [];
    const activeSession = [];
    const persist = persistence();

    function getMapRef (map, key, ifEmpty, createOnEmpty = false) {
        const value = map.get(key)
        if (value === undefined && createOnEmpty) {
            map.set(key, ifEmpty)
        }
        return value || ifEmpty
    }

    const log = function (...args) {
        if (options.log_func) {
            options.log_func.apply(this, args);
        }
    }

    /**
     *
     * @returns {string[]}
     */
    this.getExpiredSessions = function () {
        const ret = [];
        Object.keys(clients).forEach((key) => {
            if (Date.now() - clients[key].last > ttl) {
                ret.push(key);
            }
        });
        return ret;
    }

    /**
     * Delete session, including any items in persistence
     * @param {string[]} clientList
     */
    this.deleteSessions = function (clientList) {
        clientList.forEach((client) => {
            persist.cleanSubscriptions(/** @type Client */{ id: client }, () => {
            });
            delete clients[client];
            delete activeSession[client];
        });
    }

    /**
     *
     * @param {string} clientId
     * @returns {Promise<session_data_t>}
     */
    this.findSession = async function (clientId) {
        return new Promise(function (resolve, reject) {
            if (!clients[clientId]) {
                resolve(false);
            }
            clients[clientId].last = Date.now();
            let ret = Object.assign({}, clients[clientId]);
            persist.subscriptionsByClient(
                /** @type {Client} */{ id: clientId },
                (err, subs, client) => {
                    ret.subs = subs;
                });
            resolve(ret);
        });
    }

    /**
     *
     * @param {string} clientId
     */
    this.startSession = function (clientId) {
        clients[clientId] = {
            last: Date.now(),
            messageId: 1
        };
    }

    this.refreshSession = function (clientId) {
        clients[clientId].last = Date.now();
    }

    this.saveSession = function (clientId, messageId) {
        clients[clientId].last = Date.now();
        clients[clientId].messageId = messageId;
    }

    this.setOnline = function (clientId, value) {
        // console.log('setOnline', activeSession);
        if (value) {
            if (activeSession[clientId]) {
                console.log('setOnline ret == false', activeSession);
                return false; // Already online
            }
            activeSession[clientId] = true;
            console.log('setOnline ret == true', activeSession);
            return true;
        } else {
            delete activeSession[clientId];
            console.log('setOffline ret == true', activeSession, clientId);
            return true;
        }
    }

    // Bridge functions
    this.getOfflineMessages = async function (clientId) {
        return new Promise(function (resolve, reject) {
            const packets = [];
            const offlineMsgStream = persist.outgoingStream(/** @type {Client} */{ id: clientId });
            offlineMsgStream.on('data', function (packet) {
                packets.push(packet);
            });
            offlineMsgStream.on('end', function (data) {
                resolve(packets);
            });
        });
    }

    this.queueClientMessage = function (clientId, packet) {
        console.log('queueClientMessage', clientId, packet);
        persist.outgoingEnqueueCombi(
            [{ clientId: clientId }],
            packet,
            (err) => {
                if (err) {
                    log('debug', `Error at outgoingEnqueueCombi ${packet.topic} ${err}`);
                }
            });
    }

    this.queueMessage = function (packet) {
        console.log('queueMessage', packet);
        persist.subscriptionsByTopic(packet.topic,
            function (err, subscriptions) {
                if (err) {
                    log('debug', `Error at topic ${packet.topic} ${err}`);
                } else {
                    persist.outgoingEnqueueCombi(subscriptions, packet,
                        (err) => {
                            if (err) {
                                log('debug', `Error at enqueue topic ${packet.topic} ${err}`);
                            }
                        });
                }
            });
    }

    /**
     *
     * @param {string} clientId
     * @param {AedesPacket} packet
     */
    this.updateMessageId = function (clientId, packet) {
        console.log('updateMessageId', packet);
        persist.outgoingUpdate(
            /** @type {Client} */{ id: clientId }, packet, function (err) {
                if (err) {
                    log('debug', `Error at outgoingUpdate ${packet.topic} ${err}`);
                }
            });
    }

    this.deleteMessage = function (clientId, packet) {
        console.log('deleteMessage', clientId, packet);
        persist.outgoingClearMessageId(
            /** @type {Client} */{ id: clientId }, packet, () => {
            });
    }

    this.addSubscription = function (clientId, topic, qos = 1) {
        console.log('addSubscription', clientId, topic);
        persist.addSubscriptions(/** @type {Client} */{ id: clientId },
            [{
                topic,
                qos
            }], () => {
            });
    }

    this.removeSubscription = function (clientId, topic) {
        console.log('removeSubscription', clientId, topic);
        persist.removeSubscriptions(
            /** @type {Client} */{ id: clientId }, [topic], () => {
            });
    }

}

module.exports = session;

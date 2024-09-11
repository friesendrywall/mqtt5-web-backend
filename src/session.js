const persistence = require('aedes-persistence');

/**
 * @typedef session_opt_t
 * @type {object}
 * @property {number | undefined } ttl Seconds
 * @property {log_if_t | undefined} log_func
 */

/**
 * @typedef sess_sub_t
 * @type {object}
 * @property {string } topic
 * @property {number} qos
 */

/**
 * @typedef session_data_t
 * @type {object}
 * @property {number} last Last unix time
 * @property {number} messageId
 * @property {sess_sub_t[]} subs
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

    const log = function (...args) {
        if (options.log_func) {
            options.log_func.apply(this, args);
        }
    }

    /**
     * Get expired sessions for TTL management
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
     * Delete sessions, including any items in persistence
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
     * Find active session, returning existing subscriptions
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
            ret.subs = [];
            persist.subscriptionsByClient(
                /** @type {Client} */{ id: clientId },
                (err, subs, client) => {
                    ret.subs = subs ? subs : [];
                });
            resolve(ret);
        });
    }

    /**
     * Start session
     * @param {string} clientId
     */
    this.startSession = function (clientId) {
        clients[clientId] = {
            last: Date.now(),
            messageId: 1
        };
    }

    /**
     * Reset session timer
     * @param {string} clientId
     */
    this.refreshSession = function (clientId) {
        clients[clientId].last = Date.now();
    }

    /**
     * Store session variables
     * @param {string} clientId
     * @param {number} messageId
     */
    this.saveSession = function (clientId, messageId) {
        clients[clientId].last = Date.now();
        clients[clientId].messageId = messageId;
    }

    /**
     * Set client online to prevent multiple connections
     * @param {string} clientId
     * @param {boolean} value
     * @returns {boolean}
     */
    this.setOnline = function (clientId, value) {
        if (value) {
            if (activeSession[clientId]) {
                return false; // Already online
            }
            activeSession[clientId] = true;
            return true;
        } else {
            const ret = !!activeSession[clientId];
            delete activeSession[clientId];
            return ret;
        }
    }

    /* Persistence Bridge functions */
    /**
     * Get all pending messages for client
     * @param {string} clientId
     * @returns {Promise<AedesPacket>}
     */
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

    /**
     * Queue individual message to client
     * @param {string} clientId
     * @param {AedesPacket} packet
     */
    this.queueClientMessage = function (clientId, packet) {
        persist.outgoingEnqueueCombi(
            [{ clientId: clientId }],
            packet,
            (err) => {
                if (err) {
                    log('debug', `Err at queueClientMessage ${packet.topic} ${err}`);
                }
            });
    }

    /**
     * Queue message to all subscribed clients
     * @param {AedesPacket} packet
     */
    this.queueMessage = function (packet) {
        persist.subscriptionsByTopic(packet.topic,
            function (err, subscriptions) {
                if (err) {
                    log('debug', `Err at queueMessage-1 topic ${packet.topic} ${err}`);
                } else {
                    persist.outgoingEnqueueCombi(subscriptions, packet,
                        (err) => {
                            if (err) {
                                log('debug',
                                    `Err at queueMessage-2 topic ${packet.topic} ${err}`);
                            }
                        });
                }
            });
    }

    /**
     * Update message id before sending
     * @param {string} clientId
     * @param {AedesPacket} packet
     */
    this.updateMessageId = function (clientId, packet) {
        // console.log('updateMessageId', clientId, packet);
        persist.outgoingUpdate(
            /** @type {Client} */{ id: clientId }, packet, function (err) {
                if (err) {
                    log('debug', `Error at outgoingUpdate ${packet.topic} ${err}`);
                }
            });
    }

    /**
     * Delete message from persistence
     * @param {string} clientId
     * @param {AedesPacket} packet
     */
    this.deleteMessage = function (clientId, packet) {
        // console.log('deleteMessage', clientId, packet);
        persist.outgoingClearMessageId(
            /** @type {Client} */{ id: clientId }, packet, () => {
            });
    }

    /**
     * Add subscription to persistence
     * @param {string} clientId
     * @param {string} topic
     * @param {number} qos
     */
    this.addSubscription = function (clientId, topic, qos = 1) {
        // console.log('addSubscription', clientId, topic);
        persist.addSubscriptions(/** @type {Client} */{ id: clientId },
            /**@type []*/[{
                topic,
                qos
            }], () => {
            });
    }

    /**
     *
     * @param {string} clientId
     * @param {string} topic
     */
    this.removeSubscription = function (clientId, topic) {
        // console.log('removeSubscription', clientId, topic);
        persist.removeSubscriptions(
            /** @type {Client} */{ id: clientId },
            /**@type []*/[topic], () => {
            });
    }

    /**
     * Store retained packet
     * @param {AedesPacket} packet
     */
    this.storeRetained = function (packet) {
        persist.storeRetained(packet, () => {
        });
    }

    /**
     * Get all retained message for topic
     * @param {string} topic
     * @returns {Promise<AedesPacket[]>}
     */
    this.getRetainedMessages = async function (topic) {
        return new Promise(function (resolve, reject) {
            const packets = [];
            const retainedMsgStream = persist.createRetainedStreamCombi([topic]);
            retainedMsgStream.on('data', function (packet) {
                packets.push(packet);
            });
            retainedMsgStream.on('end', function (data) {
                resolve(packets);
            });
        });
    }

}

module.exports = session;

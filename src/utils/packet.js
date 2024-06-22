const MQTT = require('./../const');

/**
 * @typedef payload_topic_t
 * @type {object}
 * @property {string} topic
 * @property {any} payload
 */

/**
 *
 * @param {string} subscriptionId
 * @param {string} reasonString
 * @param {any} error
 * @returns {{puback:
 *    {reasonCode: number, properties: {reasonString: string}}, subscription: string}}
 */
const failPkt = function (subscriptionId, reasonString, error) {
  let packet = {
    subscription: subscriptionId,
    puback: {
      reasonCode: MQTT.PAYLOAD_INVALID,
      properties: {
        reasonString
      }
    }
  };
  if (error) {
    packet.puback.properties.userProperties = {
      error
    };
  }
  return packet;
};

/**
 *
 * @param {payload_topic_t[]} topics
 * @param {string} subscriptionId
 * @param {boolean} gz
 * @returns {{puback:
 *  {reasonCode: number,
 *    properties:
 *      { reasonString: string,
 *        contentType: (string)}},
 *        all: payload_topic_t[],
 *        subscription: string}}
 */
const responsePkt = function (topics, subscriptionId, gz = false) {
  return {
    all: topics,
    subscription: subscriptionId,
    puback: {
      reasonCode: 0,
      properties: {
        reasonString: 'OK',
        contentType: gz ? 'deflated' : 'inflated'
      }
    }
  };
};

/**
 *
 * @param {payload_topic_t[]} topics
 * @param {string} subscriptionId
 * @param {boolean} gz
 * @returns {{puback:
 *  {reasonCode: number,
 *    properties:
 *      { reasonString: string,
 *        contentType: (string)}},
 *        client: payload_topic_t[],
 *        subscription: string}}
 */
const prvResponsePkt = function (topics, subscriptionId, gz = false) {
  return {
    client: topics,
    subscription: subscriptionId,
    puback: {
      reasonCode: 0,
      properties: {
        reasonString: 'OK',
        contentType: gz ? 'deflated' : 'inflated'
      }
    }
  };
};

module.exports = {
  failPkt,
  responsePkt,
  prvResponsePkt
};

const MQTT = require('./../const');

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

const responsePkt = function (topics, subscriptionId) {
  return {
    all: topics,
    subscription: subscriptionId,
    puback: {
      reasonCode: 0,
      properties: {
        reasonString: 'OK'
      }
    }
  };
};

module.exports = {
  failPkt,
  responsePkt
};

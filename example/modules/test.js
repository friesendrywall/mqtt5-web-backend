const {MQTT5_ERR :MQTT } = require('./../../src/index.js');
/**
 *
 * @param {function} server.subscription
 * @param {function} server.fetcher
 * @param {function} server.publication
 */
module.exports = server => {

  let someVar = ["0","1"];

  if (process.env.NODE_ENV === 'development') {
    console.log('Loaded test module');
  }
  const subscriptionId = function () {
    return `test/#`;
  };

  server.subscription('test/#', {
    async access (params, authMeta) {
      return authMeta.access_level === 1;
    }
  });

  server.fetcher('test/+/#', {

    async load (params, authMeta) {

      let topics = [];
      for (let i = 0; i < someVar.length; i++) {
        topics.push({
          topic: `test/${i}/out/`,
          payload: `${someVar[i]}`
        });
      }
      return {
        packets: topics,
        subscription: subscriptionId()
      };
    }
  });

  // Publish to individual alert
  server.publication('test/:id/out/', {
    async access (params, authMeta) {
      return authMeta.access_level === 1;
    },
    async publish (params, authMeta, payload) {
      try {
        if (payload.toString() === 'INVALID PAYLOAD') {
          return {
            puback: {
              reasonCode: MQTT.PAYLOAD_INVALID,
              properties:{
                reasonString: 'Invalid payload'
              }
            }
          };
        }
        someVar[parseInt(params.id)] = payload.toString();
        // Forward the payload instead of new query
        let topics = [];
        topics.push({
          topic: `test/${params.id}/out/`,
          payload: `${someVar[parseInt(params.id)]}`
        });

        return {
          /*client: [{
            topic: `test/CLIENT/out/`,
            payload: Date.now().toString()
          }],*/
          all: topics,
          subscription: subscriptionId(),
          puback: {
            reasonCode: 0,
            properties: {
              reasonString: 'OK'
            }
          }
        };
      } catch (error) {
        logger.log('debug', `Topic publish failed`, error);
        return {
          puback: {
            reasonCode: MQTT.PAYLOAD_INVALID,
            properties:{
              reasonString: 'Invalid payload'
            }
          }
        };
      }
    }
  });

};

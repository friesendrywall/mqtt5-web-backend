const globby = require('globby');
const UrlPattern = require('url-pattern');
const { v4: uuidv4 } = require('uuid');
const mqEmitter = require('mqemitter');
const Qlobber = require('qlobber').Qlobber;
const clientConn = require('./client');
const MQTT5_ERR = require('./const.js');

const { relative } = require('node-core-lib/path');
const session = require('./session.js');

require('./typedefs.js');

const fetchMatcher = new Qlobber({
  wildcard_one: '+',
  wildcard_some: '#',
  separator: '/'
});

function noop () {
}

/**
 *
 * @param {string} globalProcessId
 * @param {server_opt_t|null} options
 * @returns {server|{fetcher: function(*, *): void, publication: function(*, *): void, connection: function(Socket): connection, subscription: function(*, *): void, autoloadModules: function(string[]=): Promise<void>}}
 */
const server = function (globalProcessId, options) {

  /** @typedef fetcher
   *  @param {string} matcher
   *  @function {[]} cb
   *  @function {Object[]} cb.load
   * */

  if (!(this instanceof server)) {
    return new server(globalProcessId, options);
  }

  const broker = this;
  const publications = [];
  /**
   *
   * @type {fetcher_t[]}
   */
  const fetchers = [];
  const subscriptions = [];
  this.clientList = [];
  this.userChange = null;
  this.farmChange = null;
  this.serverId = uuidv4(null, null, null);
  this.logFunction = options && options.log_func ? options.log_func : null;
  this.clientCallbacks = options && options.clientCallbacks ? options.clientCallbacks : null;
  this.auth_func = options && options.auth_func ? options.auth_func : null;
  this.metadata_func = options && options.metadata_func ? options.metadata_func : null;
  this.module_dir = options && options.module_dir ? options.module_dir : __dirname;

  this.mq = mqEmitter({
    concurrency: 250,
  });

  this.persist = new session(
      {
        log_func: options.log_func,
        ttl: 3600
      }
  );
  this.fetchMatcher = fetchMatcher;
  this.counter = 1;

  const heartbeat = function () {
    broker.publish({
      topic: '$SYS/' + broker.serverId + '/heartbeat',
      payload: Date.now().toString()
    }, noop);
    // Persistence cleanup
    const expiredSessions = broker.persist.getExpiredSessions();
    broker.persist.deleteSessions(expiredSessions);
  };

  const _heartbeatInterval = setInterval(heartbeat,
      options && options.heartbeat_time ? options.heartbeat_time * 1000 : 30 * 1000);

  const subscription = function (topic, cb) {
    subscriptions.push({
      topic,
      cb,
      pattern: new UrlPattern(topic)
    });
  };

  /**
   *
   * @param {string} matcher
   * @param {fetcher_callback_t} cb
   */
  const fetcher = function (matcher, cb) {
    // Insert index of fetcher into qlobber
    fetchMatcher.add(matcher, fetchers.length);
    fetchers.push({
      matcher,
      cb
    });
  };

  const publication = function(topic, cb){
    publications.push({
      topic,
      cb,
      pattern: new UrlPattern(topic)
    });
  };

  /**
   *
   * @param {string} topic
   * @returns {{handler: publication|null, params: null}}
   */
  this.findPublication = function (topic) {
    let params = null;
    let handler = null;
    let i;
    for (i = 0; i < publications.length; i++) {
      params = publications[i].pattern.match(topic);
      if (params) {
        handler = publications[i];
        break;
      }
    }
    return {
      params,
      handler,
      index: i
    };
  };

  /**
   *
   * @param {string} topic
   * @returns {{handler: null, params: null}}
   */
  this.findSubscription = function (topic) {
    let params = null;
    let handler = null;
    for (let j = 0; j < subscriptions.length; j++) {
      params = subscriptions[j].pattern.match(topic);
      if (params) {
        handler = subscriptions[j];
        break;
      }
    }
    return {
      params,
      handler
    };
  };

  this.retrieveFetchers = function (topic) {
    const indexes = broker.fetchMatcher.match(topic);
    const ret = [];
    for (let i = 0; i < indexes.length; i++) {
      ret.push(fetchers[indexes[i]]);
    }
    return ret;
  };

  this.getClientTopics = function (stringify = true) {
    return {
      topic: `$SYS/${broker.serverId}/clients/`,
      payload: stringify
          ? JSON.stringify(Object.keys(broker.clientList))
          : Object.keys(broker.clientList),
      qos: 0
    };
  };

  this.closeClient = function (clientId) {
    delete broker.clientList[clientId];
    const pkt = broker.getClientTopics(true);
    broker.publish(pkt, noop);
    broker.publish(
        {
          topic: '$SYS/' + broker.serverId + '/disconnect/clients',
          payload: Buffer.from(clientId, 'utf8')
        },
        noop
    );
  };

  this.addClient = function (clientId, uuid) {
    broker.clientList[clientId] = uuid;
    broker.publish(
        {
          topic: '$SYS/' + broker.serverId + '/new/clients',
          payload: Buffer.from(clientId, 'utf8')
        },
        noop
    );
    const pkt = broker.getClientTopics(true);
    broker.publish(pkt, noop);
  };

  /**
   *
   * @param {Socket} stream
   */
  const connection = function (stream) {
    return new clientConn(stream, broker, { deduplicate: true });
  };

  /**
   * LOGUX code
   * @param pattern
   * @returns {Promise<void>}
   */
  const autoloadModules = async function (
      pattern = ['modules/*/index.js', 'modules/*.js']
  ) {
    let matches = await globby(pattern, /** @type GlobbyOptions */{
      cwd: broker.module_dir,
      absolute: true,
      onlyFiles: true
    });
    if (process.env.NODE_ENV === 'development') {
      if (matches.length === 0) {
        broker.log('No matches, check directory name');
      }
    }
    for (let modulePath of matches) {
      if (process.env.NODE_ENV === 'development') {
        broker.log('Loaded from ', modulePath);
      }
      let serverModule = require(modulePath);
      if (typeof serverModule === 'function') {
        serverModule(this);
      } else {
        let moduleName = relative(broker.module_dir, modulePath);

        let error = new Error(
            'Server module should export ' + 'a function that accepts a server.'
        );
        error.note =
            `Your module ${moduleName} ` + `exports ${typeof serverModule}.`;

        throw error;
      }
    }
  };

  this.publish = function (packet, done) {
    this.mq.emit(packet, done);
  };

  this.subscribe = function (topic, func, done) {
    this.mq.on(topic, func, done);
  };

  this.unsubscribe = function (topic, func, done) {
    console.log(this);
    this.mq.removeListener(topic, func, done);
  };

  /**
   * Log function call
   * @param args
   */
  this.log = function (...args) {
    if (broker.logFunction) {
      broker.logFunction.apply(this, args);
    }
  };

  return {
    connection,
    subscription,
    publication,
    fetcher,
    autoloadModules// ,
    // getClientTopics: this.getClientTopics
  };

};

module.exports = server;
module.exports.MQTT5_ERR = MQTT5_ERR;

/* JSDOC type defs */

/**
 * @typedef log_if_t
 * @type {function}
 * @param {string} level
 * @param {string} message
 * @param {any} data
 */

/**
 * @typedef client_callback_t
 * @type {object}
 * @property {function} topicBuilder
 * @property {function} handler
 */

/**
 * @typedef server_opt_t
 * @type {object}
 * @property {log_if_t | null} log_func
 * @property {client_callback_t[] | null } clientCallbacks
 * @property {number} heartbeat_time
 * @property {function} auth_func
 * @property {function} metadata_func
 * @property {string|null} module_dir
 *
 */

/**
 * @typedef fetcher_callback_t
 * @type {object}
 * @property {function} load
 * @returns { Promise<{packets: Object[]}>}
 */

/**
 * @typedef fetcher_t
 * @type {object}
 * @property {string} matcher
 * @property {fetcher_callback_t} cb
 */

exports.unused = {};

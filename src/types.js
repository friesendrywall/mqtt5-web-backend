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
 *
 */

/**
 * @typedef fetcher_callback_t
 * @type {object}
 * @property {function} load
 */

/**
 * @typedef fetcher_t
 * @type {object}
 * @property {string} matcher
 * @property {fetcher_callback_t} cb
 */

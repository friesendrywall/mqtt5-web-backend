const jwt = require('jsonwebtoken');
const fs = require('fs');
const privateKey = fs.readFileSync(process.env.API_RSA_KEY);
const publicKey = fs.readFileSync(process.env.API_RSA_PUB_KEY);
const logger = require('lib/winston.js');
const FarmList = require('services/farm_list');

/**
 * @typedef jwtAuthToken
 * @property {number} user_id
 * @property {number} connectionId - Maps to aerware0_accounts.user_connections
 * @property {number} exp - unix time expiration
 */

/**
 *
 * @param {number} userId
 * @param {number} connectionId
 * @param {UserRoles} aerware_level
 * @returns {string}
 */
const generateToken = function (userId, connectionId, aerware_level) {

  return jwt.sign(
    {
      type: 'web',
      user_id: userId,
      connection_id: connectionId,
      aerware_level,
      exp: Math.floor(Date.now() / 1000) + (60 * 60)
    },
    privateKey,
    { algorithm: 'RS256' });
};

const generateAerlinkToken = function (device) {

  logger.log('debug', 'generateAerlinkToken', device);
  return jwt.sign(
    {
      type: 'gateway',
      device_id: device.id,
      farm_id: device.farm_id,
      location_id: device.location_id,
      guid: device.guid
    },
    privateKey,
    { algorithm: 'RS256' });
};

/**
 *
 * @param {string} token
 * @returns {jwtAuthToken|boolean}
 */
const verifyToken = function (token) {
  try {
    const decoded = jwt.verify(token, publicKey, { algorithms: ['RS256'] });
    // TODO Verify against db here
    return decoded;
  } catch (err) {
    logger.log('error', 'verifyToken', { error: err.toString() });
    return false;
  }
};

module.exports = {
  generateToken,
  generateAerlinkToken,
  verifyToken
};

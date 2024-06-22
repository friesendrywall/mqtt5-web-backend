const jwt = require('jsonwebtoken');
const fs = require('fs');
const privateKey = fs.readFileSync(process.env.API_RSA_KEY);
const publicKey = fs.readFileSync(process.env.API_RSA_PUB_KEY);

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
 * @returns {string}
 */
const generateToken = function (userId, connectionId) {

  return jwt.sign(
    {
      type: 'web',
      user_id: userId,
      connection_id: connectionId,
      exp: Math.floor(Date.now() / 1000) + (60 * 60)
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
    console.log('error', 'verifyToken', { error: err.toString() });
    return false;
  }
};

module.exports = {
  generateToken,
  verifyToken
};

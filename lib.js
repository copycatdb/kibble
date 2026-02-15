// @copycatdb/kibble — high-level wrapper
// Loads the native addon and wraps query() with the fast buffer-based path.

const native = require('./index.js');
const { decodeBuffer } = require('./decode.js');

class Client {
  constructor(connectionString) {
    this._native = new native.Client(connectionString);
  }

  async connect() {
    return this._native.connect();
  }

  async query(sql, params) {
    const buf = await this._native.queryRaw(sql, params);
    return decodeBuffer(buf);
  }

  async execute(sql, params) {
    return this._native.execute(sql, params);
  }

  async close() {
    return this._native.close();
  }

  async end() {
    return this._native.end();
  }
}

module.exports = { Client };

const { Client: NativeClient } = require('./native.js');

class Client {
  constructor(connectionString) {
    this._native = new NativeClient(connectionString);
  }

  connect() {
    return this._native.connect();
  }

  async query(sql, params) {
    const result = await this._native.query(sql, params);
    const colNames = result.columns.map(c => c.name);
    const rows = result.rows.map(row => {
      const obj = {};
      for (let i = 0; i < colNames.length; i++) {
        obj[colNames[i]] = row[i] === undefined ? null : row[i];
      }
      return obj;
    });
    return {
      rows,
      columns: result.columns,
      rowCount: result.rowCount,
    };
  }

  execute(sql, params) {
    return this._native.execute(sql, params);
  }

  close() {
    return this._native.close();
  }

  end() {
    return this._native.end();
  }
}

module.exports = { Client };
module.exports.Client = Client;

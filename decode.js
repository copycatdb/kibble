// Fast binary decoder for query_raw results — optimized hot path
// Format: [u32 col_count][u32 row_count][u32 string_table_len][i64 rows_affected]
//         [columns: type_id(u8) + name_len(u16) + name_bytes]
//         [string_table: len(u32) + bytes each]
//         [cells: tag(u8) + payload per cell]

const COL_TYPE_NAMES = [
  'null', 'bit', 'tinyint', 'smallint', 'int', 'bigint', 'int',
  'real', 'float', 'float', 'datetime', 'datetimeoffset', 'date', 'time',
  'decimal', 'uniqueidentifier', 'nvarchar', 'varchar', 'varbinary',
  'xml', 'money', 'udt', 'sql_variant',
];

function decodeBuffer(buf) {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  let off = 0;

  const colCount = dv.getUint32(off, true); off += 4;
  const rowCount = dv.getUint32(off, true); off += 4;
  const strTableLen = dv.getUint32(off, true); off += 4;
  const raLow = dv.getUint32(off, true); off += 4;
  const raHigh = dv.getInt32(off, true); off += 4;

  // Column definitions
  const columns = new Array(colCount);
  const colNames = new Array(colCount);
  for (let i = 0; i < colCount; i++) {
    const typeId = buf[off++];
    const nameLen = buf[off] | (buf[off + 1] << 8); off += 2;
    // Decode short ASCII strings inline (column names are usually ASCII)
    let name;
    if (nameLen <= 32) {
      name = '';
      for (let j = 0; j < nameLen; j++) name += String.fromCharCode(buf[off + j]);
    } else {
      name = Buffer.from(buf.buffer, buf.byteOffset + off, nameLen).toString('utf8');
    }
    off += nameLen;
    columns[i] = { name, type: COL_TYPE_NAMES[typeId] || 'unknown' };
    colNames[i] = name;
  }

  // String table - decode all strings upfront
  const strings = new Array(strTableLen);
  for (let i = 0; i < strTableLen; i++) {
    const len = dv.getUint32(off, true); off += 4;
    strings[i] = Buffer.from(buf.buffer, buf.byteOffset + off, len).toString('utf8');
    off += len;
  }

  // Decode cells - tight loop, avoid function calls
  const rows = new Array(rowCount);
  const b = buf; // local alias for speed

  if (colCount === 0) {
    return { rows: [], columns: [], rowCount: 0 };
  }

  for (let r = 0; r < rowCount; r++) {
    const row = {};
    for (let c = 0; c < colCount; c++) {
      const tag = b[off++];
      if (tag === 0) { // null
        row[colNames[c]] = null;
      } else if (tag === 3) { // f64
        row[colNames[c]] = dv.getFloat64(off, true); off += 8;
      } else if (tag === 5) { // string ref
        row[colNames[c]] = strings[b[off] | (b[off+1] << 8) | (b[off+2] << 16) | (b[off+3] << 24)]; off += 4;
      } else if (tag === 1) { // false
        row[colNames[c]] = false;
      } else if (tag === 2) { // true
        row[colNames[c]] = true;
      } else if (tag === 4) { // bigint
        row[colNames[c]] = dv.getBigInt64(off, true); off += 8;
      } else { // bytes (tag 6)
        const len = dv.getUint32(off, true); off += 4;
        row[colNames[c]] = Buffer.from(buf.buffer, buf.byteOffset + off, len); off += len;
      }
    }
    rows[r] = row;
  }

  return { rows, columns, rowCount };
}

module.exports = { decodeBuffer };

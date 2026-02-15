import { Client } from '../index.js';
import { Connection, Request, TYPES } from 'tedious';

const CONN_STR = 'Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes';

// ── tedious helpers (from bench_vs_tedious.mjs) ──
function parseTediousConfig(connStr) {
  const kv = {};
  for (const part of connStr.split(';')) {
    const [k, v] = part.split('=').map(s => s?.trim());
    if (k && v) kv[k.toLowerCase()] = v;
  }
  const [host, port] = (kv.server || 'localhost').split(',');
  return {
    server: host,
    options: {
      port: parseInt(port || '1433'),
      database: kv.database || 'master',
      trustServerCertificate: true,
      encrypt: true,
      rowCollectionOnDone: true,
      rowCollectionOnRequestCompletion: true,
    },
    authentication: {
      type: 'default',
      options: { userName: kv.uid || '', password: kv.pwd || '' },
    },
  };
}

function tediousConnect() {
  return new Promise((resolve, reject) => {
    const conn = new Connection(parseTediousConfig(CONN_STR));
    conn.on('connect', (err) => err ? reject(err) : resolve(conn));
    conn.connect();
  });
}

function tediousQuery(conn, sql) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const req = new Request(sql, (err, rowCount) => {
      if (err) return reject(err);
      resolve({ rows, rowCount });
    });
    req.on('row', (cols) => {
      rows.push(cols.map(c => c.value));
    });
    conn.execSql(req);
  });
}

function tediousClose(conn) {
  return new Promise((resolve) => { conn.on('end', resolve); conn.close(); });
}

// ── Column generation helpers ──
function generateColumns(numCols) {
  const types = ['INT', 'NVARCHAR(50)', 'FLOAT', 'BIT', 'DATETIME2'];
  const cols = ['id INT PRIMARY KEY'];
  for (let i = 1; i < numCols; i++) {
    const t = types[i % types.length];
    cols.push(`col_${i} ${t}`);
  }
  return cols;
}

function generateInsertValues(numCols) {
  // Returns a SQL expression for one row using variable n
  const types = ['INT', 'NVARCHAR(50)', 'FLOAT', 'BIT', 'DATETIME2'];
  const vals = ['n']; // id
  for (let i = 1; i < numCols; i++) {
    const t = types[i % types.length];
    switch (t) {
      case 'INT': vals.push(`n + ${i}`); break;
      case 'NVARCHAR(50)': vals.push(`CONCAT(N'val_', n, '_${i}')`); break;
      case 'FLOAT': vals.push(`n * 1.${i}`); break;
      case 'BIT': vals.push(`CAST(n % 2 AS BIT)`); break;
      case 'DATETIME2': vals.push(`DATEADD(SECOND, n + ${i}, '2024-01-01')`); break;
    }
  }
  return vals.join(', ');
}

// ── Scenarios ──
const scenarios = [
  { name: 'Narrow 4 cols × 1M rows',    cols: 4,   rows: 1000000 },
  { name: 'Medium 20 cols × 500K rows',  cols: 20,  rows: 500000  },
  { name: 'Wide 50 cols × 100K rows',    cols: 50,  rows: 100000  },
  { name: 'Wide 100 cols × 50K rows',    cols: 100, rows: 50000   },
  { name: 'Very wide 200 cols × 10K rows', cols: 200, rows: 10000 },
];

async function main() {
  const kibble = new Client(CONN_STR);
  await kibble.connect();
  const tedious = await tediousConnect();

  const results = [];

  for (const sc of scenarios) {
    const tableName = `bench_wide_${sc.cols}`;
    console.log(`\n── Setting up ${sc.name} ──`);

    // Create table
    const colDefs = generateColumns(sc.cols);
    await kibble.query(`IF OBJECT_ID('${tableName}', 'U') IS NOT NULL DROP TABLE ${tableName}`);
    await kibble.query(`CREATE TABLE ${tableName} (${colDefs.join(', ')})`);

    // Insert data in batches using cross join trick
    const vals = generateInsertValues(sc.cols);
    const batchSize = 50000;
    let inserted = 0;
    while (inserted < sc.rows) {
      const count = Math.min(batchSize, sc.rows - inserted);
      await kibble.query(`
        INSERT INTO ${tableName}
        SELECT ${vals}
        FROM (
          SELECT TOP ${count} ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + ${inserted} AS n
          FROM sys.objects a CROSS JOIN sys.objects b CROSS JOIN sys.objects c
        ) t
      `);
      inserted += count;
      process.stdout.write(`  Inserted ${inserted}/${sc.rows}\r`);
    }
    console.log(`  Inserted ${inserted} rows.`);

    // Benchmark kibble
    console.log('  Timing kibble...');
    const kStart = process.hrtime.bigint();
    const kResult = await kibble.query(`SELECT * FROM ${tableName}`);
    const kTime = Number(process.hrtime.bigint() - kStart) / 1e6;
    console.log(`  kibble: ${kTime.toFixed(0)}ms (${kResult.length} rows)`);

    // Benchmark tedious
    console.log('  Timing tedious...');
    const tStart = process.hrtime.bigint();
    const tResult = await tediousQuery(tedious, `SELECT * FROM ${tableName}`);
    const tTime = Number(process.hrtime.bigint() - tStart) / 1e6;
    console.log(`  tedious: ${tTime.toFixed(0)}ms (${tResult.rows.length} rows)`);

    const speedup = tTime / kTime;
    results.push({ ...sc, kTime, tTime, speedup });

    // Cleanup
    await kibble.query(`DROP TABLE ${tableName}`);
  }

  // Print results table
  console.log('\n\n' + '='.repeat(90));
  console.log('WIDE-COLUMN BENCHMARK RESULTS');
  console.log('='.repeat(90));
  const hdr = `${'Scenario'.padEnd(35)} ${'Cols'.padStart(5)} ${'Rows'.padStart(8)} ${'Kibble'.padStart(10)} ${'Tedious'.padStart(10)} ${'Speedup'.padStart(10)} ${'Winner'.padStart(8)}`;
  console.log(hdr);
  console.log('-'.repeat(90));

  for (const r of results) {
    const winner = r.speedup >= 1 ? 'kibble' : 'tedious';
    const ratio = r.speedup >= 1 ? `${r.speedup.toFixed(2)}x` : `${(1/r.speedup).toFixed(2)}x`;
    console.log(
      `${r.name.padEnd(35)} ${String(r.cols).padStart(5)} ${String(r.rows).padStart(8)} ${(r.kTime.toFixed(0)+'ms').padStart(10)} ${(r.tTime.toFixed(0)+'ms').padStart(10)} ${ratio.padStart(10)} ${winner.padStart(8)}`
    );
  }
  console.log('='.repeat(90));
  console.log('\nSpeedup = tedious_time / kibble_time (>1 means kibble is faster)');

  await kibble.close();
  await tediousClose(tedious);
  console.log('Done.');
}

main().catch(e => { console.error(e); process.exit(1); });

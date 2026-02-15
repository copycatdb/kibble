import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { Client } = require('../index.js');
const tedious = require('tedious');

const CONN_STR = 'Server=localhost,1433;User Id=sa;Password=TestPass123!;TrustServerCertificate=true;Database=tempdb';

// ── Tedious helpers ──
function tediousConnect() {
  return new Promise((resolve, reject) => {
    const cfg = {
      server: 'localhost',
      authentication: { type: 'default', options: { userName: 'sa', password: 'TestPass123!' } },
      options: { port: 1433, trustServerCertificate: true, database: 'tempdb', rowCollectionOnRequestCompletion: true },
    };
    const conn = new tedious.Connection(cfg);
    conn.on('connect', err => (err ? reject(err) : resolve(conn)));
    conn.connect();
  });
}

function tediousQuery(conn, sql) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const req = new tedious.Request(sql, (err, rowCount, tRows) => {
      if (err) return reject(err);
      // tRows is array of arrays of column objects
      const result = (tRows || []).map(r => {
        const obj = {};
        for (const col of r) obj[col.metadata.colName] = col.value;
        return obj;
      });
      resolve(result);
    });
    conn.execSql(req);
  });
}

function tediousExec(conn, sql) {
  return new Promise((resolve, reject) => {
    const req = new tedious.Request(sql, (err) => err ? reject(err) : resolve());
    conn.execSql(req);
  });
}

function tediousClose(conn) {
  return new Promise(resolve => { conn.on('end', resolve); conn.close(); });
}

// ── Benchmark runner ──
async function bench(name, fn, warmup = 0) {
  for (let i = 0; i < warmup; i++) await fn();
  const t0 = performance.now();
  const result = await fn();
  const ms = performance.now() - t0;
  return { name, ms, rows: result?.length ?? result?.rows?.length ?? 0 };
}

async function main() {
  console.log('=== Kibble vs Tedious: 1M Row Benchmark ===\n');

  // ── Setup: connect both drivers ──
  console.log('Connecting kibble...');
  const kb = new Client(CONN_STR);
  await kb.connect();
  console.log('Connecting tedious...');
  const td = await tediousConnect();

  // ── Create and populate 1M row table ──
  console.log('Creating table with 1M rows (this may take a minute)...');
  await kb.execute(`
    IF OBJECT_ID('tempdb..bench_1m') IS NOT NULL DROP TABLE bench_1m;
    CREATE TABLE bench_1m (
      id INT NOT NULL PRIMARY KEY,
      name NVARCHAR(100),
      value FLOAT,
      created DATETIME2
    );
  `);

  // Insert in batches of 50K using a numbers CTE
  const BATCH = 50000;
  const TOTAL = 1000000;
  for (let offset = 0; offset < TOTAL; offset += BATCH) {
    await kb.execute(`
      WITH nums AS (
        SELECT TOP ${BATCH} ${offset} + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.all_objects a CROSS JOIN sys.all_objects b
      )
      INSERT INTO bench_1m (id, name, value, created)
      SELECT n,
             CONCAT(N'user_', n),
             CAST(n AS FLOAT) * 1.1,
             DATEADD(SECOND, n % 86400, '2025-01-01')
      FROM nums;
    `);
    if ((offset + BATCH) % 200000 === 0) console.log(`  ${offset + BATCH} rows inserted...`);
  }

  const countResult = await kb.query('SELECT COUNT(*) AS cnt FROM bench_1m');
  console.log(`Table ready: ${countResult.rows[0].cnt} rows\n`);

  // ── Benchmark 1: SELECT all 1M rows ──
  console.log('--- Benchmark 1: SELECT * FROM bench_1m (1M rows) ---');

  const kb1 = await bench('kibble-1M', async () => {
    return await kb.query('SELECT id, name, value, created FROM bench_1m');
  });
  console.log(`  kibble:  ${kb1.ms.toFixed(0)} ms (${kb1.rows} rows)`);

  const td1 = await bench('tedious-1M', async () => {
    return await tediousQuery(td, 'SELECT id, name, value, created FROM bench_1m');
  });
  console.log(`  tedious: ${td1.ms.toFixed(0)} ms (${td1.rows} rows)`);
  console.log(`  speedup: ${(td1.ms / kb1.ms).toFixed(2)}x\n`);

  // ── Benchmark 2: SELECT ~10K rows with WHERE ──
  console.log('--- Benchmark 2: SELECT WHERE id BETWEEN 500000 AND 510000 (~10K rows) ---');

  const RUNS = 5;
  let kbTotal = 0, tdTotal = 0, kbRows = 0, tdRows = 0;
  for (let i = 0; i < RUNS; i++) {
    const r1 = await bench('kb', () => kb.query('SELECT id, name, value, created FROM bench_1m WHERE id BETWEEN 500000 AND 510000'));
    const r2 = await bench('td', () => tediousQuery(td, 'SELECT id, name, value, created FROM bench_1m WHERE id BETWEEN 500000 AND 510000'));
    kbTotal += r1.ms; tdTotal += r2.ms;
    kbRows = r1.rows; tdRows = r2.rows;
  }
  const kbAvg = kbTotal / RUNS, tdAvg = tdTotal / RUNS;
  console.log(`  kibble:  ${kbAvg.toFixed(0)} ms avg (${kbRows} rows, ${RUNS} runs)`);
  console.log(`  tedious: ${tdAvg.toFixed(0)} ms avg (${tdRows} rows, ${RUNS} runs)`);
  console.log(`  speedup: ${(tdAvg / kbAvg).toFixed(2)}x\n`);

  // ── Cleanup ──
  await kb.execute('DROP TABLE IF EXISTS bench_1m');
  await kb.close();
  await tediousClose(td);

  console.log('Done.');
}

main().catch(e => { console.error(e); process.exit(1); });

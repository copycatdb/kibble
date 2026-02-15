import { Client } from '../index.js';
import { Connection, Request, TYPES } from 'tedious';

const CONN_STR = process.env.DB_CONNECTION_STRING
  || 'Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes';

// ── tedious helpers ────────────────────────────────────────────────
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
      trustServerCertificate: (kv.trustservercertificate || '').toLowerCase() === 'yes',
      encrypt: true,
      rowCollectionOnDone: true,
      rowCollectionOnRequestCompletion: true,
    },
    authentication: {
      type: 'default',
      options: {
        userName: kv.uid || kv['user id'] || '',
        password: kv.pwd || kv.password || '',
      },
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
  return new Promise((resolve) => {
    conn.on('end', resolve);
    conn.close();
  });
}

// ── benchmark helper ───────────────────────────────────────────────
async function timeFn(fn, { warmup = 3, iterations = 20 } = {}) {
  for (let i = 0; i < warmup; i++) await fn();
  const times = [];
  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint();
    await fn();
    const end = process.hrtime.bigint();
    times.push(Number(end - start) / 1e6); // ms
  }
  times.sort((a, b) => a - b);
  // median
  return times[Math.floor(times.length / 2)];
}

// ── setup / cleanup ────────────────────────────────────────────────
async function setup(kibbleClient, tediousConn) {
  await kibbleClient.query(`
    IF OBJECT_ID('bench_data', 'U') IS NOT NULL DROP TABLE bench_data;
    CREATE TABLE bench_data (
      id INT PRIMARY KEY,
      name NVARCHAR(100),
      value FLOAT,
      created DATETIME2
    );
    INSERT INTO bench_data (id, name, value, created)
    SELECT n, CONCAT(N'item_', n), n * 1.1, DATEADD(SECOND, n, '2024-01-01')
    FROM (SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
          FROM sys.objects a CROSS JOIN sys.objects b) t;
  `);

  await kibbleClient.query(`
    IF OBJECT_ID('bench_write', 'U') IS NOT NULL DROP TABLE bench_write;
    CREATE TABLE bench_write (id INT, name NVARCHAR(100));
  `);
}

async function cleanup(kibbleClient) {
  await kibbleClient.query('IF OBJECT_ID(\'bench_data\', \'U\') IS NOT NULL DROP TABLE bench_data');
  await kibbleClient.query('IF OBJECT_ID(\'bench_write\', \'U\') IS NOT NULL DROP TABLE bench_write');
}

// ── run benchmarks ─────────────────────────────────────────────────
async function main() {
  const kibble = new Client(CONN_STR);
  await kibble.connect();
  const tedious = await tediousConnect();

  console.log('Setting up benchmark tables...\n');
  await setup(kibble, tedious);

  const results = [];

  async function bench(label, kibbleFn, tediousFn) {
    const kTime = await timeFn(kibbleFn);
    const tTime = await timeFn(tediousFn);
    const winner = kTime < tTime ? 'kibble' : 'tedious';
    const ratio = kTime < tTime ? (tTime / kTime).toFixed(1) + 'x' : (kTime / tTime).toFixed(1) + 'x';
    results.push({ label, kTime, tTime, winner, ratio });
  }

  // 1. Simple SELECT
  await bench('SELECT 1',
    () => kibble.query('SELECT 1 AS n'),
    () => tediousQuery(tedious, 'SELECT 1 AS n')
  );

  // 2. SELECT with data
  await bench('SELECT (10 rows)',
    () => kibble.query('SELECT TOP 10 * FROM bench_data'),
    () => tediousQuery(tedious, 'SELECT TOP 10 * FROM bench_data')
  );

  // 3. Fetch 100 rows
  await bench('Fetch 100 rows',
    () => kibble.query('SELECT TOP 100 * FROM bench_data'),
    () => tediousQuery(tedious, 'SELECT TOP 100 * FROM bench_data')
  );

  // 4. Fetch 1000 rows
  await bench('Fetch 1000 rows',
    () => kibble.query('SELECT * FROM bench_data'),
    () => tediousQuery(tedious, 'SELECT * FROM bench_data')
  );

  // 5. INSERT
  await bench('INSERT',
    async () => {
      await kibble.query("INSERT INTO bench_write VALUES (1, N'test')");
      await kibble.query('DELETE FROM bench_write');
    },
    async () => {
      await tediousQuery(tedious, "INSERT INTO bench_write VALUES (1, N'test')");
      await tediousQuery(tedious, 'DELETE FROM bench_write');
    }
  );

  // 6. Complex query
  await bench('Complex query',
    () => kibble.query(`
      SELECT TOP 10 d1.name, d1.value, d2.name AS name2
      FROM bench_data d1
      JOIN bench_data d2 ON d1.id = d2.id + 1
      WHERE d1.value > 5
      ORDER BY d1.value DESC
    `),
    () => tediousQuery(tedious, `
      SELECT TOP 10 d1.name, d1.value, d2.name AS name2
      FROM bench_data d1
      JOIN bench_data d2 ON d1.id = d2.id + 1
      WHERE d1.value > 5
      ORDER BY d1.value DESC
    `)
  );

  // 7. Connection time
  const kibbleConnTime = await timeFn(async () => {
    const c = new Client(CONN_STR);
    await c.connect();
    await c.close();
  }, { warmup: 2, iterations: 10 });

  const tediousConnTime = await timeFn(async () => {
    const c = await tediousConnect();
    await tediousClose(c);
  }, { warmup: 2, iterations: 10 });

  results.push({
    label: 'Connect+close',
    kTime: kibbleConnTime,
    tTime: tediousConnTime,
    winner: kibbleConnTime < tediousConnTime ? 'kibble' : 'tedious',
    ratio: kibbleConnTime < tediousConnTime
      ? (tediousConnTime / kibbleConnTime).toFixed(1) + 'x'
      : (kibbleConnTime / tediousConnTime).toFixed(1) + 'x',
  });

  // Print results
  const hdr = `${'Benchmark'.padEnd(20)}  ${'kibble'.padStart(10)}  ${'tedious'.padStart(10)}  ${'winner'.padStart(10)}  ${'speedup'.padStart(8)}`;
  console.log(hdr);
  console.log('='.repeat(hdr.length));

  let kibbleWins = 0, tediousWins = 0;
  for (const r of results) {
    const kStr = r.kTime < 10 ? `${r.kTime.toFixed(2)}ms` : `${r.kTime.toFixed(1)}ms`;
    const tStr = r.tTime < 10 ? `${r.tTime.toFixed(2)}ms` : `${r.tTime.toFixed(1)}ms`;
    console.log(`${r.label.padEnd(20)}  ${kStr.padStart(10)}  ${tStr.padStart(10)}  ${r.winner.padStart(10)}  ${r.ratio.padStart(8)}`);
    if (r.winner === 'kibble') kibbleWins++; else tediousWins++;
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`WINS: kibble ${kibbleWins} | tedious ${tediousWins}`);
  console.log(`\nNote: These numbers are relative, not absolute. Results vary by`);
  console.log(`hardware, OS, SQL Server version, and CI runner specs.`);
  console.log(`Use for directional comparison only.`);

  // Cleanup
  console.log('\nCleaning up...');
  await cleanup(kibble);
  await kibble.close();
  await tediousClose(tedious);
  console.log('Done.');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

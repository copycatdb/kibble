import { Client } from './index.js';
import { Connection, Request } from 'tedious';

const CONN = 'Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes';

function tediousConnect() {
  return new Promise((resolve, reject) => {
    const conn = new Connection({
      server: 'localhost',
      options: { port: 1433, database: 'master', trustServerCertificate: true, encrypt: true, rowCollectionOnDone: true, rowCollectionOnRequestCompletion: true },
      authentication: { type: 'default', options: { userName: 'sa', password: 'TestPass123!' } },
    });
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
      const row = {};
      for (const c of cols) row[c.metadata.colName] = c.value;
      rows.push(row);
    });
    conn.execSql(req);
  });
}

function tediousExec(conn, sql) {
  return new Promise((resolve, reject) => {
    const req = new Request(sql, (err) => err ? reject(err) : resolve());
    conn.execSql(req);
  });
}

function tediousClose(conn) {
  return new Promise(resolve => { conn.on('end', resolve); conn.close(); });
}

async function bench(label, fn, iterations = 5) {
  // warmup
  await fn();
  await fn();
  const times = [];
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await fn();
    times.push(performance.now() - t0);
  }
  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)];
  return median;
}

const k = new Client(CONN);
await k.connect();
const t = await tediousConnect();

// Setup tables
async function setupTable(cols, rows) {
  const colDefs = [];
  const colSelects = [];
  for (let i = 0; i < cols; i++) {
    if (i === 0) {
      colDefs.push(`c0 INT`);
      colSelects.push(`ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) as c0`);
    } else if (i % 3 === 1) {
      colDefs.push(`c${i} NVARCHAR(50)`);
      colSelects.push(`CONCAT(N'val_', ABS(CHECKSUM(NEWID())) % 100) as c${i}`);
    } else if (i % 3 === 2) {
      colDefs.push(`c${i} FLOAT`);
      colSelects.push(`CAST(ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) * 1.1 AS FLOAT) as c${i}`);
    } else {
      colDefs.push(`c${i} INT`);
      colSelects.push(`ABS(CHECKSUM(NEWID())) % 10000 as c${i}`);
    }
  }
  
  await k.query(`
    IF OBJECT_ID('bench_tbl','U') IS NOT NULL DROP TABLE bench_tbl;
    SELECT TOP ${rows} ${colSelects.join(', ')}
    INTO bench_tbl
    FROM sys.objects a CROSS JOIN sys.objects b CROSS JOIN sys.objects d CROSS JOIN sys.objects e;
  `);
  
  const cnt = await k.query('SELECT COUNT(*) as cnt FROM bench_tbl');
  return cnt.rows[0].cnt;
}

const scenarios = [
  { cols: 4, rows: 1000000 },
  { cols: 20, rows: 500000 },
  { cols: 50, rows: 100000 },
];

console.log('Benchmark: kibble (optimized) vs tedious');
console.log('='.repeat(70));

for (const { cols, rows } of scenarios) {
  const actual = await setupTable(cols, rows);
  console.log(`\n--- ${cols} cols × ${actual} rows ---`);
  
  const kTime = await bench(`kibble`, () => k.query('SELECT * FROM bench_tbl'), 3);
  const tTime = await bench(`tedious`, () => tediousQuery(t, 'SELECT * FROM bench_tbl'), 3);
  
  const winner = kTime < tTime ? 'KIBBLE' : 'TEDIOUS';
  const ratio = kTime < tTime ? (tTime / kTime).toFixed(2) : (kTime / tTime).toFixed(2);
  console.log(`  kibble:  ${kTime.toFixed(0)}ms`);
  console.log(`  tedious: ${tTime.toFixed(0)}ms`);
  console.log(`  winner:  ${winner} (${ratio}x faster)`);
}

await k.query('IF OBJECT_ID(\'bench_tbl\',\'U\') IS NOT NULL DROP TABLE bench_tbl');
await k.close();
await tediousClose(t);
console.log('\nDone.');

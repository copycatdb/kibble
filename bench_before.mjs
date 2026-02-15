import { Client } from './index.js';

const CONN = 'Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes';
const c = new Client(CONN);
await c.connect();

await c.query(`
  IF OBJECT_ID('perf_test','U') IS NOT NULL DROP TABLE perf_test;
  SELECT TOP 100000
    ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) as id,
    CONCAT(N'name_', ROW_NUMBER() OVER(ORDER BY(SELECT NULL))) as name,
    CAST(ROW_NUMBER() OVER(ORDER BY(SELECT NULL)) * 1.1 AS FLOAT) as value,
    GETDATE() as created
  INTO perf_test
  FROM sys.objects a CROSS JOIN sys.objects b CROSS JOIN sys.objects d;
`);

const count = await c.query('SELECT COUNT(*) as cnt FROM perf_test');
console.log('Rows in table:', count.rows[0]);

// Warm up
await c.query('SELECT * FROM perf_test');

for (let trial = 0; trial < 3; trial++) {
  const start = performance.now();
  const r = await c.query('SELECT * FROM perf_test');
  const elapsed = performance.now() - start;
  console.log(`BEFORE Trial ${trial+1}: ${r.rows.length} rows in ${elapsed.toFixed(0)}ms`);
}

await c.query('DROP TABLE perf_test');
await c.close();

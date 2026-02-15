import { describe, it, expect, beforeAll, afterAll } from 'vitest';

const CONN_STR = process.env.DB_CONNECTION_STRING
  || 'Server=localhost,1433;Database=master;UID=sa;PWD=TestPass123!;TrustServerCertificate=yes';

// Dynamic import — the native addon is built during CI
let Client;

beforeAll(async () => {
  const mod = await import('../lib.js');
  Client = mod.Client;
});

describe('connection', () => {
  it('should connect and close', async () => {
    const client = new Client(CONN_STR);
    await client.connect();
    await client.close();
  });

  it('should reject invalid connection string', async () => {
    const client = new Client('Server=localhost,9999;UID=sa;PWD=wrong;TrustServerCertificate=yes');
    await expect(client.connect()).rejects.toThrow();
  });
});

describe('query', () => {
  let client;

  beforeAll(async () => {
    client = new Client(CONN_STR);
    await client.connect();
  });

  afterAll(async () => {
    if (client) await client.close();
  });

  it('SELECT 1', async () => {
    const result = await client.query('SELECT 1 AS num');
    expect(result.rowCount).toBe(1);
    expect(result.rows[0].num).toBe(1);
    expect(result.columns[0].name).toBe('num');
  });

  it('SELECT string', async () => {
    const result = await client.query("SELECT N'hello' AS greeting");
    expect(result.rows[0].greeting).toBe('hello');
  });

  it('SELECT multiple rows', async () => {
    const result = await client.query(
      'SELECT n FROM (VALUES (1),(2),(3),(4),(5)) AS t(n)'
    );
    expect(result.rowCount).toBe(5);
    expect(result.rows.map(r => r.n)).toEqual([1, 2, 3, 4, 5]);
  });

  it('SELECT NULL', async () => {
    const result = await client.query('SELECT NULL AS val');
    expect(result.rows[0].val).toBeNull();
  });

  it('empty result set', async () => {
    const result = await client.query('SELECT 1 AS n WHERE 1=0');
    expect(result.rowCount).toBe(0);
    expect(result.rows).toEqual([]);
  });
});

describe('data types', () => {
  let client;

  beforeAll(async () => {
    client = new Client(CONN_STR);
    await client.connect();
  });

  afterAll(async () => {
    if (client) await client.close();
  });

  it('integers', async () => {
    const result = await client.query(`
      SELECT
        CAST(127 AS TINYINT) AS ti,
        CAST(32767 AS SMALLINT) AS si,
        CAST(2147483647 AS INT) AS i,
        CAST(9223372036854775807 AS BIGINT) AS bi
    `);
    const row = result.rows[0];
    expect(row.ti).toBe(127);
    expect(row.si).toBe(32767);
    expect(row.i).toBe(2147483647);
    // bigint might be number or bigint depending on magnitude
    expect(Number(row.bi)).toBe(9223372036854775807);
  });

  it('floats', async () => {
    const result = await client.query(`
      SELECT CAST(3.14 AS FLOAT) AS f, CAST(2.5 AS REAL) AS r
    `);
    expect(result.rows[0].f).toBeCloseTo(3.14);
    expect(result.rows[0].r).toBeCloseTo(2.5);
  });

  it('bit', async () => {
    const result = await client.query('SELECT CAST(1 AS BIT) AS t, CAST(0 AS BIT) AS f');
    expect(result.rows[0].t).toBe(true);
    expect(result.rows[0].f).toBe(false);
  });

  it('nvarchar', async () => {
    const result = await client.query("SELECT N'café ☕ 日本語' AS txt");
    expect(result.rows[0].txt).toBe('café ☕ 日本語');
  });

  it('date', async () => {
    const result = await client.query("SELECT CAST('2024-05-20' AS DATE) AS d");
    expect(result.rows[0].d).toBe('2024-05-20');
  });

  it('datetime2', async () => {
    const result = await client.query("SELECT CAST('2024-05-20T12:34:56' AS DATETIME2) AS dt");
    expect(result.rows[0].dt).toContain('2024-05-20');
    expect(result.rows[0].dt).toContain('12:34:56');
  });

  it('uniqueidentifier', async () => {
    const result = await client.query("SELECT NEWID() AS guid");
    // Should be a UUID string
    expect(result.rows[0].guid).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
    );
  });

  it('decimal', async () => {
    const result = await client.query("SELECT CAST(123.456 AS DECIMAL(10,3)) AS d");
    expect(result.rows[0].d).toBe('123.456');
  });

  it('binary', async () => {
    const result = await client.query("SELECT CAST(0xDEADBEEF AS VARBINARY(4)) AS b");
    const buf = result.rows[0].b;
    expect(Buffer.isBuffer(buf)).toBe(true);
    expect(buf.toString('hex')).toBe('deadbeef');
  });

  it('time', async () => {
    const result = await client.query("SELECT CAST('12:34:56' AS TIME) AS t");
    expect(result.rows[0].t).toContain('12:34:56');
  });
});

describe('DML', () => {
  let client;

  beforeAll(async () => {
    client = new Client(CONN_STR);
    await client.connect();
    await client.query(`
      IF OBJECT_ID('tempdb..#kibble_test') IS NOT NULL DROP TABLE #kibble_test;
      CREATE TABLE #kibble_test (id INT PRIMARY KEY, name NVARCHAR(100));
    `);
  });

  afterAll(async () => {
    if (client) await client.close();
  });

  it('INSERT', async () => {
    await client.query("INSERT INTO #kibble_test VALUES (1, N'alice')");
    const result = await client.query('SELECT * FROM #kibble_test WHERE id=1');
    expect(result.rows[0].name).toBe('alice');
  });

  it('UPDATE', async () => {
    await client.query("UPDATE #kibble_test SET name=N'bob' WHERE id=1");
    const result = await client.query('SELECT name FROM #kibble_test WHERE id=1');
    expect(result.rows[0].name).toBe('bob');
  });

  it('DELETE', async () => {
    await client.query('DELETE FROM #kibble_test WHERE id=1');
    const result = await client.query('SELECT COUNT(*) AS cnt FROM #kibble_test');
    expect(result.rows[0].cnt).toBe(0);
  });
});

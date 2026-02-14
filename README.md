# kibble 🍚

Node.js driver for SQL Server. Feed your app data, one nugget at a time.

Part of [CopyCat](https://github.com/copycatdb) 🐱

## What is this?

A fast Node.js driver for SQL Server powered by [tabby](https://github.com/copycatdb/tabby) via N-API. No tedious. No ODBC. Just JavaScript and a cat.

```javascript
const { Client } = require("@copycatdb/kibble");

const client = new Client("Server=localhost,1433;UID=sa;PWD=pass;TrustServerCertificate=yes");
await client.connect();

const result = await client.query("SELECT * FROM users WHERE id = @p1", [42]);
console.log(result.rows[0].name);

await client.end();
```

## Why not tedious?

We love tedious. Tedious is great. But the name should have been a warning.

kibble is tedious without the... tedium. Backed by native Rust, non-blocking, and installs without downloading half of npm.

## Status

🚧 Coming soon.

## Attribution

Inspired by [node-postgres (pg)](https://github.com/brianc/node-postgres). The gold standard for Node database drivers.

## License

MIT

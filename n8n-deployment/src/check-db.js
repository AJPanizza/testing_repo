const { Client } = require('pg');

console.log('Attempting to connect to the database from outside n8n app');

// Construct a PostgreSQL connection string URI. This can be more reliable than a config object.
const user = process.env.DB_POSTGRESDB_USER;
const password = encodeURIComponent(process.env.DB_POSTGRESDB_PASSWORD); // URL-encode the password
const host = process.env.DB_POSTGRESDB_HOST;
const port = process.env.DB_POSTGRESDB_PORT || 5432;
const database = process.env.DB_POSTGRESDB_DATABASE;

if (!user || !password || !host || !database) {
  console.error('Missing required DB env vars. Expected DB_POSTGRESDB_USER, DB_POSTGRESDB_PASSWORD, DB_POSTGRESDB_HOST, DB_POSTGRESDB_DATABASE.');
  process.exit(1);
}

// The sslmode=require parameter is added directly to the URI.
const connectionString = `postgresql://${user}:${password}@${host}:${port}/${database}?sslmode=verify-full`;

console.log('Constructed connection string (with password hidden):');
console.log(`postgresql://${user}:****@${host}:${port}/${database}?sslmode=verify-full`);
console.log('');

const client = new Client({
  connectionString: connectionString,
  ssl: {
    rejectUnauthorized: false,
  },
  connectionTimeoutMillis: 10000, // Increased timeout for potentially slow network handshakes
});

console.log('Client information:');
console.log(client);
console.log('');

client.connect()
  .then(() => {
    console.log('Database connection successful.');
    return client.query('SELECT 1 AS ok');
  })
  .then(result => {
    console.log('Permission check query succeeded:', result.rows[0]);
    return client.query(
      "SELECT 1 AS ok FROM information_schema.schemata WHERE schema_name = 'n8n'"
    );
  })
  .then(result => {
    if (result.rowCount > 0) {
      console.log('Schema check succeeded: n8n schema is accessible.');
      return client.query(
        'CREATE TABLE IF NOT EXISTS n8n.permission_check (id INTEGER PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW())'
      );
    } else {
      console.error('Schema check failed: n8n schema not found or not accessible.');
      process.exit(1);
    }
  })
  .then(() => {
    console.log('Sample table check succeeded: n8n.permission_check is ready.');
    return client.end();
  })
  .catch(err => {
    console.error('Database connection failed!', err.stack);
    process.exit(1);
  });

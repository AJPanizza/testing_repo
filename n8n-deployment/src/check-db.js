// const { Client } = require('pg');

// const client = new Client({
//   // Connection Details
//   user: 'n8nuser@instance-c2dd621e-e2b7-4ac3-8fda-3e759e7e8761',
//   host: 'instance-c2dd621e-e2b7-4ac3-8fda-3e759e7e8761.database.cloud.databricks.com',
//   database: 'databricks_postgres',
//   password: 'n8ndemopsql',
//   port: 5432,

//   // THE FIX: This object is mandatory for Databricks
//   ssl: {
//     rejectUnauthorized: false
//   },
  
//   connectionTimeoutMillis: 10000,
// });

// client.connect()
//   .then(() => {
//     console.log('✅ CONNECTED: The SSL fix worked!');
//     return client.end();
//   })
//   .catch(err => {
//     console.error('❌ ERROR:', err.message);
//     // If you STILL get auth errors here, change 'user' to:
//     // 'n8nuser@instance-c2dd621e-e2b7-4ac3-8fda-3e759e7e8761'
//   });


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
const connectionString = `postgresql://${user}:${password}@${host}:${port}/${database}?sslmode=require`;

console.log('Constructed connection string (with password hidden):');
console.log(`postgresql://${user}:****@${host}:${port}/${database}?sslmode=require`);
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
    client.end();
  })
  .catch(err => {
    console.error('Database connection failed!', err.stack);
    process.exit(1);
  });

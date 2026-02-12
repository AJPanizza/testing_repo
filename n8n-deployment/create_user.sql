-- 1. Create a standard local user (No databricks_create_role needed!)
CREATE ROLE n8n_local_user
WITH LOGIN PASSWORD 'n8ndemopass';

-- 2. Create schema if not exists
CREATE SCHEMA
IF NOT EXISTS n8n;

-- 3. Grant permissions
GRANT CONNECT ON DATABASE databricks_postgres TO n8n_local_user;
GRANT CREATE ON DATABASE databricks_postgres TO n8n_local_user;

GRANT SELECT ON ALL TABLES IN SCHEMA n8n TO n8n_local_user;
GRANT ALL PRIVILEGES ON SCHEMA n8n TO n8n_local_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA n8n TO n8n_local_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA n8n TO n8n_local_user;

-- Optional: Ensure it works for future tables too
ALTER DEFAULT PRIVILEGES IN SCHEMA n8n
GRANT SELECT ON TABLES TO n8n_local_user;
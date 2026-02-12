# n8n deployment

This documentation provides a comprehensive guide for setting up and deploying a self-hosted n8n instance integrated with Databricks.

## Dependencies

- **Postgres (Lakebase)**: Database used by n8n instance for persisting user data.
- **Databricks Asset Bundles:** Used to deploy App resource in the Databricks Workspace.
- **Databricks Community Nodes Package:** Used to access Databricks resources as n8n blocks.
- **n8n:** Self-hosted version to develop agent workflows.

## Installation Steps

### 1. Set up Managed Postgres

In this case, we will use Lakebase Postgres to host the solution entirely on Databricks. Steps may vary if setting up a Postgres instance hosted elsewhere.

1. Create and provision Lakebase instance.
2. Once created, modify the instance to Enable **Postgres native role login**
3. Set a user with permissions over databricks_postgres database.

This database can be used for use cases inside n8n that require a Postgres database. Creating new schemas and users is recommended.

### 2. Create Databricks Secrets and deploy App

You will need to create the following secrets in a designated scope:

| **Name**           | **Key**            | **Permission** |
| ------------------ | ------------------ | -------------- |
| postgres-host      | postgres-host      | READ           |
| postgres-user      | postgres-user      | READ           |
| postgres-password  | postgres-password  | READ           |
| n8n-encryption-key | n8n-encryption-key | READ           |

**Step 1: Create the secret scope (if not already created):**

```bash
databricks secrets create-scope --scope <name-of-scope>
```

**Step 2: Create the secrets**

```bash
databricks secrets put --scope <name-of-scope> --key postgres-password --string-value "<your-postgres-password>"
databricks secrets put --scope <name-of-scope> --key postgres-user --string-value "<your-postgres-user>"
databricks secrets put --scope <name-of-scope> --key postgres-host --string-value "<your-postgres-host>"
databricks secrets put --scope <name-of-scope> --key n8n-encryption-key --string-value "<random-32-byte-base64-encoded-string>"
```

The **n8n-encryption-key** is used to encrypt credentials stored in the Postgres DB after they are created in the n8n app via the GUI. One method for generating this secure key on Linux is:

```bash
openssl rand -base64 32
```

### 3. Deploy Databricks App through Asset Bundles

Once the n8n-app project has been cloned, modify the host in the `databricks.yml` file, and set the scope name.

Deploy the app with these commands:

- `databricks bundle validate`
- `databricks bundle deploy`
- `databricks bundle run n8n_databricks_app`

### 4. Post deployment configuration

Some variables should be set after the initial deployment. Retrieve the deployment URL from the app, and add the following values to the `app.yml` file.

```yaml
- name: N8N_WEBHOOK_URL
  value: "<https://deploymentURL>"
- name: N8N_HOST
  value: "<deploymentURL>"
```

Redeploy the app with the commands on step 3.

## Troubleshooting

- **Database Errors:** Confirm Lakebase connection details and table schema match the workflow configuration.
- **Databricks API Errors:** Check that the access token is valid and has necessary permissions.

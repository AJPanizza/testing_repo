## Testing in `good_practices`

This project is set up to run the same pytest suite in two modes:

- **Databricks mode**: tests run against a remote Spark runtime via **Databricks Connect**.
- **Local mode**: tests run against a **local PySpark** session (JVM on your machine).

The mode is controlled by a pytest flag (`--compute`) and is used to:

- Create the right kind of `SparkSession` for tests.
- Skip tests that only make sense in one environment.

### What to run

Use `uv` to run tests in the project environment:

```bash
uv run pytest --compute=databricks
uv run pytest --compute=local
```

You can also control the default with an env var:

```bash
export TEST_COMPUTE=databricks   # or local
uv run pytest
```

### Where it’s configured

All test wiring lives in `good_practices/tests/conftest.py`:

- Adds the `--compute` option.
- Provides the `spark` fixture (the Spark session used by tests).
- Provides `load_fixture` for small JSON/CSV fixtures.
- Registers markers and auto-skips by compute mode.
- Performs Databricks Connect session initialization (Databricks mode only).

### Compute selection (`--compute`)

`--compute` has two values:

- `databricks`: create a remote session using Databricks Connect.
- `local`: create a local session using `SparkSession.builder.master("local[*]")`.

Implementation details:

- The `--compute` flag is defined via `pytest_addoption` and defaults to `TEST_COMPUTE` (or `databricks`).
- The `spark` fixture reads the flag and returns either a remote or local session.

### Skipping tests by mode

Two markers are defined and enforced automatically during collection:

- `@pytest.mark.databricks_only`: skipped unless `--compute=databricks`.
- `@pytest.mark.local_only`: skipped unless `--compute=local`.

This is useful when a test depends on:

- Databricks sample tables / Unity Catalog (Databricks only), or
- Local-only mechanics like creating DataFrames from in-memory Python objects (local only).

### Databricks mode (Databricks Connect)

When running with `--compute=databricks`:

1. `pytest_configure` checks whether a compute is already configured.
2. If none is configured, it sets `DATABRICKS_SERVERLESS_COMPUTE_ID=auto` so Databricks Connect can run on serverless.
3. It eagerly initializes `DatabricksSession` (and calls `validateSession()` when supported) to fail fast on auth/version mismatches.

How to explicitly select compute:

- Set `cluster_id` or `serverless_compute_id` in your Databricks configuration (`~/.databrickscfg`) or via env vars.
- If you set `SPARK_REMOTE`, the fallback will not trigger.

### Local mode (local PySpark)

When running with `--compute=local`:

1. `conftest.py` creates a local Spark session (master `local`) and caches it for reuse across tests.
2. It shims `databricks.sdk.runtime.spark` to point at that local session.
3. It enables Delta Lake support (so you can write/read Delta tables during tests).

That shim is important because production code in this repo often does:

```python
from databricks.sdk.runtime import spark
```

So local tests can exercise the same code paths without requiring an actual Databricks runtime.

#### Delta table manipulation (local)

Local Spark is configured with Delta Lake by enabling:

- `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
- `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`

It also sets a temporary warehouse directory and Derby home so table operations can work without polluting your repo or requiring a preconfigured local metastore.

Example pattern for tests (local mode):

```python
path = "/tmp/delta_test_table"
df.write.format("delta").mode("overwrite").save(path)
read_back = spark.read.format("delta").load(path)
assert read_back.count() == df.count()
```

### Fixture loading (`load_fixture`)

`load_fixture` is a test fixture that returns a helper function:

- Reads `good_practices/fixtures/<file>.json` or `.csv`
- Converts the parsed rows into a Spark DataFrame via `spark.createDataFrame(...)`

Notes:

- This is currently most reliable in **local mode**.
- In **Databricks Connect** mode, creating DataFrames from local Python objects can fail depending on the remote Spark Connect server’s restrictions; that’s why tests that rely on `load_fixture` are typically marked `@pytest.mark.local_only`.

### Test dependencies

Dependencies are declared in `good_practices/pyproject.toml`:

- Runtime: includes `pyspark`.
- Dev/test: includes `pytest`, `databricks-connect` (Databricks mode), `zstandard` (required by Spark Connect), and `delta-spark` (local Delta support).

### Adding new tests

Recommended pattern:

- If your test needs a Spark session, accept the `spark` fixture.
- If your test needs a small dataset, add a JSON/CSV under `good_practices/fixtures/` and use `load_fixture`.
- Mark tests:
  - `databricks_only` if they depend on Databricks-only resources (e.g. `samples.nyctaxi.trips`),
  - `local_only` if they depend on local-only mechanics (like in-memory DataFrame creation).

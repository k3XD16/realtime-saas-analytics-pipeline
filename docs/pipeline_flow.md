# Pipeline Flow

This document details the automated orchestration and data processing logic within the Real-Time SaaS Analytics Pipeline.

## 1. Airflow DAG — `realtime_saas_pipeline`

The pipeline is orchestrated by a single Airflow DAG that manages the dependencies between health checks, streaming verification, and batch aggregations.

*   **Schedule**: Every 15 minutes (`*/15 * * * *`)
*   **Executor**: `CeleryExecutor`

### Task Table

| Task ID | Type | Description | Depends On | Timeout |
| :--- | :--- | :--- | :--- | :--- |
| `t1: health_check_kafka` | `PythonOperator` | Pings the Confluent Schema Registry `/subjects` endpoint to ensure the messaging layer is healthy. | None | - |
| `t2: check_bronze_streaming_health` | `PythonOperator` | Calls the Databricks Jobs API to verify that the Bronze streaming job is actively running. | `t1` | - |
| `t3: check_silver_streaming_health` | `PythonOperator` | Calls the Databricks Jobs API to verify that the Silver streaming job is actively running. | `t2` | - |
| `t4: trigger_gold_job` | `SaaSDatabricksRunNowOperator` | Triggers the Gold batch aggregation job in Databricks using the `Run Now` API. | `t3` | - |
| `t5: wait_gold_complete` | `PythonOperator` | A custom poller that monitors the Gold job run ID, waiting for it to reach a terminal success state. | `t4` | 30 mins |
| `t6: load_gold_to_snowflake` | `SQLExecuteQueryOperator` | Executes a multi-statement `MERGE INTO` SQL script in Snowflake to load Parquet data from S3. | `t5` | - |
| `t7: pipeline_success_log` | `PythonOperator` | Logs the final success of the pipeline run. | `t6` | - |

**Lineage**: `t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7`

---

## 2. Databricks Data Processing

The pipeline utilizes a "Medallion Architecture" with a mix of streaming and batch jobs.

### Bronze Job (`01_bronze_ingestion`)
*   **Type**: Spark Structured Streaming
*   **Source**: Confluent Kafka
*   **Sink**: S3 Delta Lake
*   **Trigger**: 30-second micro-batches.
*   **Logic**: Captures raw binary payloads and persists them with Kafka metadata to ensure no data loss.

### Silver Job (`02_silver_transform`)
*   **Type**: Spark Structured Streaming
*   **Source**: Bronze Delta Table
*   **Sink**: Silver Delta Tables (`events`, `sessions`, `users`)
*   **Trigger**: 30-second micro-batches.
*   **Logic**:
    *   **Events**: Avro deserialization and schema enforcement.
    *   **Sessions**: State-aware aggregation to stitch events into sessions.
    *   **Users**: SCD Type 2 merge to maintain user history.

### Gold Job (`03_gold_aggregation`)
*   **Type**: Spark Batch
*   **Source**: Silver Delta Tables
*   **Sink**: Gold Delta Tables + Parquet Export
*   **Logic**: Performs complex business logic and time-series aggregations. After updating the Gold Delta tables, it exports the results as plain Parquet files to `s3://realtime-saas-analytics-pipeline/gold_export/` for Snowflake ingestion.

---

## 3. Snowflake Load — MERGE INTO Strategy

The `load_gold_to_snowflake` task uses an idempotent loading strategy to move data from the S3 Gold Export path into Snowflake production tables.

### The Strategy
1.  **External Stages**: Snowflake points to the S3 Parquet paths via external stages (e.g., `@stg_daily_active_users`).
2.  **Temporary Tables**: Data is first selected from the stage into a temp table to handle type casting and JSON parsing (for `VARIANT` columns).
3.  **MERGE INTO**: Snowflake runs a `MERGE` command:
    *   **On Match**: Updates existing records if the metrics have changed (e.g., if a late event arrived for a previous day).
    *   **On No Match**: Inserts new records.

### Why MERGE?
This approach makes the pipeline **idempotent**. If a job fails and is re-run, it will not create duplicate records. It also allows for "self-healing" where partial-day metrics are updated as more data flows through the pipeline.

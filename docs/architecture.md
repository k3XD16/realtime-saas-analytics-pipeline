# Pipeline Architecture

This document provides a detailed overview of the end-to-end data flow and technical architecture of the Real-Time SaaS Analytics Pipeline.

![Architecture](../assets/architecture-diagram%20dark.png)

## End-to-End Data Flow

### 1. Event Ingestion
The pipeline begins with a **FastAPI-based Event Generator** that simulates real-time user activity (page views, signups, logins, feature usage, upgrades, and churn). These events are serialized using **Avro** and published to **Confluent Kafka Cloud** in the `saas.events.raw` topic. The **Schema Registry** ensures data consistency and contract enforcement.

### 2. Bronze Layer
A **Databricks Spark Structured Streaming** job (`01_bronze_ingestion`) consumes raw binary data from Kafka. It adds ingestion metadata (`ingested_at`, `kafka_offset`, `kafka_partition`, `kafka_topic`) and writes the raw records to **AWS S3 Delta Lake** at `s3://realtime-saas-analytics-pipeline/bronze/events/`. The data is partitioned by `processing_date` and utilizes checkpointing for fault tolerance.

### 3. Silver Layer
The **Silver Layer** streaming job (`02_silver_transform`) processes the Bronze data into three specialized Delta tables:
*   **Events**: Deserializes Avro payloads, flattens the JSON `properties` into top-level columns, and deduplicates records by `event_id`.
*   **Sessions**: Uses `foreachBatch` to perform session stitching. It calculates session duration, page counts, and bounce flags by grouping events by `session_id`.
*   **Users**: Implements **SCD Type 2** (Slowly Changing Dimensions) logic to track user plan changes (e.g., Free → Pro → Enterprise) over time.

### 4. Gold Layer
A batch aggregation job (`03_gold_aggregation`) runs periodically to produce business-ready KPIs. It reads from the Silver tables and generates 5 specialized Gold tables:
*   `daily_active_users`
*   `funnel_metrics`
*   `feature_adoption`
*   `mrr_metrics`
*   `session_quality`

These tables are stored in **S3 Delta Lake** and also exported as plain **Parquet** files to `s3://realtime-saas-analytics-pipeline/gold_export/` for consumption by Snowflake.

### 5. Snowflake Load
The final stage of the pipeline involves loading the Gold datasets into **Snowflake**. An Airflow task (`load_gold_to_snowflake`) executes `MERGE INTO` SQL commands. These commands read the Parquet files from S3 (via Snowflake External Stages) and idempotently update the production Snowflake tables. This ensures that the warehouse always reflects the latest calculated metrics without duplicates.

### 6. Orchestration
The entire workflow is orchestrated by **Apache Airflow** running in Docker with the **CeleryExecutor**. The `realtime_saas_pipeline` DAG runs every 15 minutes and manages the following sequence:
`health_check_kafka` → `trigger_bronze` → `wait_bronze` → `trigger_silver` → `wait_silver` → `trigger_gold` → `wait_gold` → `load_into_snowflake` → `pipeline_success_log`.

### 7. Visualization
Business users consume the data via **Metabase** dashboards connected directly to the Snowflake Gold schema. These dashboards provide real-time visibility into DAU, MRR trends, feature adoption rates, and user conversion funnels.

## Tech Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Ingestion** | FastAPI, Python | Event Generation & API Endpoint |
| **Messaging** | Confluent Kafka | Real-time Message Broker |
| **Schema** | Confluent Schema Registry | Avro Schema Management |
| **Processing** | Databricks (Spark) | Streaming & Batch ETL |
| **Storage** | AWS S3, Delta Lake | Data Lakehouse Storage |
| **Warehouse** | Snowflake | Analytical Data Warehousing |
| **Orchestration** | Apache Airflow | Workflow Automation |
| **Visualization** | Metabase | BI Dashboards |
| **Infrastructure** | Docker, AWS IAM | Containerization & Security |

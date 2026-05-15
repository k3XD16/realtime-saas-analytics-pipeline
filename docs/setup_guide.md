# Setup Guide

This guide provides step-by-step instructions to set up and run the Real-Time SaaS Analytics Pipeline.

## 1. Prerequisites

Before you begin, ensure you have the following:
*   **Docker Desktop**: Running on your local machine.
*   **Python 3.11+**: Installed locally.
*   **Databricks Workspace**: AWS region `ap-south-1` recommended.
*   **Confluent Kafka Account**: Cloud-based Kafka broker and Schema Registry.
*   **Snowflake Account**: With a database named `SAAS_ANALYTICS`.
*   **AWS S3 Bucket**: Named `realtime-saas-analytics-pipeline`.

## 2. Repository Structure

```text
realtime-saas-analytics-pipeline/
├── airflow/            # Airflow DAGs, plugins, and Docker config
├── assets/             # Architecture diagrams and images
├── databricks/         # PySpark notebooks (Bronze, Silver, Gold)
├── docs/               # Documentation (you are here)
├── producer/           # FastAPI event generator and Kafka producer
└── snowflake/          # SQL scripts for DDL and Snowpipe
```

## 3. Environment Variables

Create a `.env` file in the root directory and another in the `airflow/` directory. Use the following keys:

| Key | Description |
| :--- | :--- |
| `FERNET_KEY` | Airflow encryption key. |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secret key for Airflow API. |
| `DATABRICKS_BRONZE_JOB_ID` | Job ID for the Bronze streaming job. |
| `DATABRICKS_SILVER_JOB_ID` | Job ID for the Silver streaming job. |
| `DATABRICKS_GOLD_JOB_ID` | Job ID for the Gold batch aggregation job. |
| `KAFKA_BOOTSTRAP_SERVERS` | Confluent Kafka bootstrap server URL. |
| `KAFKA_API_KEY` | Confluent Kafka API Key. |
| `KAFKA_API_SECRET` | Confluent Kafka API Secret. |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry URL. |
| `SCHEMA_REGISTRY_API_KEY` | Schema Registry API Key. |
| `SCHEMA_REGISTRY_API_SECRET` | Schema Registry API Secret. |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier. |
| `SNOWFLAKE_USER` | Snowflake username. |
| `SNOWFLAKE_PASSWORD` | Snowflake password. |
| `SNOWFLAKE_DATABASE` | `SAAS_ANALYTICS` |
| `SNOWFLAKE_SCHEMA` | `GOLD` |
| `SNOWFLAKE_WAREHOUSE` | `COMPUTE_WH` |

## 4. Databricks Secret Scope

The Databricks notebooks expect secrets to be stored in a scope named `saas-analytics`. Run the following commands using the Databricks CLI:

```bash
# Create the scope
databricks secrets create-scope --scope saas-analytics

# Add AWS credentials
databricks secrets put --scope saas-analytics --key AWS_ACCESS_KEY_ID
databricks secrets put --scope saas-analytics --key AWS_SECRET_ACCESS_KEY

# Add Kafka credentials
databricks secrets put --scope saas-analytics --key KAFKA_BOOTSTRAP_SERVERS
databricks secrets put --scope saas-analytics --key KAFKA_API_KEY
databricks secrets put --scope saas-analytics --key KAFKA_API_SECRET
```

## 5. Start Airflow

Navigate to the `airflow/` directory and run:

```bash
docker-compose build
docker-compose up -d
```

Open your browser and go to `http://localhost:8080`. The default credentials are `airflow` / `airflow`.

## 6. Configure Airflow Connections

In the Airflow UI, go to **Admin → Connections** and add:

1.  **Connection ID**: `databricks_default`
    *   **Type**: Databricks
    *   **Host**: Your Databricks workspace URL (e.g., `https://adb-xxx.x.azuredatabricks.net`)
    *   **Password**: Your Personal Access Token (PAT).

2.  **Connection ID**: `snowflake_default`
    *   **Type**: Snowflake
    *   **Account**: Your account identifier.
    *   **User/Password**: Your Snowflake credentials.
    *   **Database**: `SAAS_ANALYTICS`
    *   **Warehouse**: `COMPUTE_WH`
    *   **Schema**: `GOLD`

## 7. Upload Databricks Notebooks

1.  Upload the three notebooks in the `databricks/` folder to your Databricks workspace.
2.  Create three **Workflows (Jobs)** in Databricks, each pointing to one of the notebooks.
3.  Configure the Bronze and Silver jobs as **Continuous** (or manually started streaming).
4.  Configure the Gold job as a **Run Now** job (it will be triggered by Airflow).
5.  Note the **Job IDs** and add them to your `.env` file.

## 8. Trigger the Pipeline

1.  In the Airflow UI, unpause the `realtime_saas_pipeline` DAG.
2.  The DAG will run automatically every 15 minutes.
3.  You can also trigger it manually by clicking the "Play" button.

## 9. Verify Data

*   **S3**: Check the S3 bucket for `bronze/`, `silver/`, and `gold/` folders.
*   **Snowflake**: Run `SELECT * FROM GOLD.DAILY_ACTIVE_USERS` to verify data load.
*   **Metabase**: Connect Metabase to your Snowflake warehouse and build your dashboards.

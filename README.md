<div align="center">

# Real-Time SaaS Analytics Pipeline

*End-to-end event-driven data engineering pipeline — Kafka → Databricks → S3 Delta Lake → Snowflake → Metabase*

[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Processing-Databricks-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Confluent Kafka](https://img.shields.io/badge/Messaging-Confluent%20Kafka-000000?logo=confluent&logoColor=white)](https://www.confluent.io/)
[![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![AWS S3](https://img.shields.io/badge/Delta%20lake-AWS%20S3-569A31?logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)
[![Metabase](https://img.shields.io/badge/Visualization-Metabase-2D9CDB?logo=metabase&logoColor=white)](https://www.metabase.com/)

</div>

---

## Dashboard Preview

Five live Metabase dashboards tracking DAU, MRR, Funnel conversion, Feature Adoption, and Session Quality — all powered by real-time data flowing through the pipeline.

<img width="1280" height="720" alt="dashboard-preview" src="https://github.com/user-attachments/assets/c69a49eb-1ea0-48c1-88d5-cbe8bb7607a1" />

---

## Why This Project?

SaaS companies live and die by real-time visibility. Product teams need to know today's DAU, not yesterday's — they need to see a funnel drop-off the moment it happens, catch a churn spike before it compounds, and understand which features drive upgrades vs cancellations. A nightly batch pipeline running at 2AM cannot answer any of those questions. This project was built to solve that gap.

The goal was not just to move data from A to B, but to design a system that reflects real production trade-offs: exactly-once streaming semantics, schema evolution handled at the contract layer, idempotent warehouse loads safe to re-run after failures, and compute costs kept low by separating long-running streaming clusters from short-lived batch jobs. Every architectural decision here has a reason — documented below.

---

## Architecture

<img width="2253" height="2154" alt="architecture_diagram_dark" src="https://github.com/user-attachments/assets/4d1ba8e1-b57b-4998-a1f1-795f30ed0aae" />


> *Full layer-by-layer design breakdown, data flow, and design decisions → [docs/architecture.md](docs/architecture.md)*

---

## Tech Stack

| Layer | Technology | Why This Tool |
|---|---|---|
| Event Ingestion | FastAPI + Python | Lightweight HTTP producer; simulates 6 real SaaS event types (signup, login, page_view, feature_used, upgrade, churn) with Avro serialisation and configurable event-type weights |
| Message Broker | Confluent Kafka + Schema Registry | Industry-standard distributed event streaming; Avro + Schema Registry enforces a strict data contract between producer and consumer — breaking schema changes are rejected before they reach Spark |
| Stream Processing | Databricks (Apache Spark) | Managed Spark with native Delta Lake support; Structured Streaming delivers exactly-once semantics, handles schema evolution via `autoMerge`, and runs three independent streaming jobs in parallel |
| Storage — Bronze | AWS S3 + Delta Lake | Raw Avro events land here with full Kafka metadata (offset, partition, topic); ACID transactions and Delta checkpointing prevent data loss on job restart |
| Storage — Silver | AWS S3 + Delta Lake | Three cleaned tables: events (deduplicated by `event_id`), sessions (foreachBatch MERGE for real-time session stitching), users (SCD Type 2 — full plan-change history) |
| Storage — Gold | AWS S3 + Delta Lake + Parquet | Five business KPI tables written as both Delta (ad-hoc queryable) and Parquet under `/gold_export/` (Snowflake-ready export format); single-pass groupBy from one cached Silver read — minimises Spark shuffle |
| Data Warehouse | Snowflake | Columnar cloud DWH with a Star Schema (3 dimension tables + 3 fact tables); Snowpipe auto-ingests from S3 Parquet on file arrival — zero-touch loading |
| Orchestration | Apache Airflow (Docker, CeleryExecutor) | 7-task DAG on `0 * * * *` hourly schedule: Kafka health check → Bronze/Silver stream health → Gold batch trigger+poll → Snowflake MERGE INTO → success log. Retries=2, delay=5min |
| Visualisation | Metabase (Docker) | Open-source BI connected directly to Snowflake; self-hosted on Docker — zero licensing cost; five dashboards covering every core SaaS growth metric |

---

## Pipeline at a Glance

Data moves from raw event to business-ready KPI on an hourly Airflow schedule (`0 * * * *`).

```
FastAPI Event Producer  (producer/event_generator.py + live_stream.py)
        │
        │  Avro-serialised events  [6 event types, configurable weights]
        ▼
Confluent Kafka Cloud
        Topic: saas.events.raw
        Schema Registry: Avro contract enforced on produce
        DLQ Topic: saas.events.dlq  ← NULL event_id / event_type / timestamp rejected here
        │
        │  Spark Structured Streaming — 30s micro-batch
        ▼
Databricks Bronze Job  ──►  S3 Delta Lake  /bronze/events/
        │  Adds: ingested_at, kafka_offset, kafka_partition, processing_date
        │  Partitioned by: processing_date
        │  Checkpoint: s3://bucket/checkpoints/bronze/
        │
        │  Airflow t2 triggers → t3 polls every 30s (timeout 10min)
        ▼
Databricks Silver Job  ──►  S3 Delta Lake  /silver/
        ├── events/       Avro deserialise → flatten properties JSON → dedup by event_id
        ├── sessions/     foreachBatch MERGE → session stitching, duration, bounce flag
        └── users/        SCD Type 2 → plan-change history (valid_from / valid_to / is_current)
        │
        │  Airflow t4 triggers → t5 polls every 30s (timeout 30 min)
        ▼
Databricks Gold Job  ──►  S3 Delta Lake  /gold/  +  S3 Parquet  /gold_export/
        ├── daily_active_users    (DAU / WAU / MAU, new vs returning)
        ├── funnel_metrics        (page_view → signup → activation → upgrade → churn rates)
        ├── feature_adoption      (per-feature: unique users, total uses, avg duration, plan breakdown)
        ├── mrr_metrics           (new / churned / expansion / net / total MRR, pro + enterprise counts)
        └── session_quality       (avg duration, bounce rate, pages/session, device split, top countries)
        │
        │  Airflow t6 triggers → t7 polls every 30s (timeout 20min)
        ▼
Snowpipe Auto-Ingest  ──►  Snowflake  SAAS_ANALYTICS database
        5 Snowpipes (one per Gold table) triggered on S3 file arrival
        Star Schema: DIM_USERS (SCD2) + DIM_DATE + DIM_FEATURES + DIM_CAMPAIGNS
                     FACT_EVENTS + FACT_SESSIONS + FACT_MRR
        Airflow t6: load_gold_to_snowflake  (SQLExecuteQueryOperator → snowflake_default)
        Reads Parquet from 5 external S3 stages → 5 temp tables
        MERGE INTO all 5 Gold tables on date key — idempotent, no duplicates on retry
        │
        ▼
Snowflake  SAAS_ANALYTICS database  →  Metabase Dashboards
        5 live dashboards  ←  see Dashboard Preview above
```

> *Full 7-task Airflow DAG breakdown with XCom flow and operator details → [docs/pipeline_flow.md](docs/pipeline_flow.md)*

---

## Key Engineering Features

| Feature | Detail |
|---|---|
| Exactly-once streaming | Spark Structured Streaming + Delta Lake checkpointing on S3; on job restart, the stream resumes from the last committed offset — no duplicates in Bronze |
| Event deduplication | Silver events layer performs `dropDuplicates(["event_id"])` before write — prevents double-counting in all downstream Gold aggregations |
| Session stitching | `foreachBatch` MERGE accumulates `session_duration_secs` and `page_count` across micro-batches; bounce flag set when `page_count = 1` |
| SCD Type 2 users | Full plan-change history tracked with `valid_from`, `valid_to`, `is_current`; enables exact point-in-time analysis of which plan a user was on during any event |
| Schema enforcement | Avro schema defined in `producer/schemas/saas_event.avsc`; Schema Registry rejects any incompatible produce attempt; Delta `autoMerge.enabled` handles additive column evolution |
| DLQ routing | Bronze layer rejects records with NULL `event_id`, `event_type`, or `timestamp` to `saas.events.dlq` before writing to Delta — bad data never reaches Silver |
| Kafka health gate | Airflow `t1` pings the Confluent Schema Registry REST API before triggering any Databricks job — the entire DAG stops cleanly if the broker is unreachable |
| Cost-optimised Spark | `spark.sql.shuffle.partitions=8` for small-data workloads; all 5 Gold tables computed in a single Databricks Gold job from one cached Silver read — minimises DBU spend |
| Idempotent Snowflake load | `SQLExecuteQueryOperator` runs `MERGE INTO` on `date` key for all 5 Gold tables; re-running Airflow `t6` on failure never duplicates rows — safe exactly-once warehouse writes |
| Observable pipeline | `loguru` structured logging in all producer, Databricks, and Airflow code; Airflow UI provides full task-level run history, XCom inspection, and retry state |

---

## Estimated Cloud Cost

Approximate monthly cost for running this pipeline in a development/demo environment on AWS `ap-south-1`.

| Service | Configuration | Approx. Monthly Cost |
|---|---|---|
| Databricks | 1× `i3.xlarge` cluster (Bronze + Silver — always-on streaming) + 1× `m5.large` (Gold — hourly batch triggered by Airflow) | ~$40–60 |
| AWS S3 | ~5 GB Delta Lake + Parquet + checkpoint storage + GET/PUT requests | ~$2–5 |
| Confluent Kafka | Basic cluster, 1 partition, low throughput | ~$0 (free tier) |
| Snowflake | X-Small warehouse, ~10 hrs/month compute | ~$0 (free trial credit) |
| Apache Airflow | Local Docker on dev machine | $0 |
| Metabase | Self-hosted on Docker | ~$0 |
| **Total** | | **~$50–60 / month** |

> Bronze and Silver use long-running Structured Streaming clusters — they stay alive to process micro-batches continuously. Gold uses a short-lived batch cluster triggered by Airflow hourly, then terminates — this is the primary lever for keeping compute cost low.

---

## Orchestration

The entire pipeline lifecycle is managed by a single Airflow DAG (`realtime_saas_pipeline`) running every hour (`0 * * * *`) with 2 retries and a 5-minute retry delay.

<img width="1911" height="910" alt="airflow-dag2" src="https://github.com/user-attachments/assets/95cac822-7a38-44a0-a773-e8af46f97abd" />

**7-task DAG — `t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7`**

| Task | ID | Type | Action |
|---|---|---|---|
| `t1` | `health_check_kafka` | PythonOperator | Ping Confluent Schema Registry REST API — stops DAG on failure |
| `t2` | `check_bronze_streaming_health` | PythonOperator | Call Databricks API (`2.1/jobs/runs/list`) to confirm Bronze Structured Streaming job has an active run — raises if not alive |
| `t3` | `check_silver_streaming_health` | PythonOperator | Same health check for Silver Structured Streaming job — raises if not alive |
| `t4` | `trigger_gold_job` | SaaSDatabricksRunNowOperator | Fire Gold batch Databricks Job (non-blocking); pushes `run_id` to XCom |
| `t5` | `wait_gold_complete` | PythonOperator (XCom poller) | Pull `run_id` from t4 XCom; poll `DatabricksHook.get_run_state()` every 30s — timeout 30min |
| `t6` | `load_gold_to_snowflake` | SQLExecuteQueryOperator | Run MERGE SQL against `snowflake_default`: reads 5 Gold Parquet stages → temp tables → `MERGE INTO` all 5 Gold tables — idempotent on table-specific business keys |
| `t7` | `pipeline_success_log` | PythonOperator | Log final success message — pipeline run complete |

> *Full operator implementation, XCom flow, and connection setup → [docs/pipeline_flow.md](docs/pipeline_flow.md)*

---

## Dashboards

Metabase connected to Snowflake — five live dashboards covering every core SaaS growth metric.

| Dashboard | Metrics |
|---|---|
| Daily Active Users | DAU / WAU / MAU trend, new vs returning user split, daily growth rate |
| MRR Metrics | New MRR, churned MRR, expansion MRR, net MRR movement over time |
| Funnel Analytics | Visitor → signup → activation → upgrade → churn conversion rates by day |
| Feature Adoption | Per-feature unique users, total uses, avg session duration, plan-tier breakdown |
| Session Quality | Avg session duration, bounce rate, pages/session, device split, top countries |

*Full dashboard PDF export → [metabase/dashboards/](metabase/dashboards/)*

---

## Screenshots

End-to-end proof — from Kafka events flowing in, through Databricks processing, to Snowflake tables loaded and Metabase dashboards live.

| Airflow DAG (7 tasks green) | Databricks Jobs |
|:---:|:---:|
<img width="1915" height="916" alt="airflow-dag3" src="https://github.com/user-attachments/assets/cf8ddb04-9e4f-4fa8-b98a-5d6e34b9441c" /> | <img width="1194" height="735" alt="databricks_jobs" src="https://github.com/user-attachments/assets/fcbf7c2f-3ddd-423a-b436-74de40316dd0" /> |

| S3 Delta Lake Bucket | Snowflake Schema |
|:---:|:---:|
| <img width="1344" height="674" alt="s3-bucket" src="https://github.com/user-attachments/assets/a3dedce6-6fe0-4317-b2fb-f1af8fbf0b3d" /> | <img width="1919" height="1079" alt="snowflake-database_and_schemas" src="https://github.com/user-attachments/assets/87740751-8cd4-4a9c-ac12-394fd91c2385" /> |


---

## Repository Structure

```
realtime-saas-analytics-pipeline/
├── airflow/
│   ├── config/airflow.cfg
│   ├── dags/
│   │   └── pipeline_dag.py                      # 7-task orchestration DAG (0 * * * * — hourly)
│   ├── plugins/
│   │   └── saas_databricks_operator.py          # Custom DatabricksRunNow operator
│   ├── docker-compose.yaml                      # Airflow + Postgres + Redis + CeleryExecutor
│   ├── Dockerfile                               # Custom Airflow image
│   ├── requirements.txt                         # Airflow + provider dependencies
│   └── .env.example
├── databricks/
│   ├── 01_bronze_ingestion.ipynb                # Kafka → S3 Delta (30s micro-batch streaming)
│   ├── 02_silver_transform.ipynb                # Bronze → 3 Silver tables (events, sessions, users)
│   └── 03_gold_aggregation.ipynb                # Silver → 5 Gold KPI tables + Parquet export
├── docs/
│   ├── architecture.md                          # System design, layer breakdown, trade-offs
│   ├── data_dictionary.md                       # All Bronze / Silver / Gold table schemas
│   ├── pipeline_flow.md                         # DAG tasks, Databricks job specs, Snowflake MERGE load strategy
│   └── setup_guide.md                           # Prerequisites, env config, run order
├── infra/
│   └── iam/
│       ├── databricks_s3_policy.json            # IAM policy: Databricks read/write S3
│       └── snowflake_s3_policy.json             # IAM policy: Snowflake read S3 for Snowpipe
├── metabase/
│   ├── docker-compose.yaml                      # Metabase + Postgres metadata DB
│   └── dashboards/
│       └── Metabase - SaaS Analytics Overview.pdf
├── producer/
│   ├── schemas/
│   │   └── saas_event.avsc                      # Avro schema — source of truth for event contract
│   ├── config.py                                # Load all env vars via python-dotenv
│   ├── event_generator.py                       # Generate 6 event types with realistic weights
│   ├── historical_seed.py                       # Bulk-fire 100k backdated events (90-day range)
│   ├── kafka_producer.py                        # Avro serialise → Kafka publish → DLQ routing
│   ├── live_stream.py                           # Continuous live stream at configurable rate
│   ├── main.py                                  # FastAPI app entry point
│   └── requirements.txt
├── snowflake/
│   ├── setup.sql                                # Full DDL: dimensions + facts + views + Snowpipes
│   └── adhoc_queries.sql                        # Analytical queries for verification
├── .env.example
├── .gitignore
├── GEMINI.md                                    # AI coding agent instructions
├── LICENSE
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop (running)
- AWS account with S3 bucket and IAM roles configured
- Databricks workspace (Community Edition works for demo)
- Confluent Cloud account (free tier)
- Snowflake account (free trial or existing)

### Run the Pipeline

**1. Clone the repo**
```bash
git clone https://github.com/k3XD16/realtime-saas-analytics-pipeline.git
cd realtime-saas-analytics-pipeline
```

**2. Set up environment variables**
```bash
cp .env.example .env
# Fill in: Kafka, Schema Registry, AWS, Databricks, Snowflake credentials
```

**3. Start Airflow**
```bash
cd airflow
cp .env.example .env
docker-compose build && docker-compose up -d
```
Open Airflow UI at `http://localhost:8080`

**4. Start Metabase**
```bash
cd metabase && docker-compose up -d
```
Open Metabase at `http://localhost:3000`

**5. Upload Databricks notebooks**

Upload all three notebooks from `databricks/` to your Databricks workspace and register each as a Job. Copy the Job IDs into your `.env`.

**6. Configure Airflow connection**

Airflow UI → Admin → Connections → Add `databricks_default` with your host and token.

**7. Seed historical data** *(run once — populates 90 days of dashboard history)*
```bash
cd producer && pip install -r requirements.txt
python historical_seed.py --events 100000 --days 90
```

**8. Unpause the DAG and start live stream**
```bash
# In Airflow UI: unpause realtime_saas_pipeline
# Then start the live stream for demo activity:
python live_stream.py --rate 900
```

> *Full setup with Snowflake DDL, IAM policy config, Databricks secret scope, and Snowpipe registration → [docs/setup_guide.md](docs/setup_guide.md)*

---

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | End-to-end system design, layer breakdown, tech stack rationale, and trade-offs |
| [Data Dictionary](docs/data_dictionary.md) | Full schema for all Bronze, Silver, and Gold Delta tables with column descriptions |
| [Pipeline Flow](docs/pipeline_flow.md) | 7-task Airflow DAG breakdown, Databricks job specs, Snowflake MERGE load strategy, XCom flow |
| [Setup Guide](docs/setup_guide.md) | Prerequisites, environment config, Databricks secret scope, full run-order checklist |
| [Snowflake DDL](snowflake/setup.sql) | CREATE TABLE/VIEW statements for the Star Schema + all 5 Snowpipe definitions |

---

## Data Strategy

The pipeline ships with two data population scripts designed for portfolio demonstration.

| Script | Command | Purpose |
|---|---|---|
| `producer/historical_seed.py` | `python historical_seed.py --events 100000 --days 90` | Bulk-fires 100,000 backdated events spanning 90 days — gives dashboards meaningful trend history on day one |
| `producer/live_stream.py` | `python live_stream.py --rate 900` | Fires 900 events/minute continuously — shows the pipeline is alive during demo sessions and interviews |

Both scripts respect the same event-type weight distribution: `page_view` 50%, `login` 20%, `feature_used` 15%, `signup` 8%, `upgrade` 5%, `churn` 2%.

---

## Author & Contact

<div align="center">

#### **Built with ❤️ by**

***[Mohamed Khasim](https://x.com/k3XD16)*** *Data Analyst → Data Engineer | Chennai, India*

![GitHub](https://img.shields.io/badge/GitHub-k3XD16-181717?style=flat-square&logo=github&logoColor=white)
![LinkedIn](https://img.shields.io/badge/LinkedIn-mohamedkhasim16-0077B5?style=flat-square&logo=linkedin&logoColor=white)
![Email](https://img.shields.io/badge/Email-mohamedkhasim.16%40gmail.com-D14836?style=flat-square&logo=gmail&logoColor=white)

</div>

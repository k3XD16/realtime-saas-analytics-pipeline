import os
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from saas_databricks_operator import SaaSDatabricksRunNowOperator
from loguru import logger


# ── Constants from environment ────────────────────────────────────────────────
DATABRICKS_BRONZE_JOB_ID = os.getenv("DATABRICKS_BRONZE_JOB_ID")
DATABRICKS_SILVER_JOB_ID = os.getenv("DATABRICKS_SILVER_JOB_ID")
DATABRICKS_GOLD_JOB_ID   = os.getenv("DATABRICKS_GOLD_JOB_ID")

SCHEMA_REGISTRY_URL        = os.getenv("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_API_KEY    = os.getenv("SCHEMA_REGISTRY_API_KEY")
SCHEMA_REGISTRY_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")


# ── Merge SQL
MERGE_SQL = """
    CREATE OR REPLACE TEMP TABLE tmp_daily_active_users AS
    SELECT
        $1:date::DATE           AS date,
        $1:dau_count::INT       AS dau_count,
        $1:new_users::INT       AS new_users,
        $1:returning_users::INT AS returning_users,
        $1:wau_count::INT       AS wau_count,
        $1:mau_count::INT       AS mau_count
    FROM @stg_daily_active_users (FILE_FORMAT => parquet_format, PATTERN => '.*\.parquet');

    CREATE OR REPLACE TEMP TABLE tmp_mrr_metrics AS
    SELECT
        $1:date::DATE              AS date,
        $1:new_mrr::DOUBLE         AS new_mrr,
        $1:churned_mrr::DOUBLE     AS churned_mrr,
        $1:expansion_mrr::DOUBLE   AS expansion_mrr,
        $1:net_mrr::DOUBLE         AS net_mrr,
        $1:total_mrr::DOUBLE       AS total_mrr,
        $1:pro_users::INT          AS pro_users,
        $1:enterprise_users::INT   AS enterprise_users,
        $1:plan::STRING            AS plan,
        $1:mrr_usd::DOUBLE         AS mrr_usd
    FROM @stg_mrr_metrics (FILE_FORMAT => parquet_format, PATTERN => '.*\.parquet');

    CREATE OR REPLACE TEMP TABLE tmp_funnel_metrics AS
    SELECT
        $1:date::DATE                         AS date,
        $1:total_page_views::INT              AS total_page_views,
        $1:unique_visitors::INT               AS unique_visitors,
        $1:signups::INT                       AS signups,
        $1:logins::INT                        AS logins,
        $1:feature_activations::INT           AS feature_activations,
        $1:upgrades::INT                      AS upgrades,
        $1:churns::INT                        AS churns,
        $1:visitor_to_signup_rate::DOUBLE     AS visitor_to_signup_rate,
        $1:signup_to_activation_rate::DOUBLE  AS signup_to_activation_rate,
        $1:activation_to_upgrade_rate::DOUBLE AS activation_to_upgrade_rate,
        $1:upgrade_to_churn_rate::DOUBLE      AS upgrade_to_churn_rate
    FROM @stg_funnel_metrics (FILE_FORMAT => parquet_format, PATTERN => '.*\.parquet');

    CREATE OR REPLACE TEMP TABLE tmp_feature_adoption AS
    SELECT
        $1:date::DATE                   AS date,
        $1:feature_name::STRING         AS feature_name,
        $1:unique_users::INT            AS unique_users,
        $1:total_uses::INT              AS total_uses,
        $1:avg_duration_seconds::DOUBLE AS avg_duration_seconds,
        $1:plan_breakdown::VARIANT      AS plan_breakdown
    FROM @stg_feature_adoption (FILE_FORMAT => parquet_format, PATTERN => '.*\.parquet');

    CREATE OR REPLACE TEMP TABLE tmp_session_quality AS
    SELECT
        $1:date::DATE                        AS date,
        $1:avg_session_duration_secs::DOUBLE AS avg_session_duration_secs,
        $1:avg_pages_per_session::DOUBLE     AS avg_pages_per_session,
        $1:bounce_rate::DOUBLE               AS bounce_rate,
        $1:mobile_pct::DOUBLE                AS mobile_pct,
        $1:desktop_pct::DOUBLE               AS desktop_pct,
        $1:tablet_pct::DOUBLE                AS tablet_pct,
        $1:top_countries::VARIANT            AS top_countries
    FROM @stg_session_quality (FILE_FORMAT => parquet_format, PATTERN => '.*\.parquet');

    MERGE INTO gold_daily_active_users t
    USING (SELECT DISTINCT * FROM tmp_daily_active_users) s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET
        dau_count = s.dau_count, new_users = s.new_users,
        returning_users = s.returning_users,
        wau_count = s.wau_count, mau_count = s.mau_count
    WHEN NOT MATCHED THEN INSERT
        (date, dau_count, new_users, returning_users, wau_count, mau_count)
    VALUES (s.date, s.dau_count, s.new_users, s.returning_users,
            s.wau_count, s.mau_count);

    MERGE INTO gold_mrr_metrics t
    USING (SELECT DISTINCT * FROM tmp_mrr_metrics) s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET
        new_mrr = s.new_mrr, churned_mrr = s.churned_mrr,
        expansion_mrr = s.expansion_mrr, net_mrr = s.net_mrr,
        total_mrr = s.total_mrr, pro_users = s.pro_users,
        enterprise_users = s.enterprise_users, mrr_usd = s.mrr_usd
    WHEN NOT MATCHED THEN INSERT
        (date, new_mrr, churned_mrr, expansion_mrr, net_mrr,
            total_mrr, pro_users, enterprise_users, plan, mrr_usd)
    VALUES (s.date, s.new_mrr, s.churned_mrr, s.expansion_mrr, s.net_mrr,
            s.total_mrr, s.pro_users, s.enterprise_users, s.plan, s.mrr_usd);

    MERGE INTO gold_funnel_metrics t
    USING (SELECT DISTINCT * FROM tmp_funnel_metrics) s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET
        total_page_views = s.total_page_views,
        unique_visitors = s.unique_visitors,
        signups = s.signups, logins = s.logins,
        feature_activations = s.feature_activations,
        upgrades = s.upgrades, churns = s.churns,
        visitor_to_signup_rate = s.visitor_to_signup_rate,
        signup_to_activation_rate = s.signup_to_activation_rate,
        activation_to_upgrade_rate = s.activation_to_upgrade_rate,
        upgrade_to_churn_rate = s.upgrade_to_churn_rate
    WHEN NOT MATCHED THEN INSERT
        (date, total_page_views, unique_visitors, signups, logins,
            feature_activations, upgrades, churns, visitor_to_signup_rate,
            signup_to_activation_rate, activation_to_upgrade_rate,
            upgrade_to_churn_rate)
    VALUES (s.date, s.total_page_views, s.unique_visitors, s.signups,
            s.logins, s.feature_activations, s.upgrades, s.churns,
            s.visitor_to_signup_rate, s.signup_to_activation_rate,
            s.activation_to_upgrade_rate, s.upgrade_to_churn_rate);

    MERGE INTO gold_feature_adoption t
    USING (SELECT DISTINCT * FROM tmp_feature_adoption) s
    ON t.date = s.date AND t.feature_name = s.feature_name
    WHEN MATCHED THEN UPDATE SET
        unique_users = s.unique_users, total_uses = s.total_uses,
        avg_duration_seconds = s.avg_duration_seconds,
        plan_breakdown = s.plan_breakdown
    WHEN NOT MATCHED THEN INSERT
        (date, feature_name, unique_users, total_uses,
            avg_duration_seconds, plan_breakdown)
    VALUES (s.date, s.feature_name, s.unique_users,
            s.total_uses, s.avg_duration_seconds, s.plan_breakdown);

    MERGE INTO gold_session_quality t
    USING (SELECT DISTINCT * FROM tmp_session_quality) s
    ON t.date = s.date
    WHEN MATCHED THEN UPDATE SET
        avg_session_duration_secs = s.avg_session_duration_secs,
        avg_pages_per_session = s.avg_pages_per_session,
        bounce_rate = s.bounce_rate, mobile_pct = s.mobile_pct,
        desktop_pct = s.desktop_pct, tablet_pct = s.tablet_pct,
        top_countries = s.top_countries
    WHEN NOT MATCHED THEN INSERT
        (date, avg_session_duration_secs, avg_pages_per_session,
            bounce_rate, mobile_pct, desktop_pct, tablet_pct, top_countries)
    VALUES (s.date, s.avg_session_duration_secs, s.avg_pages_per_session,
            s.bounce_rate, s.mobile_pct, s.desktop_pct,
            s.tablet_pct, s.top_countries);
"""


# ── Task Functions 
def health_check_kafka() -> None:
    """Task 1: Health check Confluent Schema Registry."""
    logger.info("Starting Kafka health check...")
    if not SCHEMA_REGISTRY_URL:
        raise ValueError("SCHEMA_REGISTRY_URL not set.")
    try:
        response = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects",
            auth=(SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET),
            timeout=15,
        )
        response.raise_for_status()
        logger.success("Kafka Schema Registry healthy.")
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        raise


def check_bronze_streaming_health() -> None:
    """Task 3: Verify Bronze Spark Structured Streaming job is RUNNING."""
    logger.info("Checking Bronze streaming job health...")
    if not DATABRICKS_BRONZE_JOB_ID:
        raise ValueError("DATABRICKS_BRONZE_JOB_ID not set.")

    hook = DatabricksHook(databricks_conn_id="databricks_default")
    try:
        response = hook._do_api_call(
            ("GET", f"2.1/jobs/runs/list?job_id={int(DATABRICKS_BRONZE_JOB_ID)}&active_only=true&limit=1"),
            {},
        )
        runs = response.get("runs", [])
        if not runs:
            raise Exception(
                f"Bronze streaming job (ID: {DATABRICKS_BRONZE_JOB_ID}) is NOT running! "
                "Check Databricks Workflows."
            )
        state = runs[0].get("state", {}).get("life_cycle_state", "UNKNOWN")
        logger.success(f"Bronze streaming job is ALIVE. State: {state}")
    except Exception as e:
        logger.error(f"Bronze health check failed: {e}")
        raise


def check_silver_streaming_health() -> None:
    """Task 5: Verify Silver Spark Structured Streaming job is RUNNING."""
    logger.info("Checking Silver streaming job health...")
    if not DATABRICKS_SILVER_JOB_ID:
        raise ValueError("DATABRICKS_SILVER_JOB_ID not set.")

    hook = DatabricksHook(databricks_conn_id="databricks_default")
    try:
        response = hook._do_api_call(
            ("GET", f"2.1/jobs/runs/list?job_id={int(DATABRICKS_SILVER_JOB_ID)}&active_only=true&limit=1"),
            {},
        )
        runs = response.get("runs", [])
        if not runs:
            raise Exception(
                f"Silver streaming job (ID: {DATABRICKS_SILVER_JOB_ID}) is NOT running! "
                "Check Databricks Workflows."
            )
        state = runs[0].get("state", {}).get("life_cycle_state", "UNKNOWN")
        logger.success(f"Silver streaming job is ALIVE. State: {state}")
    except Exception as e:
        logger.error(f"Silver health check failed: {e}")
        raise


def wait_for_gold_job(task_id_trigger: str, timeout_mins: int = 20, **context: Any) -> None:
    """Task 7: Poll Gold batch job until it completes."""
    ti = context["ti"]

    run_id = ti.xcom_pull(task_ids=task_id_trigger, key="return_value")
    if not run_id:
        run_id = ti.xcom_pull(task_ids=task_id_trigger, key="run_id")

    if not run_id:
        raise ValueError(f"No run_id found in XCom for task: {task_id_trigger}")

    logger.info(f"Polling Gold batch run {run_id}...")

    hook    = DatabricksHook(databricks_conn_id="databricks_default")
    end_time = time.time() + (timeout_mins * 60)

    while time.time() < end_time:
        run_state = hook.get_run_state(run_id)
        logger.info(f"Gold run {run_id} -> {run_state.life_cycle_state} / {run_state.result_state}")

        if run_state.is_terminal:
            if run_state.is_successful:
                logger.success(f"Gold batch run {run_id} completed successfully.")
                return
            else:
                raise Exception(f"Gold batch run {run_id} failed: {run_state.result_state}")

        time.sleep(30)

    raise TimeoutError(f"Timed out after {timeout_mins} minutes waiting for Gold job.")


def pipeline_success_log() -> None:
    """Task 9: Final success log."""
    logger.success("Pipeline realtime_saas_pipeline completed all tasks successfully.")


# ── DAG Definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner":           "airflow",
    "depends_on_past": False,
    "start_date":      datetime(2026, 4, 24),
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    "realtime_saas_pipeline",
    default_args=default_args,
    description=(
        "Orchestration DAG for Real-Time SaaS Analytics Pipeline. "
        "Checks Bronze/Silver streaming health, triggers Gold batch, "
        "then TRUNCATE + COPY INTO Snowflake Gold tables (idempotent, no duplicates)."
    ),
    schedule="0 * * * *",  # Every 1 hour
    catchup=False,
    tags=["saas", "realtime", "databricks", "snowflake"],
) as dag:

    # ── Task 1: Kafka health check ─────────────────────────────────────────────
    t1 = PythonOperator(
        task_id="health_check_kafka",
        python_callable=health_check_kafka,
    )

    # ── Task 2: Verify Bronze is alive ────────────────────────────────────────
    t2 = PythonOperator(
        task_id="check_bronze_streaming_health",
        python_callable=check_bronze_streaming_health,
    )

    # ── Task3: Verify Silver is alive ────────────────────────────────────────
    t3 = PythonOperator(
        task_id="check_silver_streaming_health",
        python_callable=check_silver_streaming_health,
    )

    # ── Task 4: Trigger Gold BATCH job ────────────────────────────────────────
    t4 = SaaSDatabricksRunNowOperator(
        task_id="trigger_gold_job",
        job_id=DATABRICKS_GOLD_JOB_ID,
        databricks_conn_id="databricks_default",
        wait_for_termination=False,
    )

    # ── Task 5: Wait for Gold BATCH to complete ───────────────────────────────
    t5 = PythonOperator(
        task_id="wait_gold_complete",
        python_callable=wait_for_gold_job,
        op_kwargs={"task_id_trigger": "trigger_gold_job", "timeout_mins": 30},
    )

    # ── Task 6: verify snowpipe health check ────────────────────────────────────────
    t6 = SQLExecuteQueryOperator(
        task_id="load_gold_to_snowflake",
        conn_id="snowflake_default",
        sql=MERGE_SQL,
    )

    # ── Task 7: Pipeline success log ──────────────────────────────────────────
    t7 = PythonOperator(
        task_id="pipeline_success_log",
        python_callable=pipeline_success_log,
    )

    # ── Task Lineage ──────────────────────────────────────────────────────────
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
-- ============================================================
-- SNOWFLAKE SETUP — Real-Time SaaS Analytics Pipeline
-- Run this once before starting the pipeline
-- ============================================================

-- SET ROLE AND WAREHOUSE
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

-- CREATE DATABASE AND SCHEMA
CREATE OR REPLACE DATABASE saas_analytics;
CREATE OR REPLACE SCHEMA saas_analytics.analytics;

-- SET SEARCH PATH
USE SCHEMA saas_analytics.analytics;

COMMENT ON SCHEMA saas_analytics.analytics IS 'Main analytics schema for SaaS event-driven data';


-- CREATE STORAGE INTEGRATION
-- Note: Replace <AWS_ROLE_ARN> with the actual IAM Role ARN created in Phase 3
CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://realtime-saas-analytics-pipeline/')
    STORAGE_AWS_ROLE_ARN = '<AWS_ROLE_ARN>';

-- DESCRIBE INTEGRATION TO GET STORAGE_AWS_IAM_USER_ARN AND STORAGE_AWS_EXTERNAL_ID
DESC INTEGRATION s3_int;


-- ── 1. FILE FORMAT ──────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    BINARY_AS_TEXT = FALSE;


-- ── 2. EXTERNAL STAGES (S3 → Snowflake) ─────────────────────
CREATE OR REPLACE STAGE stg_daily_active_users
    URL = 's3://realtime-saas-analytics-pipeline/gold_export/daily_active_users/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (FORMAT_NAME = parquet_format);

CREATE OR REPLACE STAGE stg_mrr_metrics
    URL = 's3://realtime-saas-analytics-pipeline/gold_export/mrr_metrics/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (FORMAT_NAME = parquet_format);

CREATE OR REPLACE STAGE stg_funnel_metrics
    URL = 's3://realtime-saas-analytics-pipeline/gold_export/funnel_metrics/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (FORMAT_NAME = parquet_format);

CREATE OR REPLACE STAGE stg_feature_adoption
    URL = 's3://realtime-saas-analytics-pipeline/gold_export/feature_adoption/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (FORMAT_NAME = parquet_format);

CREATE OR REPLACE STAGE stg_session_quality
    URL = 's3://realtime-saas-analytics-pipeline/gold_export/session_quality/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (FORMAT_NAME = parquet_format);


-- ── 3. GOLD TABLES ──────────────────────────────────────────
CREATE OR REPLACE TABLE gold_daily_active_users (
    date            DATE,
    dau_count       INT,
    new_users       INT,
    returning_users INT,
    wau_count       INT,
    mau_count       INT
);

CREATE OR REPLACE TABLE gold_mrr_metrics (
    date             DATE,
    new_mrr          DOUBLE,
    churned_mrr      DOUBLE,
    expansion_mrr    DOUBLE,
    net_mrr          DOUBLE,
    total_mrr        DOUBLE,
    pro_users        INT,
    enterprise_users INT,
    plan             STRING,
    mrr_usd          DOUBLE
);

CREATE OR REPLACE TABLE gold_funnel_metrics (
    date                       DATE,
    total_page_views           INT,
    unique_visitors            INT,
    signups                    INT,
    logins                     INT,
    feature_activations        INT,
    upgrades                   INT,
    churns                     INT,
    visitor_to_signup_rate     DOUBLE,
    signup_to_activation_rate  DOUBLE,
    activation_to_upgrade_rate DOUBLE,
    upgrade_to_churn_rate      DOUBLE
);

CREATE OR REPLACE TABLE gold_feature_adoption (
    date                 DATE,
    feature_name         STRING,
    unique_users         INT,
    total_uses           INT,
    avg_duration_seconds DOUBLE,
    plan_breakdown       VARIANT
);

CREATE OR REPLACE TABLE gold_session_quality (
    date                      DATE,
    avg_session_duration_secs DOUBLE,
    avg_pages_per_session     DOUBLE,
    bounce_rate               DOUBLE,
    mobile_pct                DOUBLE,
    desktop_pct               DOUBLE,
    tablet_pct                DOUBLE,
    top_countries             VARIANT
);
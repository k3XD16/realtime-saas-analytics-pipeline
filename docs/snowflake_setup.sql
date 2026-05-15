/*
    Snowflake Setup Script for Real-Time SaaS Analytics Pipeline
    This script initializes the database objects required for the Gold layer in Snowflake.
    It creates tables that mirror the aggregated business KPIs calculated in Databricks.
*/

USE DATABASE SAAS_ANALYTICS;
USE SCHEMA GOLD;

-- 1. Daily Active Users (DAU)
-- Tracks daily, weekly, and monthly unique user counts and growth.
CREATE TABLE IF NOT EXISTS gold_daily_active_users (
    date            DATE PRIMARY KEY,
    dau_count       NUMBER,
    new_users       NUMBER,
    returning_users NUMBER,
    wau_count       NUMBER,
    mau_count       NUMBER,
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. MRR Metrics
-- Financial performance metrics including new MRR, churn, and net growth.
CREATE TABLE IF NOT EXISTS gold_mrr_metrics (
    date             DATE PRIMARY KEY,
    new_mrr          FLOAT,
    churned_mrr      FLOAT,
    expansion_mrr    FLOAT,
    net_mrr          FLOAT,
    total_mrr        FLOAT,
    pro_users        NUMBER,
    enterprise_users NUMBER,
    plan             VARCHAR(50),
    mrr_usd          FLOAT,
    updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Funnel Metrics
-- Conversion rates across the user lifecycle from visitor to churn.
CREATE TABLE IF NOT EXISTS gold_funnel_metrics (
    date                        DATE PRIMARY KEY,
    total_page_views            NUMBER,
    unique_visitors             NUMBER,
    signups                     NUMBER,
    logins                      NUMBER,
    feature_activations         NUMBER,
    upgrades                    NUMBER,
    churns                      NUMBER,
    visitor_to_signup_rate      FLOAT,
    signup_to_activation_rate   FLOAT,
    activation_to_upgrade_rate  FLOAT,
    upgrade_to_churn_rate       FLOAT,
    updated_at                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Feature Adoption
-- Usage statistics and plan breakdown for specific product features.
CREATE TABLE IF NOT EXISTS gold_feature_adoption (
    date                    DATE,
    feature_name            VARCHAR(100),
    unique_users            NUMBER,
    total_uses              NUMBER,
    avg_duration_seconds    FLOAT,
    plan_breakdown          VARIANT,
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (date, feature_name)
);

-- 5. Session Quality
-- User engagement metrics including bounce rates and device distribution.
CREATE TABLE IF NOT EXISTS gold_session_quality (
    date                        DATE PRIMARY KEY,
    avg_session_duration_secs   FLOAT,
    avg_pages_per_session       FLOAT,
    bounce_rate                 FLOAT,
    mobile_pct                  FLOAT,
    desktop_pct                 FLOAT,
    tablet_pct                  FLOAT,
    top_countries               VARIANT,
    updated_at                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

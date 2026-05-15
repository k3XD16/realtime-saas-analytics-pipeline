-- ============================================================
-- ADHOC QUERIES — Real-Time SaaS Analytics Pipeline
-- Use these to verify pipeline health and explore data
-- ============================================================


-- ── 1. ROW COUNT VERIFICATION ────────────────────────────────
SELECT 'gold_daily_active_users' AS table_name, COUNT(*) AS row_count FROM gold_daily_active_users UNION ALL
SELECT 'gold_mrr_metrics',                       COUNT(*) FROM gold_mrr_metrics                    UNION ALL
SELECT 'gold_funnel_metrics',                    COUNT(*) FROM gold_funnel_metrics                  UNION ALL
SELECT 'gold_feature_adoption',                  COUNT(*) FROM gold_feature_adoption                UNION ALL
SELECT 'gold_session_quality',                   COUNT(*) FROM gold_session_quality
ORDER BY table_name;


-- ── 2. DUPLICATE CHECK ───────────────────────────────────────
SELECT 'daily_active_users' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT date) AS unique_keys,
    CASE WHEN COUNT(*) = COUNT(DISTINCT date)
        THEN '✅ No Duplicates' ELSE '❌ Duplicates Found' END AS status
FROM gold_daily_active_users UNION ALL
SELECT 'mrr_metrics',
    COUNT(*), COUNT(DISTINCT date),
    CASE WHEN COUNT(*) = COUNT(DISTINCT date)
        THEN '✅ No Duplicates' ELSE '❌ Duplicates Found' END
FROM gold_mrr_metrics UNION ALL
SELECT 'funnel_metrics',
    COUNT(*), COUNT(DISTINCT date),
    CASE WHEN COUNT(*) = COUNT(DISTINCT date)
        THEN '✅ No Duplicates' ELSE '❌ Duplicates Found' END
FROM gold_funnel_metrics UNION ALL
SELECT 'feature_adoption',
    COUNT(*), COUNT(DISTINCT date || feature_name),
    CASE WHEN COUNT(*) = COUNT(DISTINCT date || feature_name)
        THEN '✅ No Duplicates' ELSE '❌ Duplicates Found' END
FROM gold_feature_adoption UNION ALL
SELECT 'session_quality',
    COUNT(*), COUNT(DISTINCT date),
    CASE WHEN COUNT(*) = COUNT(DISTINCT date)
        THEN '✅ No Duplicates' ELSE '❌ Duplicates Found' END
FROM gold_session_quality;


-- ── 3. STAGE FILE CHECK ──────────────────────────────────────
-- Verify parquet files are visible in each stage
LIST @stg_daily_active_users;
LIST @stg_mrr_metrics;
LIST @stg_funnel_metrics;
LIST @stg_feature_adoption;
LIST @stg_session_quality;


-- ── 4. LATEST DATA DATE CHECK ────────────────────────────────
-- Confirm most recent date loaded across all tables
SELECT 'daily_active_users' AS table_name, MAX(date) AS latest_date FROM gold_daily_active_users UNION ALL
SELECT 'mrr_metrics',                       MAX(date) FROM gold_mrr_metrics                    UNION ALL
SELECT 'funnel_metrics',                    MAX(date) FROM gold_funnel_metrics                  UNION ALL
SELECT 'feature_adoption',                  MAX(date) FROM gold_feature_adoption                UNION ALL
SELECT 'session_quality',                   MAX(date) FROM gold_session_quality
ORDER BY table_name;


-- ── 5. SAMPLE DATA PREVIEW ───────────────────────────────────
SELECT * FROM gold_daily_active_users   ORDER BY date DESC LIMIT 5;
SELECT * FROM gold_mrr_metrics          ORDER BY date DESC LIMIT 5;
SELECT * FROM gold_funnel_metrics       ORDER BY date DESC LIMIT 5;
SELECT * FROM gold_feature_adoption     ORDER BY date DESC LIMIT 5;
SELECT * FROM gold_session_quality      ORDER BY date DESC LIMIT 5;


-- ── 6. MRR TREND (Last 30 Days) ──────────────────────────────
SELECT date, total_mrr, net_mrr, new_mrr, churned_mrr
FROM gold_mrr_metrics
ORDER BY date DESC
LIMIT 30;


-- ── 7. TOP FEATURES BY USAGE ─────────────────────────────────
SELECT feature_name,
    SUM(total_uses)    AS total_uses,
    SUM(unique_users)  AS unique_users,
    ROUND(AVG(avg_duration_seconds), 2) AS avg_duration_secs
FROM gold_feature_adoption
GROUP BY feature_name
ORDER BY total_uses DESC;


-- ── 8. DAU / WAU / MAU TREND ─────────────────────────────────
SELECT date, dau_count, wau_count, mau_count,
    ROUND(dau_count * 100.0 / NULLIF(mau_count, 0), 2) AS dau_mau_ratio
FROM gold_daily_active_users
ORDER BY date DESC
LIMIT 30;
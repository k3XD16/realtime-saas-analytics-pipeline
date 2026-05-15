# Data Dictionary

Describes all table schemas across Bronze, Silver, and Gold layers stored in S3 Delta Lake and Snowflake.

## Bronze Layer

### Table: `bronze.events`
Raw events ingested directly from Kafka.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `value` | BINARY | N | Raw Avro-serialized event payload. |
| `ingested_at` | TIMESTAMP | N | Timestamp when the record was written to Bronze. |
| `kafka_offset` | LONG | N | Kafka message offset. |
| `kafka_partition` | INT | N | Kafka partition number. |
| `kafka_topic` | STRING | N | Source Kafka topic name. |
| `processing_date` | DATE | N | Partition key (date of ingestion). |

---

## Silver Layer

### Table: `silver.events`
Cleaned and flattened event data.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `event_id` | STRING | N | Unique event identifier (Primary Key). |
| `event_type` | STRING | N | Type of event (e.g., page_view, signup, upgrade). |
| `event_timestamp` | TIMESTAMP | N | The original timestamp from the event payload. |
| `session_id` | STRING | Y | Unique session identifier. |
| `user_id` | STRING | Y | Unique user identifier (if authenticated). |
| `anonymous_id` | STRING | Y | Identifier for anonymous users. |
| `page` | STRING | Y | The URL path of the page viewed. |
| `referrer` | STRING | Y | The referring URL. |
| `feature_name` | STRING | Y | Name of the feature used (for feature_used events). |
| `device` | STRING | Y | User device type (mobile, desktop, tablet). |
| `country` | STRING | Y | Country code (ISO). |
| `city` | STRING | Y | City name. |
| `plan` | STRING | Y | The user's current subscription plan. |
| `mrr_usd` | DOUBLE | Y | MRR amount in USD (for upgrade events). |
| `mrr_lost_usd` | DOUBLE | Y | MRR amount lost (for churn events). |
| `from_plan` | STRING | Y | Previous plan before upgrade. |
| `to_plan` | STRING | Y | New plan after upgrade. |
| `duration_seconds` | INT | Y | Feature usage duration in seconds. |
| `reason` | STRING | Y | Reason for churn (for churn events). |
| `processed_at` | TIMESTAMP | N | Timestamp when the record was processed into Silver. |
| `processing_date` | DATE | N | Partition key (date of the event). |

### Table: `silver.sessions`
Stitched session data with engagement metrics.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `session_id` | STRING | N | Unique session identifier (Primary Key). |
| `user_id` | STRING | Y | Unique user identifier. |
| `anonymous_id` | STRING | Y | Identifier for anonymous users. |
| `device` | STRING | Y | Device type used during the session. |
| `country` | STRING | Y | Country where the session originated. |
| `session_start` | TIMESTAMP | N | Timestamp of the first event in the session. |
| `session_end` | TIMESTAMP | N | Timestamp of the last event in the session. |
| `page_count` | LONG | N | Total number of page views in the session. |
| `entry_page` | STRING | Y | The first page visited in the session. |
| `exit_page` | STRING | Y | The last page visited in the session. |
| `session_duration_secs` | LONG | N | Total duration of the session in seconds. |
| `is_bounce` | BOOLEAN | N | Flag indicating if the session was a bounce (page_count = 1). |
| `session_date` | DATE | N | Partition key (date of session start). |

### Table: `silver.users` (SCD Type 2)
Slowly Changing Dimension table for user profiles and plans.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `user_id` | STRING | N | Unique user identifier. |
| `plan` | STRING | N | Current subscription plan. |
| `country` | STRING | Y | User's country. |
| `device` | STRING | Y | User's primary device. |
| `valid_from` | TIMESTAMP | N | Start timestamp for this record's validity. |
| `valid_to` | TIMESTAMP | Y | End timestamp for this record's validity (NULL if current). |
| `is_current` | BOOLEAN | N | Flag indicating if this is the latest record for the user. |
| `updated_at` | TIMESTAMP | N | Timestamp when the record was last updated. |

---

## Gold Layer

### Table: `gold.daily_active_users`
Daily user engagement trends.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | DATE | N | The date of the metrics. |
| `dau_count` | INT | N | Daily Active Users count. |
| `new_users` | INT | N | Number of new signups on this date. |
| `returning_users` | INT | N | Number of returning users on this date. |
| `wau_count` | INT | N | Weekly Active Users (rolling 7-day). |
| `mau_count` | INT | N | Monthly Active Users (rolling 30-day). |

### Table: `gold.funnel_metrics`
Conversion rates across the user journey.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | DATE | N | The date of the metrics. |
| `total_page_views` | INT | N | Total page views. |
| `unique_visitors` | INT | N | Total unique anonymous visitors. |
| `signups` | INT | N | Total signups. |
| `logins` | INT | N | Total logins. |
| `feature_activations` | INT | N | Total feature usage events. |
| `upgrades` | INT | N | Total plan upgrades. |
| `churns` | INT | N | Total user churns. |
| `visitor_to_signup_rate` | DOUBLE | N | Conversion rate from visitor to signup. |
| `signup_to_activation_rate` | DOUBLE | N | Rate of signups using a feature. |
| `activation_to_upgrade_rate` | DOUBLE | N | Rate of active users upgrading to Pro/Enterprise. |
| `upgrade_to_churn_rate` | DOUBLE | N | Churn rate relative to upgrades. |

### Table: `gold.feature_adoption`
Usage statistics per feature.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | DATE | N | The date of the metrics. |
| `feature_name` | STRING | N | Name of the feature. |
| `unique_users` | INT | N | Number of unique users who used this feature. |
| `total_uses` | INT | N | Total number of times this feature was used. |
| `avg_duration_seconds` | DOUBLE | N | Average usage duration per session. |
| `plan_breakdown` | STRING | N | JSON string showing usage count per plan. |

### Table: `gold.mrr_metrics`
Financial performance and subscription growth.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | DATE | N | The date of the metrics. |
| `new_mrr` | DOUBLE | N | MRR from new upgrades. |
| `churned_mrr` | DOUBLE | N | MRR lost due to churn. |
| `expansion_mrr` | DOUBLE | N | Estimated expansion MRR. |
| `net_mrr` | DOUBLE | N | Net MRR change (New + Expansion - Churn). |
| `total_mrr` | DOUBLE | N | Estimated total MRR. |
| `pro_users` | INT | N | Total count of Pro plan users. |
| `enterprise_users` | INT | N | Total count of Enterprise plan users. |
| `plan` | STRING | Y | Subscription plan (if applicable). |
| `mrr_usd` | DOUBLE | Y | MRR amount in USD. |

### Table: `gold.session_quality`
User engagement and traffic quality metrics.

| Column | Data Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `date` | DATE | N | The date of the metrics. |
| `avg_session_duration_secs` | DOUBLE | N | Average session duration in seconds. |
| `avg_pages_per_session` | DOUBLE | N | Average number of pages visited per session. |
| `bounce_rate` | DOUBLE | N | Percentage of single-page sessions. |
| `mobile_pct` | DOUBLE | N | Percentage of sessions from mobile devices. |
| `desktop_pct` | DOUBLE | N | Percentage of sessions from desktop devices. |
| `tablet_pct` | DOUBLE | N | Percentage of sessions from tablet devices. |
| `top_countries` | STRING | N | JSON string showing session distribution by country. |

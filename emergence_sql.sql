-- Create the database if it does not exist
CREATE DATABASE IF NOT EXISTS saas_analytics;

-- Use the database
USE saas_analytics;

-- Create raw tables for the datasets.
-- Assumptions: IDs are strings (padded like C0001), dates as DATE, prices as DECIMAL for precision.
-- No PK/FK enforced initially to allow loading dirty data; add in cleaning if needed.

-- Drop existing tables to start clean
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS customers;

-- Create raw/staging tables with loose types to accept any CSV data
CREATE TABLE customers (
    customer_id VARCHAR(20),
    signup_date VARCHAR(20),      -- Keep as string initially
    segment VARCHAR(50),
    country VARCHAR(10),
    is_enterprise VARCHAR(10)     -- Accept 'True'/'False' as text
);

CREATE TABLE subscriptions (
    subscription_id VARCHAR(20),
    customer_id VARCHAR(20),
    start_date VARCHAR(20),
    end_date VARCHAR(20),
    monthly_price VARCHAR(20),
    status VARCHAR(20)
);

CREATE TABLE events (
    event_id VARCHAR(20),
    customer_id VARCHAR(20),
    event_type VARCHAR(50),
    event_date VARCHAR(20),
    source VARCHAR(50)
);

-- verify data load
SELECT COUNT(*) FROM customers;        -- 1000
SELECT COUNT(*) FROM subscriptions;    -- 941
SELECT COUNT(*) FROM events;           -- 2411

SELECT * FROM customers LIMIT 5;
SELECT * FROM subscriptions LIMIT 5;
----------------------------------------------------------
-- Clean customers: proper dates, boolean, filled segment
CREATE TABLE customers_cleaned AS
SELECT 
    customer_id,
    STR_TO_DATE(NULLIF(TRIM(signup_date), ''), '%Y-%m-%d') AS signup_date,
    NULLIF(TRIM(segment), '') AS segment,
    TRIM(country) AS country,
    CASE 
        WHEN UPPER(TRIM(is_enterprise)) IN ('TRUE', 'T', '1') THEN 1
        WHEN UPPER(TRIM(is_enterprise)) IN ('FALSE', 'F', '0') THEN 0
        ELSE NULL 
    END AS is_enterprise
FROM customers;

-- Add proper constraints
ALTER TABLE customers_cleaned 
    ADD PRIMARY KEY (customer_id),
    MODIFY is_enterprise TINYINT(1) NOT NULL,
    MODIFY signup_date DATE,
    MODIFY segment VARCHAR(20),
    MODIFY country VARCHAR(2);

-- Fill missing segments with 'Unknown'
UPDATE customers_cleaned 
SET segment = 'Unknown' 
WHERE segment IS NULL OR segment = '';

-- Clean subscriptions: proper dates and numeric price
CREATE TABLE subscriptions_cleaned AS
SELECT 
    subscription_id,
    customer_id,
    STR_TO_DATE(TRIM(start_date), '%Y-%m-%d') AS start_date,
    STR_TO_DATE(NULLIF(TRIM(end_date), ''), '%Y-%m-%d') AS end_date,
    CAST(TRIM(monthly_price) AS DECIMAL(10,2)) AS monthly_price,
    TRIM(status) AS status
FROM subscriptions;

ALTER TABLE subscriptions_cleaned 
    ADD PRIMARY KEY (subscription_id),
    MODIFY start_date DATE NOT NULL,
    MODIFY monthly_price DECIMAL(10,2) NOT NULL,
    MODIFY status VARCHAR(10) NOT NULL;

-- Clean events: proper date
CREATE TABLE events_cleaned AS
SELECT 
    event_id,
    customer_id,
    TRIM(event_type) AS event_type,
    STR_TO_DATE(TRIM(event_date), '%Y-%m-%d') AS event_date,
    TRIM(source) AS source
FROM events;

ALTER TABLE events_cleaned 
    ADD PRIMARY KEY (event_id),
    MODIFY event_date DATE NOT NULL;
    
-------------------------------------------------

-- Check row counts (should match raw)
SELECT COUNT(*) AS customers_cleaned FROM customers_cleaned;        -- 1000
SELECT COUNT(*) AS subscriptions_cleaned FROM subscriptions_cleaned; -- 941
SELECT COUNT(*) AS events_cleaned FROM events_cleaned;              -- 2411

-- Spot checks
SELECT * FROM customers_cleaned LIMIT 5;
SELECT * FROM subscriptions_cleaned LIMIT 5;
SELECT * FROM events_cleaned LIMIT 5;

-- Check boolean and segment fix
SELECT is_enterprise, COUNT(*) FROM customers_cleaned GROUP BY is_enterprise;
SELECT segment, COUNT(*) FROM customers_cleaned GROUP BY segment;

-- Check no invalid dates
SELECT COUNT(*) FROM customers_cleaned WHERE signup_date IS NULL;      -- Should be 0 after fix
SELECT COUNT(*) FROM events_cleaned WHERE event_date IS NULL;          -- Should be 0
SELECT COUNT(*) FROM subscriptions_cleaned WHERE start_date IS NULL;          -- Should be 0
SELECT COUNT(*) FROM subscriptions_cleaned WHERE end_date IS NULL;          -- Should be 0



-- Deduplication
-- Step 1: Create a new temporary table with deduplicated data
CREATE TABLE events_cleaned_dedup AS
SELECT MIN(event_id) AS event_id,
       customer_id,
       event_type,
       event_date,
       source
FROM events_cleaned
GROUP BY customer_id, event_type, event_date, source;

-- Step 2: Drop the old events_cleaned
DROP TABLE events_cleaned;

-- Step 3: Rename the deduplicated one to the final name
ALTER TABLE events_cleaned_dedup RENAME TO events_cleaned;

-- Step 4: Re-add primary key and indexes
ALTER TABLE events_cleaned 
    ADD PRIMARY KEY (event_id),
    MODIFY event_date DATE NOT NULL;
CREATE INDEX idx_events_customer ON events_cleaned (customer_id);
CREATE INDEX idx_events_type_date ON events_cleaned (event_type, event_date);

-- Deduplicate subscriptions
CREATE TABLE subscriptions_cleaned_dedup AS
SELECT MIN(subscription_id) AS subscription_id,
       customer_id,
       start_date,
       end_date,
       monthly_price,
       status
FROM subscriptions_cleaned
GROUP BY customer_id, start_date, end_date, monthly_price, status;

DROP TABLE subscriptions_cleaned;

ALTER TABLE subscriptions_cleaned_dedup RENAME TO subscriptions_cleaned;

ALTER TABLE subscriptions_cleaned 
    ADD PRIMARY KEY (subscription_id),
    MODIFY start_date DATE NOT NULL,
    MODIFY monthly_price DECIMAL(10,2) NOT NULL,
    MODIFY status VARCHAR(10) NOT NULL;

-- 1. Check if customer_id is truly unique
SELECT customer_id, COUNT(*) AS row_count
FROM customers  -- or customers_cleaned
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- 0 rows returned (no dedupe needed)

-- 2. Check for any full duplicate rows
SELECT customer_id, signup_date, segment, country, is_enterprise, COUNT(*) AS dup_count
FROM customers_cleaned
GROUP BY customer_id, signup_date, segment, country, is_enterprise
HAVING COUNT(*) > 1;
-- 0 rows (no dedupe needed)

-- Step 1: Create a new table with the updated signup_date
CREATE TABLE customers_cleaned_updated AS
SELECT 
    c.customer_id,
    COALESCE(e.event_signup_date, c.signup_date) AS signup_date,
    c.segment,
    c.country,
    c.is_enterprise
FROM customers_cleaned c
LEFT JOIN (
    SELECT 
        customer_id,
        MIN(event_date) AS event_signup_date
    FROM events_cleaned
    WHERE event_type = 'signup'
    GROUP BY customer_id
) e ON c.customer_id = e.customer_id;

-- Step 2: Drop the old customers_cleaned table
DROP TABLE customers_cleaned;

-- Step 3: Rename the new one to the final name
ALTER TABLE customers_cleaned_updated RENAME TO customers_cleaned;

-- Step 4: Re-add the primary key and any indexes if needed 
ALTER TABLE customers_cleaned 
    ADD PRIMARY KEY (customer_id),
    MODIFY signup_date DATE;

-- Final verification: no missing signup dates
SELECT COUNT(*) AS missing_signup_dates FROM customers_cleaned WHERE signup_date IS NULL;
-- Expected result: 0

-- =============================================
-- Core SaaS Metrics
-- =============================================

-- ==================================================
-- UNIFIED SaaS Metrics Table: Overall + Detailed Breakdowns
-- ==================================================

-- UNIFIED Metrics Table - FIXED Ambiguity & Warnings

-- ==================================================
-- FINAL UNIFIED SaaS Metrics Table - NO MORE ERRORS
-- ==================================================

-- ==================================================
-- FINAL - UNIFIED SaaS Metrics Table (Overall + Detailed) - NO ALIAS ERRORS
-- ==================================================

-- ==================================================
-- FINAL UNIFIED METRICS TABLE - WARNING-FREE VERSION
-- ==================================================

-- ==================================================
-- FINAL UNIFIED METRICS TABLE - ALL ERRORS FIXED
-- ==================================================

-- ==================================================
-- FINAL UNIFIED METRICS TABLE - ALL ERRORS FIXED
-- ==================================================

-- ==================================================
-- FINAL UNIFIED METRICS TABLE - ALL ERRORS & WARNINGS FIXED
-- ==================================================

-- ==================================================
-- SIMPLIFIED DETAILED SAA S METRICS TABLE - FOR POWER BI FILTERING
-- ==================================================

-- ==================================================
-- FINAL UNIFIED METRICS TABLE - FIXED ONLY_FULL_GROUP_BY ERROR
-- ==================================================

-- Step 1: Temporarily disable strict group by mode (session only)
SET SESSION sql_mode = REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', '');

-- Step 2: Create the detailed metrics table
DROP TABLE IF EXISTS core_saas_metrics_detailed;

CREATE TABLE core_saas_metrics_detailed AS
WITH RECURSIVE months AS (
    SELECT DATE_FORMAT(MIN(signup_date), '%Y-%m-01') AS month_start
    FROM customers_cleaned
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM months
    WHERE month_start < (
        SELECT DATE_FORMAT(MAX(the_date), '%Y-%m-01')
        FROM (
            SELECT end_date AS the_date FROM subscriptions_cleaned WHERE end_date IS NOT NULL
            UNION ALL SELECT event_date AS the_date FROM events_cleaned
            UNION ALL SELECT start_date AS the_date FROM subscriptions_cleaned
            UNION ALL SELECT signup_date AS the_date FROM customers_cleaned
        ) all_dates
    )
),
month_ends AS (
    SELECT month_start, LAST_DAY(month_start) AS month_end FROM months
),

-- Active customers per month with source
detailed_active AS (
    SELECT 
        m.month_start,
        s.customer_id,
        c.segment,
        c.country,
        COALESCE((SELECT MAX(es.source) FROM events_cleaned es WHERE es.customer_id = s.customer_id AND es.event_type = 'signup'), 'Unknown') AS source
    FROM month_ends m
    JOIN subscriptions_cleaned s 
        ON s.start_date <= m.month_end
        AND (s.end_date IS NULL OR s.end_date > m.month_end)
        AND s.status = 'active'
    JOIN customers_cleaned c ON s.customer_id = c.customer_id
    GROUP BY m.month_start, s.customer_id, c.segment, c.country
),

-- Revenue per month per breakdown
detailed_revenue AS (
    SELECT
        da.month_start,
        da.segment,
        da.country,
        da.source,
        COALESCE(SUM(s.monthly_price), 0) AS mrr,
        COALESCE(SUM(s.monthly_price), 0) * 12 AS arr,
        COUNT(DISTINCT da.customer_id) AS active_customers
    FROM detailed_active da
    JOIN subscriptions_cleaned s 
        ON s.customer_id = da.customer_id
        AND s.start_date <= da.month_start + INTERVAL 1 MONTH - INTERVAL 1 DAY
        AND (s.end_date IS NULL OR s.end_date > da.month_start + INTERVAL 1 MONTH - INTERVAL 1 DAY)
        AND s.status = 'active'
    GROUP BY da.month_start, da.segment, da.country, da.source
),

-- Churn base (pre-aggregate)
detailed_churn_base AS (
    SELECT 
        c.customer_id,
        c.segment,
        c.country,
        DATE_FORMAT(COALESCE(e.event_date, s.end_date), '%Y-%m-01') AS month_start,
        COALESCE((SELECT MAX(es.source) FROM events_cleaned es WHERE es.customer_id = c.customer_id AND es.event_type = 'signup'), 'Unknown') AS source,
        s.monthly_price
    FROM customers_cleaned c
    LEFT JOIN events_cleaned e ON c.customer_id = e.customer_id AND e.event_type = 'churned'
    LEFT JOIN subscriptions_cleaned s ON c.customer_id = s.customer_id
        AND s.status = 'canceled' AND s.end_date IS NOT NULL
    WHERE e.event_date IS NOT NULL OR s.end_date IS NOT NULL
),

-- Churn aggregated
detailed_churned AS (
    SELECT
        month_start,
        segment,
        country,
        source,
        COUNT(DISTINCT customer_id) AS churned_logos,
        COALESCE(SUM(monthly_price), 0) AS lost_mrr
    FROM detailed_churn_base
    GROUP BY month_start, segment, country, source
),

-- Final metrics
detailed_metrics AS (
    SELECT
        dr.month_start,
        dr.segment,
        dr.country,
        dr.source,
        dr.mrr,
        dr.arr,
        dr.active_customers,
        COALESCE(dc.churned_logos, 0) AS churned_logos,
        COALESCE(dc.lost_mrr, 0) AS lost_mrr,
        ROUND(COALESCE(dc.churned_logos / NULLIF(LAG(dr.active_customers) OVER (PARTITION BY dr.source, dr.segment, dr.country ORDER BY dr.month_start), 0), 0) * 100, 2) AS logo_churn_rate_pct,
        ROUND(COALESCE(dc.lost_mrr / NULLIF(LAG(dr.mrr) OVER (PARTITION BY dr.source, dr.segment, dr.country ORDER BY dr.month_start), 0), 0) * 100, 2) AS revenue_churn_rate_pct,
        ROUND(dr.mrr / NULLIF(dr.active_customers, 0), 2) AS arpc
    FROM detailed_revenue dr
    LEFT JOIN detailed_churned dc 
        ON dr.month_start = dc.month_start
        AND dr.source = dc.source
        AND dr.segment = dc.segment
        AND dr.country = dc.country
)
SELECT * FROM detailed_metrics
ORDER BY month_start, segment, country, source;

SELECT * FROM core_saas_metrics_detailed;


-- ==================================================
-- Funnel Performance - Detailed Breakdown by Month, Source, Segment, Country
-- ==================================================

-- ==================================================
-- UPDATED Funnel Performance - With Inconsistency Flag & Caps
-- ==================================================

-- ==================================================
-- FINAL Funnel Performance - NULLs to 0 in % Columns
-- ==================================================

-- ==================================================
-- REVISED Funnel Performance - Aggregated to Unique Rows + NULL Handling
-- ==================================================

DROP TABLE IF EXISTS funnel_performance_detailed;

CREATE TABLE funnel_performance_detailed AS
WITH RECURSIVE months AS (
    SELECT DATE_FORMAT(MIN(signup_date), '%Y-%m-01') AS month_start
    FROM customers_cleaned
    UNION ALL
    SELECT DATE_ADD(month_start, INTERVAL 1 MONTH)
    FROM months
    WHERE month_start < (
        SELECT DATE_FORMAT(MAX(the_date), '%Y-%m-01')
        FROM (
            SELECT end_date AS the_date FROM subscriptions_cleaned WHERE end_date IS NOT NULL
            UNION ALL SELECT event_date AS the_date FROM events_cleaned
            UNION ALL SELECT start_date AS the_date FROM subscriptions_cleaned
            UNION ALL SELECT signup_date AS the_date FROM customers_cleaned
        ) all_dates
    )
),
customer_funnel_base AS (
    SELECT 
        DATE_FORMAT(c.signup_date, '%Y-%m-01') AS month_start,
        c.customer_id,
        COALESCE(c.segment, 'Unknown') AS segment,
        COALESCE(c.country, 'Unknown') AS country,
        COALESCE(MAX(es.source), 'Unknown') AS source,
        1 AS signup,
        CASE WHEN MIN(CASE WHEN e.event_type = 'trial_start' THEN e.event_date END) IS NOT NULL THEN 1 ELSE 0 END AS trial,
        CASE WHEN MIN(CASE WHEN e.event_type = 'activated' THEN e.event_date END) IS NOT NULL THEN 1 ELSE 0 END AS activated,
        CASE WHEN MIN(s.start_date) IS NOT NULL THEN 1 ELSE 0 END AS paid,
        CASE WHEN MIN(CASE WHEN e.event_type = 'churned' THEN e.event_date END) IS NOT NULL OR MAX(CASE WHEN s.status = 'canceled' THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS churned
    FROM customers_cleaned c
    LEFT JOIN events_cleaned e ON c.customer_id = e.customer_id
    LEFT JOIN subscriptions_cleaned s ON c.customer_id = s.customer_id AND s.status = 'active'
    LEFT JOIN events_cleaned es ON c.customer_id = es.customer_id AND es.event_type = 'signup'
    GROUP BY month_start, c.customer_id, segment, country
),
funnel_aggregated_raw AS (
    SELECT
        month_start,
        segment,
        country,
        source,
        SUM(signup) AS total_signups,
        SUM(trial) AS total_trials,
        SUM(activated) AS total_activated,
        SUM(paid) AS total_paid,
        SUM(CASE WHEN paid = 1 AND churned = 1 THEN 1 ELSE 0 END) AS total_churned
    FROM customer_funnel_base
    GROUP BY month_start, segment, country, source
),
funnel_aggregated AS (
    SELECT
        month_start,
        segment,
        country,
        source,
        SUM(total_signups) AS total_signups,
        SUM(total_trials) AS total_trials,
        SUM(total_activated) AS total_activated,
        SUM(total_paid) AS total_paid,
        SUM(total_churned) AS total_churned
    FROM funnel_aggregated_raw
    GROUP BY month_start, segment, country, source  -- Final dedup sum
),
funnel_with_flags AS (
    SELECT
        month_start,
        segment,
        country,
        source,
        total_signups,
        total_trials,
        total_activated,
        total_paid,
        total_churned,
        COALESCE(LEAST(ROUND(total_trials / NULLIF(total_signups, 0) * 100, 2), 100), 0) AS signup_to_trial_pct,
        COALESCE(100 - LEAST(ROUND(total_trials / NULLIF(total_signups, 0) * 100, 2), 100), 0) AS signup_dropoff_pct,
        COALESCE(LEAST(ROUND(total_activated / NULLIF(total_trials, 0) * 100, 2), 100), 0) AS trial_to_activated_pct,
        COALESCE(100 - LEAST(ROUND(total_activated / NULLIF(total_trials, 0) * 100, 2), 100), 0) AS trial_dropoff_pct,
        COALESCE(LEAST(ROUND(total_paid / NULLIF(total_activated, 0) * 100, 2), 100), 0) AS activated_to_paid_pct,
        COALESCE(100 - LEAST(ROUND(total_paid / NULLIF(total_activated, 0) * 100, 2), 100), 0) AS activated_dropoff_pct,
        COALESCE(LEAST(ROUND(total_churned / NULLIF(total_paid, 0) * 100, 2), 100), 0) AS paid_to_churn_pct,
        COALESCE(GREATEST(100 - LEAST(ROUND(total_churned / NULLIF(total_paid, 0) * 100, 2), 100), 0), 0) AS paid_retention_pct,
        TRIM(
            CONCAT(
                CASE WHEN total_paid > total_activated THEN 'paid > activated, ' ELSE '' END,
                CASE WHEN total_paid > total_trials THEN 'paid > trials, ' ELSE '' END,
                CASE WHEN total_churned > total_paid THEN 'churned > paid, ' ELSE '' END,
                CASE WHEN total_signups = 0 AND (total_trials > 0 OR total_activated > 0 OR total_paid > 0) THEN 'no signups but later stages, ' ELSE '' END
            )
        ) AS data_flag_raw
    FROM funnel_aggregated
)
SELECT
    month_start,
    segment,
    country,
    source,
    total_signups,
    total_trials,
    total_activated,
    total_paid,
    total_churned,
    signup_to_trial_pct,
    signup_dropoff_pct,
    trial_to_activated_pct,
    trial_dropoff_pct,
    activated_to_paid_pct,
    activated_dropoff_pct,
    paid_to_churn_pct,
    paid_retention_pct,
    CASE 
        WHEN data_flag_raw = '' THEN 'consistent'
        ELSE TRIM(TRAILING ',' FROM data_flag_raw)
    END AS data_flag
FROM funnel_with_flags
ORDER BY month_start, segment, country, source;

-- Verification
SELECT * FROM funnel_performance_detailed;
SELECT * FROM funnel_performance_detailed LIMIT 10;
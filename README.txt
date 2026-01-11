SaaS Analytics Project README

Date: January 11, 2026

Overview of the Analysis

This project delivers a comprehensive, end-to-end analytics workflow for a B2B SaaS company, transforming raw CSV data into actionable insights on revenue, customer retention, acquisition efficiency, and funnel performance. Following the assessment guidelines, the analysis involves loading and validating three datasets (customers.csv, subscriptions.csv, events.csv), performing thorough cleaning to resolve inconsistencies, calculating core SaaS metrics (Monthly Recurring Revenue [MRR], Annual Recurring Revenue [ARR], customer churn rates, Average Revenue Per Customer [ARPC]), and building a cohort-based funnel (Signup → Trial → Activated → Paid → Churned) with conversion rates, drop-off points, and breakdowns by acquisition source, customer segment, and country.
The workflow emphasizes data quality (e.g., handling duplicates and mismatches), reproducibility (via scripted processes), and visualization for stakeholder decision-making. Key outputs include:

Validated and cleaned tables in MySQL.
A detailed metrics table (core_saas_metrics_detailed) with monthly breakdowns (e.g., MRR by segment/source/country).
A funnel performance table (funnel_performance_detailed) with rates and flags for anomalies.
An interactive Power BI dashboard for filtering and exploration.

The analysis covers January to July 2023 data, revealing rapid Q1 growth (MRR from $43k to $176k), followed by stagnation, improving churn, and funnel bottlenecks. These findings highlight opportunities for retention optimization and acquisition revival.
Tools Used

Python 3.8+: For data loading, initial exploration, validation, and sanity checks. Libraries: pandas (data manipulation and Excel output), numpy (numerical computations), sqlalchemy and mysql-connector-python (MySQL connection and loading), openpyxl (Excel writing). Script: data_validation.py — outputs validation_results.xlsx for issue documentation.
MySQL 8.0: For database storage, data cleaning (deduplication, NULL handling, date overrides), and analytical queries (joins, CTEs, window functions like LAG for churn rates, recursive CTEs for month generation, date-based aggregations). Scripts in /sql folder, executed in sequence for reproducibility.
Power BI Desktop: For interactive dashboard building. Imported MySQL tables, created custom measures (e.g., summed MRR for overall views), slicers (month, segment, country, source, data_flag), and visuals (line charts for MRR growth, funnel charts for conversions, KPIs for ARPC/churn, matrices for breakdowns). File: saas_metrics_dashboard.pbix — supports filtering anomalies and drilling down.

These tools were chosen for their accessibility, scalability, and integration (Python for validation, MySQL for heavy lifting, Power BI for visualization).
Data Issues Identified
The datasets were analyzed using Python for initial validation (validation_results.xlsx with 20+ sheets), revealing several quality issues. These were mitigated in SQL cleaning without data loss, but flagged where relevant (e.g., in funnel data_flag). Key issues:

Missing Values: 36 missing signup_dates in customers.csv (3.6% of 1,000 rows), fixed by overriding with earliest 'signup' event_date. 243 missing segments (24.3%), set to 'Unknown' to avoid bias. No missing in critical fields like customer_id or subscription status. Handled with COALESCE in SQL.
Duplicates: 94 customers with identical 'signup' events (same date/source, different event_id) — deduped by keeping MIN(event_id). 35 customers with duplicate subscriptions — deduped similarly. No duplicates in customers table (unique customer_id confirmed).
Date Mismatches: 954 signup_date mismatches between customers and events (e.g., profile date later than event — 1040 events before signup_date). Overridden with event as truth (more reliable log). No invalid dates (start > end) or future dates.
Funnel Anomalies: ~25% of 647 funnel rows flagged as inconsistent (e.g., paid > activated in 159 rows, paid > trials in 77 rows). Likely missing events or bypass paths (e.g., direct paid for Enterprise). Rates capped at 0-100% to avoid negatives/infinities.
Other Sanity Checks: No negative prices, no active subs with end_date, no referential integrity issues (all customer_ids consistent across tables). Signups limited to Q1 2023 (no new cohorts after April — possible truncation). 647 rows in funnel table, with unique keys ensured via double aggregation.
Validation Cross-Check: Independent recalculation (e.g., summed Jan totals: 166 signups, 127 trials, 105 activated, 133 paid, 12 churned) matches query output.

These issues (e.g., 11.9% with paid > trials) indicate tracking gaps but were handled transparently for reliable insights.
Metric Definitions
Metrics are monthly, point-in-time snapshots with breakdowns by source/segment/country in core_saas_metrics_detailed (647 rows). Formulas use LAG for prior-month comparisons, COALESCE/NULLIF to handle zeros/NULLs.

month_start: First day of the month (cohort anchor).
segment: Customer type (SMB, Mid-Market, Enterprise, Unknown).
country: Customer location (AU, CA, DE, IN, UK, US, Unknown).
source: Acquisition channel (ads, organic, outbound, referral, Unknown).
mrr: Sum(monthly_price) for active subscriptions (start_date ≤ month_end, end_date NULL or > month_end, status = 'active').
arr: mrr × 12 (yearly projection).
active_customers: COUNT(DISTINCT customer_id) with active subscriptions.
churned_logos: COUNT(DISTINCT customer_id) with 'churned' event or canceled subscription in month.
lost_mrr: Sum(monthly_price) from churned subscriptions.
logo_churn_rate_pct: (churned_logos / LAG(active_customers)) × 100.
revenue_churn_rate_pct: (lost_mrr / LAG(mrr)) × 100.
arpc: mrr / active_customers.

Funnel metrics (in funnel_performance_detailed, cohort-based on signup month):

total_signups: COUNT(DISTINCT customer_id) with signup_date in month.
total_trials: Signups with 'trial_start' event.
total_activated: Trials with 'activated' event.
total_paid: Activated with active subscription start_date.
total_churned: Paid with 'churned' event or canceled subscription.
signup_to_trial_pct: (total_trials / total_signups) × 100 (capped 0-100).
signup_dropoff_pct: 100 - signup_to_trial_pct.
trial_to_activated_pct: (total_activated / total_trials) × 100 (capped).
trial_dropoff_pct: 100 - trial_to_activated_pct.
activated_to_paid_pct: (total_paid / total_activated) × 100 (capped).
activated_dropoff_pct: 100 - activated_to_paid_pct.
paid_to_churn_pct: (total_churned / total_paid) × 100 (capped).
paid_retention_pct: 100 - paid_to_churn_pct (capped ≥0).
data_flag: 'consistent' or anomaly details (e.g., 'paid > activated').

Key Insights
From core_saas_metrics_detailed (647 rows) and funnel_performance_detailed (647 rows), here are detailed, actionable findings for senior leadership. The data shows a promising Q1 launch but risks from stagnation and anomalies.

Revenue & Growth: MRR grew 306% from Jan ($43k) to Apr ($176k), then flatlined through July (ARR ~$2.1M). ARPC stabilized at ~$258, with Enterprise highest (~$300–400) and SMB lowest (~$200–300). Insight: Strong initial scaling, but no net additions post-Q1 (active_customers locked at 683) — likely acquisition halt. By source, ads drove ~$33k mrr in July (SMB UK highest at $3,319). Recommendation: Restart marketing (e.g., ads in US/UK, $2.4k mrr in July SMB US) to target 20% MoM growth, aiming for $300k MRR by Q4 2026.
Churn & Retention: Logo churn peaked at 21.5% in Apr (136 logos, $20k lost), improved to 1.6% by July (11 logos, $827 lost). Revenue churn lagged (12.8% Apr → 0.47% July), indicating retention of high-value customers. Insight: Early churn hit SMB hardest (e.g., 28.54% revenue churn in July SMB US ads), while Enterprise was resilient (0% in many breakdowns). Country-wise, US had higher lost_mrr ($699 in July SMB ads). Recommendation: Prioritize SMB retention (e.g., onboarding for ads source, reducing 25% churn in SMB US organic). Goal: Sustain <5% monthly churn to save $10k+ MRR/year.
Funnel Performance: Signup to trial averaged 80% (20% drop-off — main bottleneck). Trial to activated ~79% (21% drop-off). Activated to paid ~100% (capped; anomalies in 25% rows). Paid to churn ~14% (86% retention). Insight: Mid-funnel strong, but early drop-offs high in SMB (e.g., 0% signup-to-trial in Apr SMB IN outbound). Enterprise excelled (100% in many, e.g., Jan Enterprise CA referral). Ads source had ~66% signup-to-trial, referral lower (~33%). Anomalies (e.g., paid > trials in Jan NULL AU ads) in 25% rows suggest tracking gaps. Recommendation: Optimize early funnel (e.g., trial incentives for SMB/ads, increasing to 90% conversion). Audit anomalies (flagged rows) for lost revenue opportunities.
Breakdowns: Enterprise led value (high ARPC, low churn), SMB trailed (high churn in IN/UK). US/UK dominated mrr ($2.4k in July SMB US ads), IN/AU lower. Ads/outbound had better conversions but more anomalies. Insight: Segment focus could unlock growth (e.g., Enterprise US organic: 100% retention in Mar).
Observations for Leadership: Q1 success (367 signups, $176k peak MRR) stalled, with no new cohorts post-Apr — risk of decline if churn rises. Data anomalies (flagged) indicate ~25% tracking issues, potentially underestimating funnel leaks. Strengths: Retention improved 90% Q1–Q2, mid-funnel efficiency. Risks: SMB churn (up to 100% in some rows) and acquisition gap.

Dashboard Explanation
The Power BI dashboard (saas_metrics_dashboard.pbix) is interactive, with two pages for metrics/funnel. It imports core_saas_metrics_detailed and funnel_performance_detailed from MySQL, using DAX measures for aggregations (e.g., Total MRR = SUM(mrr)) and slicers for breakdowns.

Metrics Page: Line charts for MRR/ARR growth over months; bar charts for churn rates by segment/source; KPI cards for ARPC (overall/selected), active customers; matrix table for detailed rows (filter by country). Slicers: month_start, segment, country, source. Use data_flag slicer to exclude inconsistencies.
Funnel Page: Funnel visual for stages (total_signups → total_paid); line charts for conversion rates (e.g., signup_to_trial_pct) over months; bar for drop-offs by stage; table for churn/retention with flags. Slicers: same as metrics, plus data_flag (show 'consistent' for reliable views).
Usage: Select 'Enterprise' + 'ads' for drill-down (e.g., 66.67% signup-to-trial in Jan). Aggregates to overall when no filters. Conditional formatting: Red for high churn (>10%), green for high retention (>90%). Tooltips show data_flag explanations.

Assumptions and Limitations

Assumptions:
Event logs (events.csv) are the source of truth for dates/stages (e.g., override customers.signup_date with min 'signup' event for accuracy, as profile dates were mismatched in 95% cases).
NULLs/unknowns consolidated (e.g., segment/country/source to 'Unknown') to avoid bias, assuming they represent missing tracking rather than a separate category.
Funnel is sequential and cohort-based (by signup month) — assumes stages happen in order, but allows anomalies (flagged) for direct paths (e.g., no trial for Enterprise). Churn only from paid customers.
Rates capped at 0-100% to handle anomalies (e.g., paid > activated = 100% conversion, retention ≥0).
Data is Q1-focused (signups Jan-Apr); later months have activity (churn) but no new cohorts — assumed truncated dataset, not ongoing business halt.
Duplicates in raw events/subscriptions deduped by MIN(id) — assumes earliest is authoritative.

Limitations:
Signups stop after April — funnel has no rows for May-Jul, limiting long-term retention analysis (e.g., Jan cohort churn over 7 months not shown per month).
Anomalies (25% flagged, e.g., paid > trials) suggest tracking gaps — may understate drop-offs or overstate conversions; mitigated with data_flag, but requires audit.
No expansion/upsell data — metrics show gross churn (not net); ARPC assumes static plans.
Small subgroups (e.g., 1 customer) skew rates (e.g., 100% churn from 1 logo) — filter for larger groups in dashboard.
No seasonality (7 months data) or external factors (e.g., marketing spend) — insights are descriptive, not causal.
Power BI assumes MySQL connection; if offline, export CSVs from MySQL and import.


Instructions to Reproduce Results

Setup Environment:
Install MySQL 8.0+; create database saas_analytics (CREATE DATABASE saas_analytics; USE saas_analytics;).
Install Python 3.8+ with dependencies: pip install pandas numpy sqlalchemy mysql-connector-python openpyxl.
Install Power BI Desktop (free from Microsoft).

Load & Validate Data:
Place CSVs in /data.
Update DB credentials in data_validation.py (DB_HOST, DB_USER, DB_PASSWORD).
Run python data_validation.py — loads data to MySQL, generates validation_results.xlsx for issues.

Run SQL Scripts:
In MySQL Workbench: Run /sql/01_create_tables.sql (raw tables).
Run 02_data_cleaning.sql (cleaning, dedup, overrides).
Run 03_core_metrics.sql (metrics table with breakdowns).
Run 04_funnel_performance.sql (funnel table with rates/flags).

Power BI Dashboard:
Open saas_metrics_dashboard.pbix.
Connect to MySQL (Query: SELECT * FROM core_saas_metrics_detailed; SELECT * FROM funnel_performance_detailed;).
Refresh data.
Use slicers (month, segment, country, source, data_flag) to explore.
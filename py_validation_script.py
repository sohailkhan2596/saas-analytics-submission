

# data_validation.py
# This script handles loading the CSV files, initial data exploration, validation, and sanity checks.
# It also includes code to connect to MySQL and load the data into the database tables.
# Enhanced with more exploration, validations, sanity checks.
# Outputs results to an Excel file 'validation_results.xlsx' with multiple sheets.
# Assumptions:
# - MySQL server is running locally or accessible (update connection details as needed).
# - Database 'saas_analytics' is created beforehand.
# - Tables will be created via SQL scripts, but this script can load data after tables exist.
# - Using SQLAlchemy for ease of loading DataFrames to SQL.
# - Dates will be parsed during loading.
# - Current date for sanity check: 2026-01-09

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import os

CURRENT_DATE = datetime(2026, 1, 9)

# Create output directory if needed
os.makedirs('outputs', exist_ok=True)
EXCEL_PATH = 'C:/Users/sohail.khan/Downloads/emergence_py_validation_results.xlsx'

# Step 1: Load CSV files into Pandas DataFrames
# Update paths if needed; assuming files are in 'data/' directory.

customers_df = pd.read_csv('C:/Users/sohail.khan/Downloads/customers.csv', parse_dates=['signup_date'])
subscriptions_df = pd.read_csv('C:/Users/sohail.khan/Downloads/subscriptions.csv', parse_dates=['start_date', 'end_date'])
events_df = pd.read_csv('C:/Users/sohail.khan/Downloads/events.csv', parse_dates=['event_date'])

# Step 2: Enhanced Data Exploration

exploration_dict = {}

# Customers
customers_info = customers_df.info(verbose=True, show_counts=True)
customers_desc = customers_df.describe(include='all')
customers_unique_segments = customers_df['segment'].value_counts(dropna=False)
customers_unique_countries = customers_df['country'].value_counts(dropna=False)
customers_is_enterprise = customers_df['is_enterprise'].value_counts(dropna=False)

exploration_dict['customers_desc'] = customers_desc
exploration_dict['customers_unique_segments'] = customers_unique_segments.reset_index(name='count')
exploration_dict['customers_unique_countries'] = customers_unique_countries.reset_index(name='count')
exploration_dict['customers_is_enterprise'] = customers_is_enterprise.reset_index(name='count')

# Subscriptions
subscriptions_info = subscriptions_df.info(verbose=True, show_counts=True)
subscriptions_desc = subscriptions_df.describe(include='all')
subscriptions_unique_status = subscriptions_df['status'].value_counts(dropna=False)
subscriptions_price_dist = subscriptions_df['monthly_price'].value_counts(dropna=False)

exploration_dict['subscriptions_desc'] = subscriptions_desc
exploration_dict['subscriptions_unique_status'] = subscriptions_unique_status.reset_index(name='count')
exploration_dict['subscriptions_price_dist'] = subscriptions_price_dist.reset_index(name='count')

# Events
events_info = events_df.info(verbose=True, show_counts=True)
events_desc = events_df.describe(include='all')
events_type_counts = events_df['event_type'].value_counts(dropna=False)
events_source_counts = events_df['source'].value_counts(dropna=False)

exploration_dict['events_desc'] = events_desc
exploration_dict['events_type_counts'] = events_type_counts.reset_index(name='count')
exploration_dict['events_source_counts'] = events_source_counts.reset_index(name='count')

# Step 3: Missing Values

missing_dict = {
    'customers_missing': customers_df.isnull().sum().reset_index(name='missing_count'),
    'subscriptions_missing': subscriptions_df.isnull().sum().reset_index(name='missing_count'),
    'events_missing': events_df.isnull().sum().reset_index(name='missing_count')
}

# Step 4: Duplicates

duplicates_dict = {}

customers_duplicates = customers_df[customers_df.duplicated(keep=False)]
subscriptions_duplicates = subscriptions_df[subscriptions_df.duplicated(keep=False)]
events_duplicates = events_df[events_df.duplicated(keep=False)]

duplicates_dict['customers_duplicates'] = customers_duplicates
duplicates_dict['subscriptions_duplicates'] = subscriptions_duplicates
duplicates_dict['events_duplicates'] = events_duplicates

# Step 5: Enhanced Validation and Sanity Checks

validation_dict = {}

# 5.1: Unique customer_ids in customers
unique_customers = customers_df['customer_id'].nunique()
total_customers = len(customers_df)
validation_dict['customers_unique'] = pd.DataFrame([{'unique_count': unique_customers, 'total_rows': total_customers}])

# 5.2: Duplicate customer_ids in customers
duplicate_customer_ids = customers_df[customers_df.duplicated(subset=['customer_id'], keep=False)]

validation_dict['duplicate_customer_ids'] = duplicate_customer_ids

# 5.3: Referential integrity
all_customer_ids = set(customers_df['customer_id'])
subs_invalid_customers = subscriptions_df[~subscriptions_df['customer_id'].isin(all_customer_ids)]
events_invalid_customers = events_df[~events_df['customer_id'].isin(all_customer_ids)]

validation_dict['subs_invalid_customers'] = subs_invalid_customers
validation_dict['events_invalid_customers'] = events_invalid_customers

# 5.4: Invalid prices or dates in subscriptions
invalid_prices = subscriptions_df[subscriptions_df['monthly_price'] <= 0]
invalid_dates = subscriptions_df[(subscriptions_df['end_date'].notnull()) & (subscriptions_df['start_date'] > subscriptions_df['end_date'])]

validation_dict['invalid_prices'] = invalid_prices
validation_dict['invalid_dates'] = invalid_dates

# 5.5: Multiple signups per customer
signup_events = events_df[events_df['event_type'] == 'signup']
multiple_signups = signup_events[signup_events.duplicated(subset=['customer_id'], keep=False)].groupby('customer_id').size().reset_index(name='count')

validation_dict['multiple_signups'] = multiple_signups

# 5.6: Event dates before signup_date
customers_signup = customers_df[['customer_id', 'signup_date']].dropna(subset=['signup_date'])
events_with_signup = events_df.merge(customers_signup, on='customer_id', how='left')
invalid_event_dates = events_with_signup[(events_with_signup['event_date'] < events_with_signup['signup_date']) & (events_with_signup['signup_date'].notnull())]

validation_dict['invalid_event_dates'] = invalid_event_dates

# 5.7: Active subscriptions with end_date
active_with_end = subscriptions_df[(subscriptions_df['status'] == 'active') & (subscriptions_df['end_date'].notnull())]

validation_dict['active_with_end'] = active_with_end

# 5.8: Canceled without end_date
canceled_without_end = subscriptions_df[(subscriptions_df['status'] == 'canceled') & (subscriptions_df['end_date'].isnull())]

validation_dict['canceled_without_end'] = canceled_without_end

# 5.9: Customers without subscriptions
customers_no_subs = customers_df[~customers_df['customer_id'].isin(subscriptions_df['customer_id'])]

validation_dict['customers_no_subs'] = customers_no_subs

# 5.10: Customers without events
customers_no_events = customers_df[~customers_df['customer_id'].isin(events_df['customer_id'])]

validation_dict['customers_no_events'] = customers_no_events

# 5.11: Subscriptions without events
subs_no_events = subscriptions_df[~subscriptions_df['customer_id'].isin(events_df['customer_id'])]

validation_dict['subs_no_events'] = subs_no_events

# 5.12: Future dates check
future_customers = customers_df[customers_df['signup_date'] > CURRENT_DATE]
future_subs_start = subscriptions_df[subscriptions_df['start_date'] > CURRENT_DATE]
future_subs_end = subscriptions_df[subscriptions_df['end_date'] > CURRENT_DATE]
future_events = events_df[events_df['event_date'] > CURRENT_DATE]

validation_dict['future_customers'] = future_customers
validation_dict['future_subs_start'] = future_subs_start
validation_dict['future_subs_end'] = future_subs_end
validation_dict['future_events'] = future_events

# 5.13: Event sequence check (simplified: check if trial_start before signup for each customer)
event_sequence_issues = []
for cust_id in events_df['customer_id'].unique():
    cust_events = events_df[events_df['customer_id'] == cust_id].sort_values('event_date')
    signup_date = cust_events[cust_events['event_type'] == 'signup']['event_date'].min()
    if pd.isna(signup_date):
        continue
    trial_date = cust_events[cust_events['event_type'] == 'trial_start']['event_date'].min()
    activated_date = cust_events[cust_events['event_type'] == 'activated']['event_date'].min()
    churned_date = cust_events[cust_events['event_type'] == 'churned']['event_date'].min()
    
    issues = []
    if not pd.isna(trial_date) and trial_date < signup_date:
        issues.append('trial_before_signup')
    if not pd.isna(activated_date) and activated_date < signup_date:
        issues.append('activated_before_signup')
    if not pd.isna(churned_date) and churned_date < signup_date:
        issues.append('churned_before_signup')
    
    if issues:
        event_sequence_issues.append({'customer_id': cust_id, 'issues': ', '.join(issues)})

validation_dict['event_sequence_issues'] = pd.DataFrame(event_sequence_issues)

# 5.14: Signup date mismatch between customers and events
signup_events = events_df[events_df['event_type'] == 'signup'][['customer_id', 'event_date']].rename(columns={'event_date': 'event_signup_date'})
signup_mismatch = customers_signup.merge(signup_events, on='customer_id', how='left')
signup_mismatch = signup_mismatch[signup_mismatch['signup_date'] != signup_mismatch['event_signup_date']]

validation_dict['signup_mismatch'] = signup_mismatch

# Step 6: Write all to Excel

with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
    # Exploration
    for sheet_name, df in exploration_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Missing
    for sheet_name, df in missing_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Duplicates
    for sheet_name, df in duplicates_dict.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Validations
    for sheet_name, df in validation_dict.items():
        if not df.empty or not df.empty:  # Write even if empty to show no issues
            df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"Validation results saved to {EXCEL_PATH}")

# Step 7: Data Loading to MySQL
# Update connection details as needed (host, user, password, database)
# Example: Local MySQL setup

from sqlalchemy import create_engine
import pandas as pd

# === CHANGE THESE 4 LINES ONLY ===
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'hardlygym123!'  # ← Put your real password here
DB_NAME = 'saas_analytics'


# Create connection string
engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

# Assuming your DataFrames are already created (customers_df, subscriptions_df, events_df)
try:
    customers_df.to_sql('customers', con=engine, if_exists='replace', index=False)
    print("Customers loaded successfully!")
except Exception as e:
    print(f"Customers error: {e}")

try:
    subscriptions_df.to_sql('subscriptions', con=engine, if_exists='replace', index=False)
    print("Subscriptions loaded successfully!")
except Exception as e:
    print(f"Subscriptions error: {e}")

try:
    events_df.to_sql('events', con=engine, if_exists='replace', index=False)
    print("Events loaded successfully!")
except Exception as e:
    print(f"Events error: {e}")

# Verify load (optional but very useful)
with engine.connect() as conn:
    print("Rows in customers:", pd.read_sql("SELECT COUNT(*) FROM customers", conn).iloc[0,0])
    print("Rows in subscriptions:", pd.read_sql("SELECT COUNT(*) FROM subscriptions", conn).iloc[0,0])
    print("Rows in events:", pd.read_sql("SELECT COUNT(*) FROM events", conn).iloc[0,0])
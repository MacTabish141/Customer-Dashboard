import pandas as pd
import sqlite3
from datetime import timedelta

# === Load CSVs & Clean ===
def clean_df(df):
    df = df.drop_duplicates()
    df = df.dropna(how='all')  # Drop rows where all values are NaN
    df = df.dropna(axis=1, how='all')  # Drop completely empty columns
    df = df.fillna('')  # Optional: fill remaining NaNs with empty strings
    return df

customers = clean_df(pd.read_csv('data/customers.csv'))
support_tickets = clean_df(pd.read_csv('data/support_tickets.csv'))
usage_logs = clean_df(pd.read_csv('data/usage_logs.csv'))
subscriptions = clean_df(pd.read_csv('data/subscriptions.csv'))
product_features = clean_df(pd.read_csv('data/product_features.csv'))

# === SCD Type II for Customers ===
customers['effective_start'] = pd.to_datetime(customers['signup_date'])
customers['effective_end'] = pd.NaT
customers['is_current'] = True

customers.loc[customers.sample(frac=0.05).index, 'status'] = 'Cancelled'
scd_customers = pd.concat([customers, customers.sample(frac=0.05)], ignore_index=True)
scd_customers['effective_start'] = pd.to_datetime(scd_customers['signup_date'])
scd_customers['effective_end'] = scd_customers['effective_start'] + pd.Timedelta(days=180)
scd_customers['is_current'] = scd_customers['effective_end'].isna()

# === SCD Type II for Subscriptions ===
subscriptions['start_date'] = pd.to_datetime(subscriptions['start_date'])
subscriptions['end_date'] = pd.to_datetime(subscriptions['end_date'])

subscriptions_scd2 = subscriptions.copy()
subscriptions_scd2['effective_start'] = subscriptions_scd2['start_date']
subscriptions_scd2['effective_end'] = pd.NaT
subscriptions_scd2['is_current'] = True

changed_subs = subscriptions_scd2.sample(frac=0.10).copy()
changed_subs['plan'] = changed_subs['plan'].apply(lambda x: 'Pro' if x == 'Basic' else 'Enterprise')
changed_subs['effective_start'] = changed_subs['start_date'] + timedelta(days=180)
changed_subs['effective_end'] = pd.NaT
changed_subs['is_current'] = True

subscriptions_scd2.update(
    subscriptions_scd2.loc[changed_subs.index].assign(
        is_current=False,
        effective_end=changed_subs['effective_start']
    )
)

dim_subscription = pd.concat([subscriptions_scd2, changed_subs], ignore_index=True)

# === Revenue Snapshot ===
month_range = pd.date_range(start='2024-04-01', end='2025-04-01', freq='MS')
plan_prices = {'Basic': 50, 'Pro': 100, 'Enterprise': 200}
snapshot_records = []

for month_start in month_range:
    for _, row in dim_subscription.iterrows():
        if (row['effective_start'] <= month_start and
            (pd.isna(row['effective_end']) or row['effective_end'] > month_start) and
            row['is_current']):
            
            revenue = plan_prices.get(row['plan'], 0)
            snapshot_records.append({
                'customer_id': row['customer_id'],
                'month': month_start.strftime('%Y-%m'),
                'plan': row['plan'],
                'status': row['status'],
                'monthly_revenue': revenue
            })

revenue_snapshot = pd.DataFrame(snapshot_records)

# === Monthly Usage Snapshot ===
usage_logs['log_date'] = pd.to_datetime(usage_logs['log_date'])
usage_logs['month'] = usage_logs['log_date'].dt.to_period('M').astype(str)
usage_snapshot = usage_logs.groupby(['customer_id', 'month']).agg({
    'duration_minutes': 'sum',
    'log_id': 'count'
}).reset_index().rename(columns={'log_id': 'activity_count'})

# === Load to SQLite ===
conn = sqlite3.connect('warehouse.db')

scd_customers.to_sql('dim_customer', conn, if_exists='replace', index=False)
support_tickets.to_sql('fact_support_ticket', conn, if_exists='replace', index=False)
usage_logs.to_sql('usage_logs', conn, if_exists='replace', index=False)
usage_snapshot.to_sql('fact_usage_snapshot', conn, if_exists='replace', index=False)
dim_subscription.to_sql('dim_subscription', conn, if_exists='replace', index=False)
product_features.to_sql('dim_product_feature', conn, if_exists='replace', index=False)
revenue_snapshot.to_sql('fact_revenue_snapshot', conn, if_exists='replace', index=False)

conn.commit()
conn.close()

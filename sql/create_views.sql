-- Drop existing views to avoid errors
DROP VIEW IF EXISTS weekly_active_users;
DROP VIEW IF EXISTS support_ticket_trends;
DROP VIEW IF EXISTS monthly_revenue;
DROP VIEW IF EXISTS feature_adoption;
DROP VIEW IF EXISTS monthly_revenue_summary;

-- ✅ Active Users by Week (raw data from usage_logs)
CREATE VIEW weekly_active_users AS
SELECT 
    strftime('%Y-%W', log_date) AS week_number,
    customer_id,
    COUNT(DISTINCT log_id) AS sessions
FROM usage_logs
GROUP BY week_number, customer_id;

-- ✅ Support Ticket Volume Trends
CREATE VIEW support_ticket_trends AS
SELECT 
    strftime('%Y-%m', created_at) AS month,
    issue_type,
    COUNT(*) AS ticket_count
FROM fact_support_ticket
GROUP BY month, issue_type;

-- ✅ Monthly Recurring Revenue (Estimation from Subscriptions)
CREATE VIEW monthly_revenue AS
SELECT 
    strftime('%Y-%m', start_date) AS month,
    plan,
    COUNT(*) AS subscribers,
    CASE 
        WHEN plan = 'Basic' THEN COUNT(*) * 50
        WHEN plan = 'Pro' THEN COUNT(*) * 100
        WHEN plan = 'Enterprise' THEN COUNT(*) * 200
        ELSE 0
    END AS estimated_revenue
FROM dim_subscription
WHERE status = 'Active'
GROUP BY month, plan;

-- ✅ Feature Adoption by Plan Type
CREATE VIEW feature_adoption AS
SELECT 
    s.plan,
    u.feature_used,
    COUNT(*) AS usage_count
FROM usage_logs u
JOIN dim_subscription s ON u.customer_id = s.customer_id
GROUP BY s.plan, u.feature_used;

-- ✅ Monthly Recurring Revenue (From fact_revenue_snapshot)
CREATE VIEW monthly_revenue_summary AS
SELECT
    month,
    SUM(monthly_revenue) AS total_mrr,
    COUNT(DISTINCT customer_id) AS active_customers
FROM fact_revenue_snapshot
GROUP BY month
ORDER BY month;

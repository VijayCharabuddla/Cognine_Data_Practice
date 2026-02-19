-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "3f089b42-61b1-4831-9f94-87f1655c9d07",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "3f089b42-61b1-4831-9f94-87f1655c9d07",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--tickets with highest operational effort
SELECT 
    f.ticket_id,
    COUNT(w.worklog_key) AS total_updates
FROM fact_ticket_worklog w
JOIN fact_ticket f
    ON w.ticket_key = f.ticket_key
WHERE f.is_deleted = 0
GROUP BY f.ticket_id
ORDER BY total_updates DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Average Updates Per Ticket By Priority
SELECT 
    f.priority,
    COUNT(w.worklog_key) * 1.0 / COUNT(DISTINCT f.ticket_key) AS avg_updates_per_ticket
FROM fact_ticket_worklog w
JOIN fact_ticket f
    ON w.ticket_key = f.ticket_key
WHERE f.ticket_status = 'CLOSED'
  AND f.is_deleted = 0
GROUP BY f.priority
ORDER BY avg_updates_per_ticket DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Support Group Workload Intensity
SELECT 
    sg.group_name,
    COUNT(w.worklog_key) AS total_updates,
    COUNT(DISTINCT f.ticket_key) AS total_tickets,
    COUNT(w.worklog_key) * 1.0 / COUNT(DISTINCT f.ticket_key) AS updates_per_ticket
FROM fact_ticket_worklog w
JOIN fact_ticket f
    ON w.ticket_key = f.ticket_key
JOIN dim_support_group sg
    ON f.support_group_key = sg.support_group_key
WHERE f.is_deleted = 0
GROUP BY sg.group_name
ORDER BY updates_per_ticket DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Monthly Worklog Activity Trend
SELECT 
    d.year,
    d.month,
    COUNT(w.worklog_key) AS total_updates
FROM fact_ticket_worklog w
JOIN dim_date d
    ON w.worklog_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--User-Level Productivity
SELECT 
    u.full_name,
    COUNT(w.worklog_key) AS total_updates
FROM fact_ticket_worklog w
JOIN dim_user u
    ON w.user_key = u.user_key
WHERE u.is_current = 1
GROUP BY u.full_name
ORDER BY total_updates DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--sla breach vs activity corelation
SELECT 
    f.sla_breach_flag,
    COUNT(w.worklog_key) * 1.0 / COUNT(DISTINCT f.ticket_key) AS avg_updates_per_ticket
FROM fact_ticket_worklog w
JOIN fact_ticket f
    ON w.ticket_key = f.ticket_key
WHERE f.ticket_status = 'CLOSED'
GROUP BY f.sla_breach_flag;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--average time between worklog updates
SELECT 
    f.ticket_id,
    COUNT(w.worklog_key) * 1.0 
        / COUNT(DISTINCT w.worklog_date_key) AS updates_per_active_day
FROM fact_ticket_worklog w
JOIN fact_ticket f
    ON w.ticket_key = f.ticket_key
WHERE w.is_deleted = 0
GROUP BY f.ticket_id
ORDER BY updates_per_active_day DESC;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--sla compliance percentage
SELECT
    COUNT(*) AS closed_tickets,
    SUM(CASE WHEN sla_breach_flag = 1 THEN 1 ELSE 0 END) AS breach_count,
    100.0 *
    (COUNT(*) - SUM(CASE WHEN sla_breach_flag = 1 THEN 1 ELSE 0 END))
    / COUNT(*) AS compliance_percentage
FROM fact_ticket
WHERE is_deleted = 0
  AND ticket_status = 'CLOSED';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--sla compliance by service
SELECT 
    s.service_name,
    COUNT(*) AS closed_tickets,
    SUM(CASE WHEN f.sla_breach_flag = 1 THEN 1 ELSE 0 END) AS breach_count,
    100.0 *
    (COUNT(*) - SUM(CASE WHEN f.sla_breach_flag = 1 THEN 1 ELSE 0 END))
    / COUNT(*) AS compliance_percentage
FROM fact_ticket f
JOIN dim_service s
    ON f.service_key = s.service_key
WHERE f.is_deleted = 0
  AND f.ticket_status = 'CLOSED'
GROUP BY s.service_name
ORDER BY breach_count DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--monthly sla trend
SELECT 
    d.year,
    d.month,
    COUNT(*) AS closed_tickets,
    SUM(CASE WHEN f.sla_breach_flag = 1 THEN 1 ELSE 0 END) AS breach_count,
    100.0 *
    (COUNT(*) - SUM(CASE WHEN f.sla_breach_flag = 1 THEN 1 ELSE 0 END))
    / COUNT(*) AS compliance_percentage
FROM fact_ticket f
JOIN dim_date d
    ON f.closed_date_key = d.date_key
WHERE f.is_deleted = 0
  AND f.ticket_status = 'CLOSED'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--Resolution Time by Priority
SELECT 
    priority,
    COUNT(*) AS closed_tickets,
    AVG(resolution_minutes) AS avg_resolution_minutes
FROM fact_ticket
WHERE is_deleted = 0
  AND ticket_status = 'CLOSED'
GROUP BY priority
ORDER BY avg_resolution_minutes;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

--support group performance
SELECT TOP 10
    sg.group_name,
    COUNT(*) AS closed_tickets
FROM fact_ticket f
JOIN dim_support_group sg
    ON f.support_group_key = sg.support_group_key
WHERE f.is_deleted = 0
  AND f.ticket_status = 'CLOSED'
GROUP BY sg.group_name
ORDER BY closed_tickets DESC;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

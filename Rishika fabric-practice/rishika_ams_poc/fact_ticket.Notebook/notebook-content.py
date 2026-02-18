# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ac491e8-04d6-45d2-8319-8db2f709c0e7",
# META       "default_lakehouse_name": "ticket_data",
# META       "default_lakehouse_workspace_id": "4f3a671f-b3bc-4df2-abed-b625a3e18722",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ac491e8-04d6-45d2-8319-8db2f709c0e7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW fact_ticket_src AS
# MAGIC SELECT *
# MAGIC FROM silver_tickets
# MAGIC WHERE load_timestamp > (
# MAGIC     SELECT max_watermark
# MAGIC     FROM load_control
# MAGIC     WHERE table_name = 'fact_ticket'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO fact_ticket tgt
# MAGIC USING fact_ticket_src src
# MAGIC ON tgt.ticket_id = src.ticket_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.ticket_type = src.ticket_type,
# MAGIC       tgt.ticket_status = src.ticket_status,
# MAGIC       tgt.priority = src.priority,
# MAGIC       tgt.impact = src.impact,
# MAGIC       tgt.urgency = src.urgency,
# MAGIC       tgt.location = src.location,
# MAGIC       tgt.resolution_minutes = src.resolution_minutes,
# MAGIC       tgt.sla_target_minutes = src.sla_target_minutes,
# MAGIC       tgt.sla_breach_flag = src.sla_breach_flag,
# MAGIC       tgt.invalid_date_flag = src.invalid_date_flag,
# MAGIC       tgt.is_deleted = CASE WHEN src.is_deleted = 'Y' THEN true ELSE false END,
# MAGIC       tgt.opened_date_key = CAST(date_format(src.opened_ts, 'yyyyMMdd') AS INT),
# MAGIC       tgt.closed_date_key = CAST(date_format(src.closed_ts, 'yyyyMMdd') AS INT),
# MAGIC       tgt.updated_ts = current_timestamp();


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC WITH new_rows AS (
# MAGIC     SELECT src.*
# MAGIC     FROM fact_ticket_src src
# MAGIC     LEFT JOIN fact_ticket tgt
# MAGIC         ON tgt.ticket_id = src.ticket_id
# MAGIC     WHERE tgt.ticket_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(ticket_key), 0) AS max_key
# MAGIC     FROM fact_ticket
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY ticket_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO fact_ticket
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn,
# MAGIC     n.ticket_id,
# MAGIC 
# MAGIC     COALESCE(u.user_key, -1),
# MAGIC     s.service_key,
# MAGIC     g.support_group_key,
# MAGIC     c.category_key,
# MAGIC 
# MAGIC     CAST(date_format(n.opened_ts, 'yyyyMMdd') AS INT),
# MAGIC     CAST(date_format(n.closed_ts, 'yyyyMMdd') AS INT),
# MAGIC 
# MAGIC     n.ticket_type,
# MAGIC     n.ticket_status,
# MAGIC     n.priority,
# MAGIC     n.impact,
# MAGIC     n.urgency,
# MAGIC     n.location,
# MAGIC 
# MAGIC     n.resolution_minutes,
# MAGIC     n.sla_target_minutes,
# MAGIC     n.sla_breach_flag,
# MAGIC     n.invalid_date_flag,
# MAGIC 
# MAGIC     CASE WHEN n.is_deleted = 'Y' THEN true ELSE false END,
# MAGIC     current_timestamp()
# MAGIC 
# MAGIC FROM numbered n
# MAGIC CROSS JOIN key_base kb
# MAGIC 
# MAGIC LEFT JOIN dim_user u
# MAGIC     ON n.requester_id = u.user_id
# MAGIC     AND u.is_current = true
# MAGIC 
# MAGIC 
# MAGIC LEFT JOIN dim_service s
# MAGIC     ON n.service_id = s.service_id
# MAGIC 
# MAGIC LEFT JOIN dim_support_group g
# MAGIC     ON n.assigned_group_id = g.group_id
# MAGIC 
# MAGIC LEFT JOIN dim_category c
# MAGIC     ON n.category_id = c.category_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE load_control
# MAGIC SET max_watermark = (
# MAGIC     SELECT MAX(load_timestamp) FROM silver_tickets
# MAGIC ),
# MAGIC last_run_ts = current_timestamp()
# MAGIC WHERE table_name = 'fact_ticket';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

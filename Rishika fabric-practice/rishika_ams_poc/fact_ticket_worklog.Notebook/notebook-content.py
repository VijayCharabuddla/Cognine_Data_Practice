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
# MAGIC CREATE OR REPLACE TEMP VIEW fact_worklog_src AS
# MAGIC SELECT *
# MAGIC FROM silver_ticket_worklogs
# MAGIC WHERE load_timestamp > (
# MAGIC     SELECT max_watermark
# MAGIC     FROM load_control
# MAGIC     WHERE table_name = 'fact_ticket_worklog'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO fact_ticket_worklog tgt
# MAGIC USING fact_worklog_src src
# MAGIC ON tgt.worklog_id = src.worklog_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.update_type = src.update_type,
# MAGIC       tgt.worklog_comment = src.worklog_comment,
# MAGIC       tgt.invalid_timestamp_flag = src.invalid_timestamp_flag,
# MAGIC       tgt.worklog_date_key = CAST(date_format(src.update_ts, 'yyyyMMdd') AS INT),
# MAGIC       tgt.is_deleted = false,
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
# MAGIC     FROM fact_worklog_src src
# MAGIC     LEFT JOIN fact_ticket_worklog tgt
# MAGIC         ON tgt.worklog_id = src.worklog_id
# MAGIC     WHERE tgt.worklog_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(worklog_key), 0) AS max_key
# MAGIC     FROM fact_ticket_worklog
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY worklog_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO fact_ticket_worklog
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn,
# MAGIC     n.worklog_id,
# MAGIC 
# MAGIC     ft.ticket_key,
# MAGIC     COALESCE(u.user_key, -1),
# MAGIC 
# MAGIC     CAST(date_format(n.update_ts, 'yyyyMMdd') AS INT),
# MAGIC 
# MAGIC     n.update_type,
# MAGIC     n.worklog_comment,
# MAGIC     n.invalid_timestamp_flag,
# MAGIC 
# MAGIC     false,
# MAGIC     current_timestamp()
# MAGIC 
# MAGIC FROM numbered n
# MAGIC CROSS JOIN key_base kb
# MAGIC 
# MAGIC LEFT JOIN fact_ticket ft
# MAGIC     ON n.ticket_id = ft.ticket_id
# MAGIC 
# MAGIC LEFT JOIN dim_user u
# MAGIC     ON n.updated_by = u.user_id
# MAGIC     AND u.is_current = true;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE load_control
# MAGIC SET max_watermark = (
# MAGIC     SELECT MAX(load_timestamp) FROM silver_ticket_worklogs
# MAGIC ),
# MAGIC last_run_ts = current_timestamp()
# MAGIC WHERE table_name = 'fact_ticket_worklog';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

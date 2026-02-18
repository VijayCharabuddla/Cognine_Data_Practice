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
# MAGIC --incremental source view
# MAGIC CREATE OR REPLACE TEMP VIEW dim_support_group_src AS
# MAGIC SELECT
# MAGIC     s.group_id,
# MAGIC     s.group_name,
# MAGIC     s.location,
# MAGIC     CASE WHEN s.active_flag = 'Y' THEN true ELSE false END AS active_flag,
# MAGIC     CASE WHEN s.is_deleted = 'Y' THEN true ELSE false END AS is_deleted,
# MAGIC     s.load_timestamp
# MAGIC FROM silver_supportgroups s
# MAGIC WHERE s.load_timestamp > (
# MAGIC     SELECT max_watermark
# MAGIC     FROM load_control
# MAGIC     WHERE table_name = 'dim_support_group'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_support_group tgt
# MAGIC USING dim_support_group_src src
# MAGIC ON tgt.group_id = src.group_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.group_name = src.group_name,
# MAGIC       tgt.location = src.location,
# MAGIC       tgt.active_flag = src.active_flag,
# MAGIC       tgt.is_deleted = src.is_deleted,
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
# MAGIC     FROM dim_support_group_src src
# MAGIC     LEFT JOIN dim_support_group tgt
# MAGIC         ON tgt.group_id = src.group_id
# MAGIC     WHERE tgt.group_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(support_group_key), 0) AS max_key
# MAGIC     FROM dim_support_group
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY group_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_support_group (
# MAGIC     support_group_key,
# MAGIC     group_id,
# MAGIC     group_name,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     updated_ts
# MAGIC )
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn,
# MAGIC     n.group_id,
# MAGIC     n.group_name,
# MAGIC     n.location,
# MAGIC     n.active_flag,
# MAGIC     n.is_deleted,
# MAGIC     current_timestamp()
# MAGIC FROM numbered n
# MAGIC CROSS JOIN key_base kb;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE load_control
# MAGIC SET max_watermark = (
# MAGIC     SELECT MAX(load_timestamp) FROM silver_supportgroups
# MAGIC ),
# MAGIC last_run_ts = current_timestamp()
# MAGIC WHERE table_name = 'dim_support_group';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

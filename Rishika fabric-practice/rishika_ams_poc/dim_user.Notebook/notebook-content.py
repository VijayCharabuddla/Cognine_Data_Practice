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
# MAGIC 
# MAGIC --building source(incremental+hash)
# MAGIC CREATE OR REPLACE TEMP VIEW dim_user_src AS
# MAGIC SELECT
# MAGIC     s.user_id,
# MAGIC     s.user_type,
# MAGIC     s.first_name,
# MAGIC     s.last_name,
# MAGIC     s.full_name,
# MAGIC     s.email,
# MAGIC     s.location,
# MAGIC     CASE WHEN s.active_flag = 'Y' THEN true ELSE false END AS active_flag,
# MAGIC     CASE WHEN s.is_deleted = 'Y' THEN true ELSE false END AS is_deleted,
# MAGIC     s.created_ts,
# MAGIC     s.load_timestamp,
# MAGIC     sha2(concat_ws('|',
# MAGIC         coalesce(s.user_type,''),
# MAGIC         coalesce(s.first_name,''),
# MAGIC         coalesce(s.last_name,''),
# MAGIC         coalesce(s.full_name,''),
# MAGIC         coalesce(s.email,''),
# MAGIC         coalesce(s.location,''),
# MAGIC         cast(CASE WHEN s.active_flag = 'Y' THEN true ELSE false END as string),
# MAGIC         cast(CASE WHEN s.is_deleted = 'Y' THEN true ELSE false END as string)
# MAGIC     ), 256) AS hash_value
# MAGIC FROM silver_user s
# MAGIC WHERE s.load_timestamp > (
# MAGIC     SELECT max_watermark
# MAGIC     FROM load_control
# MAGIC     WHERE table_name = 'dim_user'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --expire changed records 
# MAGIC MERGE INTO dim_user tgt
# MAGIC USING dim_user_src src
# MAGIC ON tgt.user_id = src.user_id
# MAGIC AND tgt.is_current = true
# MAGIC 
# MAGIC WHEN MATCHED AND tgt.hash_value <> src.hash_value THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.valid_to = src.load_timestamp,
# MAGIC       tgt.is_current = false,
# MAGIC       tgt.updated_ts = current_timestamp();
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --insert new and updated versions
# MAGIC -- insert new and updated versions
# MAGIC WITH new_rows AS (
# MAGIC     SELECT src.*
# MAGIC     FROM dim_user_src src
# MAGIC     LEFT JOIN dim_user tgt
# MAGIC         ON tgt.user_id = src.user_id
# MAGIC         AND tgt.is_current = true
# MAGIC     WHERE tgt.user_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(user_key), 0) AS max_key
# MAGIC     FROM dim_user
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY user_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_user (
# MAGIC     user_key,
# MAGIC     user_id,
# MAGIC     user_type,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     full_name,
# MAGIC     email,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     valid_from,
# MAGIC     valid_to,
# MAGIC     is_current,
# MAGIC     hash_value,
# MAGIC     created_ts,
# MAGIC     updated_ts
# MAGIC )
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn AS user_key,
# MAGIC     n.user_id,
# MAGIC     n.user_type,
# MAGIC     n.first_name,
# MAGIC     n.last_name,
# MAGIC     n.full_name,
# MAGIC     n.email,
# MAGIC     n.location,
# MAGIC     n.active_flag,
# MAGIC     n.is_deleted,
# MAGIC     n.load_timestamp,
# MAGIC     TIMESTAMP '9999-12-31',
# MAGIC     true,
# MAGIC     n.hash_value,
# MAGIC     n.created_ts,
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
# MAGIC     SELECT max(load_timestamp) FROM silver_user
# MAGIC ),
# MAGIC last_run_ts = current_timestamp()
# MAGIC WHERE table_name = 'dim_user';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

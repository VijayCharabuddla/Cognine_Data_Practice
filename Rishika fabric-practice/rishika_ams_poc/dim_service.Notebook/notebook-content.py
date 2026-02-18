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
# MAGIC # Welcome to your new notebook
# MAGIC # Type here in the cell editor to add code!
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW dim_service_src AS
# MAGIC SELECT
# MAGIC     s.service_id,
# MAGIC     s.service_name,
# MAGIC     s.service_owner_id,
# MAGIC     s.criticality,
# MAGIC     CASE WHEN s.active_flag = 'Y' THEN true ELSE false END AS active_flag,
# MAGIC     CASE WHEN s.is_deleted = 'Y' THEN true ELSE false END AS is_deleted,
# MAGIC     s.load_timestamp
# MAGIC FROM silver_services s
# MAGIC WHERE s.load_timestamp > (
# MAGIC     SELECT max_watermark
# MAGIC     FROM load_control
# MAGIC     WHERE table_name = 'dim_service'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_service tgt
# MAGIC USING dim_service_src src
# MAGIC ON tgt.service_id = src.service_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.service_name = src.service_name,
# MAGIC       tgt.service_owner_id = src.service_owner_id,
# MAGIC       tgt.criticality = src.criticality,
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
# MAGIC     FROM dim_service_src src
# MAGIC     LEFT JOIN dim_service tgt
# MAGIC         ON tgt.service_id = src.service_id
# MAGIC     WHERE tgt.service_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(service_key), 0) AS max_key
# MAGIC     FROM dim_service
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY service_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_service (
# MAGIC     service_key,
# MAGIC     service_id,
# MAGIC     service_name,
# MAGIC     service_owner_id,
# MAGIC     criticality,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     updated_ts
# MAGIC )
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn,
# MAGIC     n.service_id,
# MAGIC     n.service_name,
# MAGIC     n.service_owner_id,
# MAGIC     n.criticality,
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
# MAGIC     SELECT MAX(load_timestamp) FROM silver_services
# MAGIC ),
# MAGIC last_run_ts = current_timestamp()
# MAGIC WHERE table_name = 'dim_service';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

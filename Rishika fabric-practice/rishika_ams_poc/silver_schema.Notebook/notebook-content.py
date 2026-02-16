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
# MAGIC CREATE TABLE IF NOT EXISTS silver_ticket (
# MAGIC     ticket_id STRING,
# MAGIC     ticket_type STRING,
# MAGIC 
# MAGIC     opened_ts TIMESTAMP,
# MAGIC     closed_ts TIMESTAMP,
# MAGIC     record_created_ts TIMESTAMP,
# MAGIC     record_updated_ts TIMESTAMP,
# MAGIC 
# MAGIC     status_code STRING,
# MAGIC     status_label STRING,
# MAGIC 
# MAGIC     priority_code STRING,
# MAGIC     priority_label STRING,
# MAGIC 
# MAGIC     impact_code STRING,
# MAGIC     urgency_code STRING,
# MAGIC 
# MAGIC     requester_id STRING,
# MAGIC     assigned_group_id STRING,
# MAGIC     service_id STRING,
# MAGIC     category_id STRING,
# MAGIC     sla_id STRING,
# MAGIC 
# MAGIC     resolution_minutes INT,
# MAGIC 
# MAGIC     status_unknown_flag BOOLEAN,
# MAGIC     priority_unknown_flag BOOLEAN,
# MAGIC     invalid_date_flag BOOLEAN,
# MAGIC     negative_resolution_flag BOOLEAN,
# MAGIC 
# MAGIC     load_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_user (
# MAGIC     user_id STRING,
# MAGIC 
# MAGIC     user_type STRING,
# MAGIC 
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     full_name STRING,
# MAGIC 
# MAGIC     email STRING,
# MAGIC     location STRING,
# MAGIC 
# MAGIC     active_flag STRING,
# MAGIC     is_deleted STRING,
# MAGIC 
# MAGIC     created_ts TIMESTAMP,
# MAGIC 
# MAGIC     load_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table silver_user

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

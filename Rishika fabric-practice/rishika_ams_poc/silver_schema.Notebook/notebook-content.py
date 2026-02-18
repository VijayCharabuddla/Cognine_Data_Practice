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
# MAGIC CREATE TABLE silver_user (
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
# MAGIC     ingestion_ts TIMESTAMP,      -- from Bronze
# MAGIC     load_timestamp TIMESTAMP     -- when Silver processed
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC %%sql
# MAGIC --create support groups
# MAGIC create table if not exists silver_supportgroups (
# MAGIC     group_id string,
# MAGIC 
# MAGIC     group_name string,
# MAGIC     location string,
# MAGIC 
# MAGIC     active_flag string,
# MAGIC     ingestion_ts timestamp,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC %%sql
# MAGIC --table creation
# MAGIC create table if not exists silver_services (
# MAGIC     service_id string,
# MAGIC 
# MAGIC     service_name string,
# MAGIC     service_owner_id string,
# MAGIC 
# MAGIC     criticality string,
# MAGIC     active_flag string,
# MAGIC 
# MAGIC     ingestion_ts timestamp,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --create
# MAGIC create table if not exists silver_sla_definitions (
# MAGIC     sla_id string,
# MAGIC 
# MAGIC     priority string,
# MAGIC     response_minutes int,
# MAGIC     resolution_minutes int,
# MAGIC 
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists silver_categories (
# MAGIC     category_id string,
# MAGIC     category_name string,
# MAGIC     subcategory_name string,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists silver_servicecategorymap (
# MAGIC     service_id string,
# MAGIC     category_id string,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists silver_servicegroupmap (
# MAGIC     service_id string,
# MAGIC     group_id string,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table if not exists silver_tickets (
# MAGIC     ticket_id string,
# MAGIC 
# MAGIC     ticket_type string,
# MAGIC 
# MAGIC     opened_ts timestamp,
# MAGIC     closed_ts timestamp,
# MAGIC 
# MAGIC     ticket_status string,
# MAGIC 
# MAGIC     requester_id string,
# MAGIC     assigned_group_id string,
# MAGIC     service_id string,
# MAGIC     category_id string,
# MAGIC 
# MAGIC     priority string,
# MAGIC     impact string,
# MAGIC     urgency string,
# MAGIC 
# MAGIC     sla_id string,
# MAGIC 
# MAGIC     location string,
# MAGIC     short_description string,
# MAGIC     description string,
# MAGIC 
# MAGIC     record_created_ts timestamp,
# MAGIC     record_updated_ts timestamp,
# MAGIC 
# MAGIC     resolution_minutes int,
# MAGIC     sla_target_minutes int,
# MAGIC     sla_breach_flag boolean,
# MAGIC 
# MAGIC     invalid_date_flag boolean,
# MAGIC 
# MAGIC     ingestion_ts timestamp,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --create
# MAGIC create table if not exists silver_ticket_worklogs (
# MAGIC     worklog_id string,
# MAGIC     ticket_id string,
# MAGIC 
# MAGIC     updated_by string,
# MAGIC     update_type string,
# MAGIC 
# MAGIC     update_ts timestamp,
# MAGIC 
# MAGIC     worklog_comment string,
# MAGIC 
# MAGIC     invalid_timestamp_flag boolean,
# MAGIC 
# MAGIC     ingestion_ts timestamp,
# MAGIC     load_timestamp timestamp
# MAGIC )
# MAGIC using delta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

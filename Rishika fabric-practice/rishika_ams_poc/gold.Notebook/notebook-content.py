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

# MARKDOWN ********************

# **<h1>Dim_User( SCD TYPE 2)</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS dim_user;
# MAGIC DROP TABLE IF EXISTS load_control;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table dim_user;
# MAGIC truncate table load_control;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE load_control (
# MAGIC     table_name STRING,
# MAGIC     max_watermark TIMESTAMP,
# MAGIC     last_run_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --creating dim_user
# MAGIC CREATE TABLE dim_user (
# MAGIC     user_key BIGINT,
# MAGIC     user_id STRING,
# MAGIC     user_type STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     full_name STRING,
# MAGIC     email STRING,
# MAGIC     location STRING,
# MAGIC     active_flag BOOLEAN,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current BOOLEAN,
# MAGIC     hash_value STRING,
# MAGIC     created_ts TIMESTAMP,      -- source creation time
# MAGIC     updated_ts TIMESTAMP       -- warehouse update time
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
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
# MAGIC     -1,
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     'UNKNOWN',
# MAGIC     false,
# MAGIC     false,
# MAGIC     TIMESTAMP '1900-01-01',
# MAGIC     TIMESTAMP '9999-12-31',
# MAGIC     true,
# MAGIC     'UNKNOWN',
# MAGIC     TIMESTAMP '1900-01-01',
# MAGIC     current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM dim_user WHERE user_key = -1
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO load_control
# MAGIC VALUES (
# MAGIC     'dim_user',
# MAGIC     TIMESTAMP '1900-01-01',
# MAGIC     current_timestamp()
# MAGIC );
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --simulated change
# MAGIC UPDATE silver_user
# MAGIC SET email = 'scd_test_email@test.com',
# MAGIC     load_timestamp = current_timestamp()
# MAGIC WHERE user_id = 'USR00005';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --revert silver
# MAGIC UPDATE silver_user
# MAGIC SET email = 'amanda59@example.org',
# MAGIC     load_timestamp = current_timestamp()
# MAGIC WHERE user_id = 'USR00005';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Dim_Service (SCD Type 1)</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC --create
# MAGIC DROP TABLE IF EXISTS dim_service;
# MAGIC 
# MAGIC CREATE TABLE dim_service (
# MAGIC     service_key BIGINT,
# MAGIC     service_id STRING,
# MAGIC     service_name STRING,
# MAGIC     service_owner_id STRING,
# MAGIC     criticality STRING,
# MAGIC     active_flag BOOLEAN,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO load_control
# MAGIC SELECT 'dim_service', TIMESTAMP '1900-01-01', current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM load_control WHERE table_name = 'dim_service'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Dim_Support_Group (SCD Type 1)</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC --table creation
# MAGIC DROP TABLE IF EXISTS dim_support_group;
# MAGIC 
# MAGIC CREATE TABLE dim_support_group (
# MAGIC     support_group_key BIGINT,
# MAGIC     group_id STRING,
# MAGIC     group_name STRING,
# MAGIC     location STRING,
# MAGIC     active_flag BOOLEAN,
# MAGIC     is_deleted BOOLEAN,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO load_control
# MAGIC SELECT 'dim_support_group', TIMESTAMP '1900-01-01', current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 
# MAGIC     FROM load_control 
# MAGIC     WHERE table_name = 'dim_support_group'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Dim_Category (SCD Type 1)</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS dim_category;
# MAGIC 
# MAGIC CREATE TABLE dim_category (
# MAGIC     category_key BIGINT,
# MAGIC     category_id STRING,
# MAGIC     category_name STRING,
# MAGIC     subcategory_name STRING,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Dim_date</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     MIN(min_date) AS overall_min_date,
# MAGIC     MAX(max_date) AS overall_max_date
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         MIN(opened_ts) AS min_date,
# MAGIC         MAX(opened_ts) AS max_date
# MAGIC     FROM silver_tickets
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC         MIN(closed_ts),
# MAGIC         MAX(closed_ts)
# MAGIC     FROM silver_tickets
# MAGIC     
# MAGIC     UNION ALL
# MAGIC     
# MAGIC     SELECT 
# MAGIC         MIN(update_ts),
# MAGIC         MAX(update_ts)
# MAGIC     FROM silver_ticket_worklogs
# MAGIC ) t;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS dim_date;
# MAGIC 
# MAGIC CREATE TABLE dim_date (
# MAGIC     date_key INT,
# MAGIC     full_date DATE,
# MAGIC     day INT,
# MAGIC     month INT,
# MAGIC     year INT,
# MAGIC     quarter INT
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC WITH dates AS (
# MAGIC     SELECT explode(
# MAGIC         sequence(
# MAGIC             to_date('2023-01-01'),
# MAGIC             to_date('2036-12-31'),
# MAGIC             interval 1 day
# MAGIC         )
# MAGIC     ) AS full_date
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_date
# MAGIC SELECT
# MAGIC     CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC     full_date,
# MAGIC     day(full_date) AS day,
# MAGIC     month(full_date) AS month,
# MAGIC     year(full_date) AS year,
# MAGIC     quarter(full_date) AS quarter
# MAGIC FROM dates;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT MIN(full_date), MAX(full_date)
# MAGIC FROM dim_date;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Fact Ticket</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS fact_ticket;
# MAGIC 
# MAGIC CREATE TABLE fact_ticket (
# MAGIC     ticket_key BIGINT,
# MAGIC     ticket_id STRING,
# MAGIC 
# MAGIC     requester_key BIGINT,
# MAGIC     service_key BIGINT,
# MAGIC     support_group_key BIGINT,
# MAGIC     category_key BIGINT,
# MAGIC 
# MAGIC     opened_date_key INT,
# MAGIC     closed_date_key INT,
# MAGIC 
# MAGIC     ticket_type STRING,
# MAGIC     ticket_status STRING,
# MAGIC     priority STRING,
# MAGIC     impact STRING,
# MAGIC     urgency STRING,
# MAGIC     location STRING,
# MAGIC 
# MAGIC     resolution_minutes INT,
# MAGIC     sla_target_minutes INT,
# MAGIC     sla_breach_flag BOOLEAN,
# MAGIC     invalid_date_flag BOOLEAN,
# MAGIC 
# MAGIC     is_deleted BOOLEAN,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO load_control
# MAGIC SELECT 'fact_ticket', TIMESTAMP '1900-01-01', current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM load_control WHERE table_name = 'fact_ticket'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Fact ticket_worklog</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS fact_ticket_worklog;
# MAGIC 
# MAGIC CREATE TABLE fact_ticket_worklog (
# MAGIC     worklog_key BIGINT,
# MAGIC     worklog_id STRING,
# MAGIC 
# MAGIC     ticket_key BIGINT,
# MAGIC     user_key BIGINT,
# MAGIC 
# MAGIC     worklog_date_key INT,
# MAGIC 
# MAGIC     update_type STRING,
# MAGIC     worklog_comment STRING,
# MAGIC     invalid_timestamp_flag BOOLEAN,
# MAGIC 
# MAGIC     is_deleted BOOLEAN,
# MAGIC     updated_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO load_control
# MAGIC SELECT 'fact_ticket_worklog', TIMESTAMP '1900-01-01', current_timestamp()
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM load_control WHERE table_name = 'fact_ticket_worklog'
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

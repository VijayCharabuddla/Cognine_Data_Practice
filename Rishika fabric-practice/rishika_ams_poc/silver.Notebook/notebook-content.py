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

# **<h1>TICKETS</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS silver_user;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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


%%sql
--ticket incremental source
CREATE OR REPLACE TEMP VIEW incremental_user AS
SELECT *
FROM bronze_Users_Stg
WHERE ingestion_ts > (
    SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1900-01-01'))
    FROM silver_user
);


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

# MAGIC %%sql
# MAGIC --merge tickets
# MAGIC MERGE INTO silver_ticket AS target
# MAGIC USING transformed_incremental_ticket AS source
# MAGIC ON target.ticket_id = source.ticket_id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.ticket_type = source.ticket_type,
# MAGIC     target.opened_ts = source.opened_ts,
# MAGIC     target.closed_ts = source.closed_ts,
# MAGIC     target.record_created_ts = source.record_created_ts,
# MAGIC     target.record_updated_ts = source.record_updated_ts,
# MAGIC     target.status_code = source.status_code,
# MAGIC     target.status_label = source.status_label,
# MAGIC     target.priority_code = source.priority_code,
# MAGIC     target.priority_label = source.priority_label,
# MAGIC     target.impact_code = source.impact_code,
# MAGIC     target.urgency_code = source.urgency_code,
# MAGIC     target.requester_id = source.requester_id,
# MAGIC     target.assigned_group_id = source.assigned_group_id,
# MAGIC     target.service_id = source.service_id,
# MAGIC     target.category_id = source.category_id,
# MAGIC     target.sla_id = source.sla_id,
# MAGIC     target.resolution_minutes = source.resolution_minutes,
# MAGIC     target.status_unknown_flag = source.status_unknown_flag,
# MAGIC     target.priority_unknown_flag = source.priority_unknown_flag,
# MAGIC     target.invalid_date_flag = source.invalid_date_flag,
# MAGIC     target.negative_resolution_flag = source.negative_resolution_flag,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT *;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>USERS</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC describe silver_user;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_user AS
# MAGIC SELECT *
# MAGIC FROM bronze_Users_Stg
# MAGIC WHERE ingestion_ts > (
# MAGIC     SELECT COALESCE(MAX(load_timestamp), TIMESTAMP('1900-01-01'))
# MAGIC     FROM silver_user
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --transform
# MAGIC CREATE OR REPLACE TEMP VIEW transformed_user AS
# MAGIC SELECT
# MAGIC     user_id,
# MAGIC 
# MAGIC     upper(trim(user_type)) AS user_type,
# MAGIC 
# MAGIC     initcap(trim(first_name)) AS first_name,
# MAGIC     initcap(trim(last_name)) AS last_name,
# MAGIC 
# MAGIC     concat_ws(' ',
# MAGIC         initcap(trim(first_name)),
# MAGIC         initcap(trim(last_name))
# MAGIC     ) AS full_name,
# MAGIC 
# MAGIC     lower(trim(email)) AS email,
# MAGIC 
# MAGIC     initcap(trim(location)) AS location,
# MAGIC 
# MAGIC     upper(trim(active_flag)) AS active_flag,
# MAGIC     upper(trim(is_deleted)) AS is_deleted,
# MAGIC 
# MAGIC     CAST(created_date AS TIMESTAMP) AS created_ts,
# MAGIC 
# MAGIC     ingestion_ts,
# MAGIC 
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC 
# MAGIC FROM incremental_user
# MAGIC WHERE user_id IS NOT NULL;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge
# MAGIC MERGE INTO silver_user AS target
# MAGIC USING transformed_user AS source
# MAGIC ON target.user_id = source.user_id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.user_type = source.user_type,
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.full_name = source.full_name,
# MAGIC     target.email = source.email,
# MAGIC     target.location = source.location,
# MAGIC     target.active_flag = source.active_flag,
# MAGIC     target.is_deleted = source.is_deleted,
# MAGIC     target.created_ts = source.created_ts,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SUPPORT GROUPS</h1>**

# CELL ********************

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
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC describe silver_supportgroups

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --incremental extraction
# MAGIC create or replace temp view incremental_supportgroups as
# MAGIC select *
# MAGIC from bronze_supportgroups_stg
# MAGIC where ingestion_ts > (
# MAGIC     select coalesce(max(ingestion_ts), timestamp('1900-01-01'))
# MAGIC     from silver_supportgroups
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --transformation
# MAGIC create or replace temp view transformed_supportgroups as
# MAGIC select
# MAGIC     group_id,
# MAGIC     initcap(trim(group_name)) as group_name,
# MAGIC     initcap(trim(location)) as location,
# MAGIC     upper(trim(active_flag)) as active_flag,
# MAGIC     ingestion_ts,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from incremental_supportgroups
# MAGIC where group_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge
# MAGIC merge into silver_supportgroups as target
# MAGIC using transformed_supportgroups as source
# MAGIC on target.group_id = source.group_id
# MAGIC 
# MAGIC when matched then update set
# MAGIC     target.group_name = source.group_name,
# MAGIC     target.location = source.location,
# MAGIC     target.active_flag = source.active_flag,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC when not matched then insert *;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SERVICES</h1>**

# CELL ********************

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
# MAGIC --incremental extract
# MAGIC create or replace temp view incremental_services as
# MAGIC select *
# MAGIC from bronze_services_stg
# MAGIC where ingestion_ts > (
# MAGIC     select coalesce(max(ingestion_ts), timestamp('1900-01-01'))
# MAGIC     from silver_services
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --transform
# MAGIC create or replace temp view transformed_services as
# MAGIC select
# MAGIC     service_id,
# MAGIC 
# MAGIC     initcap(trim(service_name)) as service_name,
# MAGIC 
# MAGIC     case
# MAGIC         when service_owner_id is null or trim(service_owner_id) = '' then null
# MAGIC         else trim(service_owner_id)
# MAGIC     end as service_owner_id,
# MAGIC 
# MAGIC     case
# MAGIC         when lower(trim(criticality)) in ('high') then 'high'
# MAGIC         when lower(trim(criticality)) in ('medium') then 'medium'
# MAGIC         when lower(trim(criticality)) in ('low') then 'low'
# MAGIC         else null
# MAGIC     end as criticality,
# MAGIC 
# MAGIC     case
# MAGIC         when upper(trim(active_flag)) = 'Y' then 'Y'
# MAGIC         when upper(trim(active_flag)) = 'N' then 'N'
# MAGIC         else null
# MAGIC     end as active_flag,
# MAGIC 
# MAGIC     ingestion_ts,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from incremental_services
# MAGIC where service_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge
# MAGIC merge into silver_services as target
# MAGIC using transformed_services as source
# MAGIC on target.service_id = source.service_id
# MAGIC 
# MAGIC when matched then update set
# MAGIC     target.service_name = source.service_name,
# MAGIC     target.service_owner_id = source.service_owner_id,
# MAGIC     target.criticality = source.criticality,
# MAGIC     target.active_flag = source.active_flag,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC when not matched then insert *;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SLA DEFINITIONS</h1>**

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
# MAGIC --transform
# MAGIC create or replace temp view transformed_sla_definitions as
# MAGIC select
# MAGIC     sla_id,
# MAGIC 
# MAGIC     upper(trim(priority)) as priority,
# MAGIC 
# MAGIC     case
# MAGIC         when response_minutes rlike '^[0-9]+$'
# MAGIC         then cast(response_minutes as int)
# MAGIC         else null
# MAGIC     end as response_minutes,
# MAGIC 
# MAGIC     case
# MAGIC         when resolution_minutes rlike '^[0-9]+$'
# MAGIC         then cast(resolution_minutes as int)
# MAGIC         else null
# MAGIC     end as resolution_minutes,
# MAGIC 
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from bronze_sladefinitions
# MAGIC where sla_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --load
# MAGIC create or replace table silver_sla_definitions
# MAGIC using delta
# MAGIC as
# MAGIC select * from transformed_sla_definitions;


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

# MARKDOWN ********************

# **<h1>Categories</h1>**

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
# MAGIC create or replace temp view transformed_categories as
# MAGIC select
# MAGIC     category_id,
# MAGIC 
# MAGIC     initcap(trim(category_name)) as category_name,
# MAGIC 
# MAGIC     initcap(trim(subcategory_name)) as subcategory_name,
# MAGIC 
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from bronze_categories
# MAGIC where category_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace table silver_categories
# MAGIC using delta
# MAGIC as
# MAGIC select * from transformed_categories;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SERVICE CATEGORY MAP</h1>**

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
# MAGIC create or replace temp view transformed_servicecategorymap as
# MAGIC select distinct
# MAGIC     trim(service_id) as service_id,
# MAGIC     trim(category_id) as category_id,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC from bronze_servicecategorymap
# MAGIC where service_id is not null
# MAGIC   and category_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace table silver_servicecategorymap
# MAGIC using delta
# MAGIC as
# MAGIC select * from transformed_servicecategorymap;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SERVICE GROUP MAP</h1>**

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
# MAGIC create or replace temp view transformed_servicegroupmap as
# MAGIC select distinct
# MAGIC     trim(service_id) as service_id,
# MAGIC     trim(group_id) as group_id,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC from bronze_servicegroupmap
# MAGIC where service_id is not null
# MAGIC   and group_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace table silver_servicegroupmap
# MAGIC using delta
# MAGIC as
# MAGIC select * from transformed_servicegroupmap;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>TICKETS</h1>**

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
# MAGIC create or replace temp view incremental_tickets as
# MAGIC select *
# MAGIC from bronze_tickets_stg
# MAGIC where ingestion_ts > (
# MAGIC     select coalesce(max(ingestion_ts), timestamp('1900-01-01'))
# MAGIC     from silver_tickets
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create or replace temp view transformed_tickets as
# MAGIC select
# MAGIC     t.ticket_id,
# MAGIC 
# MAGIC     upper(trim(t.ticket_type)) as ticket_type,
# MAGIC 
# MAGIC     cast(t.opened_date as timestamp) as opened_ts,
# MAGIC     cast(t.closed_date as timestamp) as closed_ts,
# MAGIC 
# MAGIC     upper(trim(t.ticket_status)) as ticket_status,
# MAGIC 
# MAGIC     trim(t.requester_id) as requester_id,
# MAGIC     trim(t.assigned_group_id) as assigned_group_id,
# MAGIC     trim(t.service_id) as service_id,
# MAGIC     trim(t.category_id) as category_id,
# MAGIC 
# MAGIC     upper(trim(t.priority)) as priority,
# MAGIC     upper(trim(t.impact)) as impact,
# MAGIC     upper(trim(t.urgency)) as urgency,
# MAGIC 
# MAGIC     trim(t.sla_id) as sla_id,
# MAGIC 
# MAGIC     initcap(trim(t.location)) as location,
# MAGIC 
# MAGIC     trim(t.short_description) as short_description,
# MAGIC     trim(t.description) as description,
# MAGIC 
# MAGIC     cast(t.record_created_at as timestamp) as record_created_ts,
# MAGIC     cast(t.record_updated_at as timestamp) as record_updated_ts,
# MAGIC 
# MAGIC     -- resolution minutes
# MAGIC     case
# MAGIC         when t.opened_date is not null
# MAGIC          and t.closed_date is not null
# MAGIC          and cast(t.closed_date as timestamp) >= cast(t.opened_date as timestamp)
# MAGIC         then
# MAGIC             (unix_timestamp(t.closed_date) - unix_timestamp(t.opened_date)) / 60
# MAGIC         else null
# MAGIC     end as resolution_minutes,
# MAGIC 
# MAGIC     -- sla target
# MAGIC     s.resolution_minutes as sla_target_minutes,
# MAGIC 
# MAGIC     -- sla breach flag
# MAGIC     case
# MAGIC         when t.closed_date is null then null
# MAGIC         when s.resolution_minutes is null then null
# MAGIC         when (unix_timestamp(t.closed_date) - unix_timestamp(t.opened_date)) / 60
# MAGIC              > s.resolution_minutes
# MAGIC         then true
# MAGIC         else false
# MAGIC     end as sla_breach_flag,
# MAGIC 
# MAGIC     -- invalid date flag
# MAGIC     case
# MAGIC         when t.opened_date is null then true
# MAGIC         when t.closed_date is not null
# MAGIC              and cast(t.closed_date as timestamp)
# MAGIC                  < cast(t.opened_date as timestamp)
# MAGIC         then true
# MAGIC         else false
# MAGIC     end as invalid_date_flag,
# MAGIC 
# MAGIC     t.ingestion_ts,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from incremental_tickets t
# MAGIC left join silver_sla_definitions s
# MAGIC     on upper(trim(t.priority)) = s.priority
# MAGIC where t.ticket_id is not null;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC merge into silver_tickets as target
# MAGIC using transformed_tickets as source
# MAGIC on target.ticket_id = source.ticket_id
# MAGIC 
# MAGIC when matched then update set
# MAGIC     target.ticket_type = source.ticket_type,
# MAGIC     target.opened_ts = source.opened_ts,
# MAGIC     target.closed_ts = source.closed_ts,
# MAGIC     target.ticket_status = source.ticket_status,
# MAGIC     target.requester_id = source.requester_id,
# MAGIC     target.assigned_group_id = source.assigned_group_id,
# MAGIC     target.service_id = source.service_id,
# MAGIC     target.category_id = source.category_id,
# MAGIC     target.priority = source.priority,
# MAGIC     target.impact = source.impact,
# MAGIC     target.urgency = source.urgency,
# MAGIC     target.sla_id = source.sla_id,
# MAGIC     target.location = source.location,
# MAGIC     target.short_description = source.short_description,
# MAGIC     target.description = source.description,
# MAGIC     target.record_created_ts = source.record_created_ts,
# MAGIC     target.record_updated_ts = source.record_updated_ts,
# MAGIC     target.resolution_minutes = source.resolution_minutes,
# MAGIC     target.sla_target_minutes = source.sla_target_minutes,
# MAGIC     target.sla_breach_flag = source.sla_breach_flag,
# MAGIC     target.invalid_date_flag = source.invalid_date_flag,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC when not matched then insert *;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>Ticket worklogs</h1>**

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# **<h1>USERS</h1>**

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW incremental_user AS
# MAGIC SELECT *
# MAGIC FROM bronze_Users_Stg
# MAGIC WHERE ingestion_ts > (
# MAGIC     SELECT COALESCE(MAX(ingestion_ts), TIMESTAMP('1900-01-01'))
# MAGIC     FROM silver_user
# MAGIC );
# MAGIC 
# MAGIC 


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
# MAGIC     upper(trim(user_type)) AS user_type,
# MAGIC     initcap(trim(first_name)) AS first_name,
# MAGIC     initcap(trim(last_name)) AS last_name,
# MAGIC     concat_ws(' ',
# MAGIC         initcap(trim(first_name)),
# MAGIC         initcap(trim(last_name))
# MAGIC     ) AS full_name,
# MAGIC     lower(trim(email)) AS email,
# MAGIC     initcap(trim(location)) AS location,
# MAGIC     upper(trim(active_flag)) AS active_flag,
# MAGIC     CAST(created_date AS TIMESTAMP) AS created_ts,
# MAGIC     ingestion_ts,
# MAGIC     current_timestamp() AS load_timestamp
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
# MAGIC     target.is_deleted = 'N',
# MAGIC     target.created_ts = source.created_ts,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     user_id,
# MAGIC     user_type,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     full_name,
# MAGIC     email,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     created_ts,
# MAGIC     ingestion_ts,
# MAGIC     load_timestamp
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.user_id,
# MAGIC     source.user_type,
# MAGIC     source.first_name,
# MAGIC     source.last_name,
# MAGIC     source.full_name,
# MAGIC     source.email,
# MAGIC     source.location,
# MAGIC     source.active_flag,
# MAGIC     'N',
# MAGIC     source.created_ts,
# MAGIC     source.ingestion_ts,
# MAGIC     source.load_timestamp
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete handling
# MAGIC MERGE INTO silver_user AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT user_id
# MAGIC     FROM delta_Users_Stg
# MAGIC     WHERE `__$operation` = 1
# MAGIC ) AS source
# MAGIC ON target.user_id = source.user_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_deleted = 'Y',
# MAGIC         target.active_flag = 'N',
# MAGIC         target.load_timestamp = current_timestamp();
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
# MAGIC MERGE INTO silver_supportgroups AS target
# MAGIC USING transformed_supportgroups AS source
# MAGIC ON target.group_id = source.group_id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.group_name = source.group_name,
# MAGIC     target.location = source.location,
# MAGIC     target.active_flag = source.active_flag,
# MAGIC     target.is_deleted = 'N',
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     group_id,
# MAGIC     group_name,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     ingestion_ts,
# MAGIC     load_timestamp
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.group_id,
# MAGIC     source.group_name,
# MAGIC     source.location,
# MAGIC     source.active_flag,
# MAGIC     'N',
# MAGIC     source.ingestion_ts,
# MAGIC     source.load_timestamp
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete
# MAGIC MERGE INTO silver_supportgroups AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT group_id
# MAGIC     FROM delta_SupportGroups_Stg
# MAGIC     WHERE `__$operation` = 1
# MAGIC ) AS source
# MAGIC ON target.group_id = source.group_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_deleted = 'Y',
# MAGIC         target.active_flag = 'N',
# MAGIC         target.load_timestamp = current_timestamp();


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SERVICES</h1>**

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
# MAGIC MERGE INTO silver_services AS target
# MAGIC USING transformed_services AS source
# MAGIC ON target.service_id = source.service_id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.service_name = source.service_name,
# MAGIC     target.service_owner_id = source.service_owner_id,
# MAGIC     target.criticality = source.criticality,
# MAGIC     target.active_flag = source.active_flag,
# MAGIC     target.is_deleted = 'N',
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     service_id,
# MAGIC     service_name,
# MAGIC     service_owner_id,
# MAGIC     criticality,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     ingestion_ts,
# MAGIC     load_timestamp
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.service_id,
# MAGIC     source.service_name,
# MAGIC     source.service_owner_id,
# MAGIC     source.criticality,
# MAGIC     source.active_flag,
# MAGIC     'N',
# MAGIC     source.ingestion_ts,
# MAGIC     source.load_timestamp
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete
# MAGIC MERGE INTO silver_services AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT service_id
# MAGIC     FROM delta_Services_Stg
# MAGIC     WHERE `__$operation` = 1
# MAGIC ) AS source
# MAGIC ON target.service_id = source.service_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_deleted = 'Y',
# MAGIC         target.active_flag = 'N',
# MAGIC         target.load_timestamp = current_timestamp();


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **<h1>SLA DEFINITIONS</h1>**

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

# MARKDOWN ********************

# **<h1>Categories</h1>**

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
# MAGIC CREATE OR REPLACE TEMP VIEW transformed_tickets AS
# MAGIC SELECT
# MAGIC     t.ticket_id,
# MAGIC 
# MAGIC     upper(trim(t.ticket_type)) AS ticket_type,
# MAGIC 
# MAGIC     CAST(t.opened_date AS TIMESTAMP) AS opened_ts,
# MAGIC     CAST(t.closed_date AS TIMESTAMP) AS closed_ts,
# MAGIC 
# MAGIC     upper(trim(t.ticket_status)) AS ticket_status,
# MAGIC 
# MAGIC     trim(t.requester_id) AS requester_id,
# MAGIC     trim(t.assigned_group_id) AS assigned_group_id,
# MAGIC     trim(t.service_id) AS service_id,
# MAGIC     trim(t.category_id) AS category_id,
# MAGIC 
# MAGIC     upper(trim(t.priority)) AS priority,
# MAGIC     upper(trim(t.impact)) AS impact,
# MAGIC     upper(trim(t.urgency)) AS urgency,
# MAGIC 
# MAGIC     trim(t.sla_id) AS sla_id,
# MAGIC 
# MAGIC     initcap(trim(t.location)) AS location,
# MAGIC 
# MAGIC     trim(t.short_description) AS short_description,
# MAGIC     trim(t.description) AS description,
# MAGIC 
# MAGIC     CAST(t.record_created_at AS TIMESTAMP) AS record_created_ts,
# MAGIC     CAST(t.record_updated_at AS TIMESTAMP) AS record_updated_ts,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN t.opened_date IS NOT NULL
# MAGIC          AND t.closed_date IS NOT NULL
# MAGIC          AND CAST(t.closed_date AS TIMESTAMP) >= CAST(t.opened_date AS TIMESTAMP)
# MAGIC         THEN (unix_timestamp(t.closed_date) - unix_timestamp(t.opened_date)) / 60
# MAGIC         ELSE NULL
# MAGIC     END AS resolution_minutes,
# MAGIC 
# MAGIC     s.resolution_minutes AS sla_target_minutes,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN t.closed_date IS NULL THEN NULL
# MAGIC         WHEN s.resolution_minutes IS NULL THEN NULL
# MAGIC         WHEN (unix_timestamp(t.closed_date) - unix_timestamp(t.opened_date)) / 60
# MAGIC              > s.resolution_minutes
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS sla_breach_flag,
# MAGIC 
# MAGIC     CASE
# MAGIC         WHEN t.opened_date IS NULL THEN TRUE
# MAGIC         WHEN t.closed_date IS NOT NULL
# MAGIC              AND CAST(t.closed_date AS TIMESTAMP)
# MAGIC                  < CAST(t.opened_date AS TIMESTAMP)
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS invalid_date_flag,
# MAGIC 
# MAGIC     t.ingestion_ts,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC 
# MAGIC FROM incremental_tickets t
# MAGIC LEFT JOIN silver_sla_definitions s
# MAGIC     ON upper(trim(t.priority)) = s.priority
# MAGIC WHERE t.ticket_id IS NOT NULL;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO silver_tickets AS target
# MAGIC USING transformed_tickets AS source
# MAGIC ON target.ticket_id = source.ticket_id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
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
# MAGIC     target.is_deleted = 'N',
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     ticket_id,
# MAGIC     ticket_type,
# MAGIC     opened_ts,
# MAGIC     closed_ts,
# MAGIC     ticket_status,
# MAGIC     requester_id,
# MAGIC     assigned_group_id,
# MAGIC     service_id,
# MAGIC     category_id,
# MAGIC     priority,
# MAGIC     impact,
# MAGIC     urgency,
# MAGIC     sla_id,
# MAGIC     location,
# MAGIC     short_description,
# MAGIC     description,
# MAGIC     record_created_ts,
# MAGIC     record_updated_ts,
# MAGIC     resolution_minutes,
# MAGIC     sla_target_minutes,
# MAGIC     sla_breach_flag,
# MAGIC     invalid_date_flag,
# MAGIC     is_deleted,
# MAGIC     ingestion_ts,
# MAGIC     load_timestamp
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.ticket_id,
# MAGIC     source.ticket_type,
# MAGIC     source.opened_ts,
# MAGIC     source.closed_ts,
# MAGIC     source.ticket_status,
# MAGIC     source.requester_id,
# MAGIC     source.assigned_group_id,
# MAGIC     source.service_id,
# MAGIC     source.category_id,
# MAGIC     source.priority,
# MAGIC     source.impact,
# MAGIC     source.urgency,
# MAGIC     source.sla_id,
# MAGIC     source.location,
# MAGIC     source.short_description,
# MAGIC     source.description,
# MAGIC     source.record_created_ts,
# MAGIC     source.record_updated_ts,
# MAGIC     source.resolution_minutes,
# MAGIC     source.sla_target_minutes,
# MAGIC     source.sla_breach_flag,
# MAGIC     source.invalid_date_flag,
# MAGIC     'N',
# MAGIC     source.ingestion_ts,
# MAGIC     source.load_timestamp
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --delete merge
# MAGIC MERGE INTO silver_tickets AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT ticket_id
# MAGIC     FROM staging_Tickets_Stg
# MAGIC     WHERE `__$operation` = 1
# MAGIC ) AS source
# MAGIC ON target.ticket_id = source.ticket_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_deleted = 'Y',
# MAGIC         target.active_flag = 'N',
# MAGIC         target.load_timestamp = current_timestamp();


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

# CELL ********************

# MAGIC %%sql
# MAGIC --incremental extraction
# MAGIC create or replace temp view incremental_worklogs as
# MAGIC select *
# MAGIC from bronze_ticketworklogs_stg
# MAGIC where ingestion_ts > (
# MAGIC     select coalesce(max(ingestion_ts), timestamp('1900-01-01'))
# MAGIC     from silver_ticket_worklogs
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --transformation
# MAGIC create or replace temp view transformed_worklogs as
# MAGIC select
# MAGIC     worklog_id,
# MAGIC 
# MAGIC     trim(ticket_id) as ticket_id,
# MAGIC 
# MAGIC     trim(updated_by) as updated_by,
# MAGIC 
# MAGIC     upper(trim(update_type)) as update_type,
# MAGIC 
# MAGIC     cast(update_timestamp as timestamp) as update_ts,
# MAGIC 
# MAGIC     trim(worklog_comment) as worklog_comment,
# MAGIC 
# MAGIC     case
# MAGIC         when update_timestamp is null then true
# MAGIC         else false
# MAGIC     end as invalid_timestamp_flag,
# MAGIC 
# MAGIC     ingestion_ts,
# MAGIC     current_timestamp() as load_timestamp
# MAGIC 
# MAGIC from incremental_worklogs
# MAGIC where worklog_id is not null
# MAGIC   and ticket_id is not null;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC merge into silver_ticket_worklogs as target
# MAGIC using transformed_worklogs as source
# MAGIC on target.worklog_id = source.worklog_id
# MAGIC 
# MAGIC when matched then update set
# MAGIC     target.ticket_id = source.ticket_id,
# MAGIC     target.updated_by = source.updated_by,
# MAGIC     target.update_type = source.update_type,
# MAGIC     target.update_ts = source.update_ts,
# MAGIC     target.worklog_comment = source.worklog_comment,
# MAGIC     target.invalid_timestamp_flag = source.invalid_timestamp_flag,
# MAGIC     target.ingestion_ts = source.ingestion_ts,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC 
# MAGIC when not matched then insert *;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

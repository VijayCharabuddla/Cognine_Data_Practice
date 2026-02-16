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
# MAGIC --merge tickets
# MAGIC MERGE INTO bronze_Tickets_Stg AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM staging_Tickets_Stg
# MAGIC     WHERE `__$operation` IN (1,2,4)
# MAGIC ) AS source
# MAGIC ON target.ticket_id = source.ticket_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 1 THEN
# MAGIC   DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 4 THEN
# MAGIC   UPDATE SET
# MAGIC     ticket_type = source.ticket_type,
# MAGIC     opened_date = source.opened_date,
# MAGIC     closed_date = source.closed_date,
# MAGIC     ticket_status = source.ticket_status,
# MAGIC     requester_id = source.requester_id,
# MAGIC     assigned_group_id = source.assigned_group_id,
# MAGIC     service_id = source.service_id,
# MAGIC     category_id = source.category_id,
# MAGIC     priority = source.priority,
# MAGIC     impact = source.impact,
# MAGIC     urgency = source.urgency,
# MAGIC     sla_id = source.sla_id,
# MAGIC     location = source.location,
# MAGIC     short_description = source.short_description,
# MAGIC     description = source.description,
# MAGIC     record_created_at = source.record_created_at,
# MAGIC     record_updated_at = source.record_updated_at,
# MAGIC     ingestion_ts = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED AND source.`__$operation` = 2 THEN
# MAGIC   INSERT (
# MAGIC     ticket_id,
# MAGIC     ticket_type,
# MAGIC     opened_date,
# MAGIC     closed_date,
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
# MAGIC     record_created_at,
# MAGIC     record_updated_at,
# MAGIC     ingestion_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.ticket_id,
# MAGIC     source.ticket_type,
# MAGIC     source.opened_date,
# MAGIC     source.closed_date,
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
# MAGIC     source.record_created_at,
# MAGIC     source.record_updated_at,
# MAGIC     current_timestamp()
# MAGIC   );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge users
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO bronze_Users_Stg AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM delta_Users_Stg
# MAGIC     WHERE `__$operation` IN (1,2,4)
# MAGIC ) AS source
# MAGIC ON target.user_id = source.user_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 1 THEN
# MAGIC   DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 4 THEN
# MAGIC   UPDATE SET
# MAGIC     user_type = source.user_type,
# MAGIC     first_name = source.first_name,
# MAGIC     last_name = source.last_name,
# MAGIC     email = source.email,
# MAGIC     location = source.location,
# MAGIC     active_flag = source.active_flag,
# MAGIC     is_deleted = source.is_deleted,
# MAGIC     created_date = source.created_date,
# MAGIC     ingestion_ts = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED AND source.`__$operation` = 2 THEN
# MAGIC   INSERT (
# MAGIC     user_id,
# MAGIC     user_type,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     is_deleted,
# MAGIC     created_date,
# MAGIC     ingestion_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.user_id,
# MAGIC     source.user_type,
# MAGIC     source.first_name,
# MAGIC     source.last_name,
# MAGIC     source.email,
# MAGIC     source.location,
# MAGIC     source.active_flag,
# MAGIC     source.is_deleted,
# MAGIC     source.created_date,
# MAGIC     current_timestamp()
# MAGIC   );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge services
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO bronze_Services_Stg AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM delta_Services_Stg
# MAGIC     WHERE `__$operation` IN (1,2,4)
# MAGIC ) AS source
# MAGIC ON target.service_id = source.service_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 1 THEN
# MAGIC   DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 4 THEN
# MAGIC   UPDATE SET
# MAGIC     service_name = source.service_name,
# MAGIC     service_owner_id = source.service_owner_id,
# MAGIC     criticality = source.criticality,
# MAGIC     active_flag = source.active_flag,
# MAGIC     ingestion_ts = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED AND source.`__$operation` = 2 THEN
# MAGIC   INSERT (
# MAGIC     service_id,
# MAGIC     service_name,
# MAGIC     service_owner_id,
# MAGIC     criticality,
# MAGIC     active_flag,
# MAGIC     ingestion_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.service_id,
# MAGIC     source.service_name,
# MAGIC     source.service_owner_id,
# MAGIC     source.criticality,
# MAGIC     source.active_flag,
# MAGIC     current_timestamp()
# MAGIC   );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC MERGE INTO bronze_SupportGroups_Stg AS target
# MAGIC USING delta_SupportGroups_Stg AS source
# MAGIC ON target.group_id = source.group_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 1 THEN DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 4 THEN
# MAGIC   UPDATE SET
# MAGIC     group_name = source.group_name,
# MAGIC     location = source.location,
# MAGIC     active_flag = source.active_flag,
# MAGIC     ingestion_ts = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED AND source.`__$operation` = 2 THEN
# MAGIC   INSERT (
# MAGIC     group_id,
# MAGIC     group_name,
# MAGIC     location,
# MAGIC     active_flag,
# MAGIC     ingestion_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.group_id,
# MAGIC     source.group_name,
# MAGIC     source.location,
# MAGIC     source.active_flag,
# MAGIC     current_timestamp()
# MAGIC   );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --merge ticket_worklogs
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO bronze_TicketWorklogs_Stg AS target
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM delta_TicketWorklogs_Stg
# MAGIC     WHERE `__$operation` IN (1,2,4)
# MAGIC ) AS source
# MAGIC ON target.worklog_id = source.worklog_id
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 1 THEN
# MAGIC   DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source.`__$operation` = 4 THEN
# MAGIC   UPDATE SET
# MAGIC     ticket_id = source.ticket_id,
# MAGIC     updated_by = source.updated_by,
# MAGIC     update_type = source.update_type,
# MAGIC     update_timestamp = source.update_timestamp,
# MAGIC     worklog_comment = source.worklog_comment,
# MAGIC     ingestion_ts = current_timestamp()
# MAGIC 
# MAGIC WHEN NOT MATCHED AND source.`__$operation` = 2 THEN
# MAGIC   INSERT (
# MAGIC     worklog_id,
# MAGIC     ticket_id,
# MAGIC     updated_by,
# MAGIC     update_type,
# MAGIC     update_timestamp,
# MAGIC     worklog_comment,
# MAGIC     ingestion_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.worklog_id,
# MAGIC     source.ticket_id,
# MAGIC     source.updated_by,
# MAGIC     source.update_type,
# MAGIC     source.update_timestamp,
# MAGIC     source.worklog_comment,
# MAGIC     current_timestamp()
# MAGIC   );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE delta_Users_Stg;
# MAGIC TRUNCATE TABLE delta_Services_Stg;
# MAGIC TRUNCATE TABLE delta_SupportGroups_Stg;
# MAGIC TRUNCATE TABLE delta_TicketWorklogs_Stg;
# MAGIC TRUNCATE TABLE staging_Tickets_Stg;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC truncate table delta_SupportGroups_Stg

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

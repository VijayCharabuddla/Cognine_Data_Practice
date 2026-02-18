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
# MAGIC WITH numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY category_id) AS rn,
# MAGIC         category_id,
# MAGIC         category_name,
# MAGIC         subcategory_name
# MAGIC     FROM silver_categories
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_category (
# MAGIC     category_key,
# MAGIC     category_id,
# MAGIC     category_name,
# MAGIC     subcategory_name,
# MAGIC     updated_ts
# MAGIC )
# MAGIC SELECT
# MAGIC     rn,
# MAGIC     category_id,
# MAGIC     category_name,
# MAGIC     subcategory_name,
# MAGIC     current_timestamp()
# MAGIC FROM numbered;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO dim_category tgt
# MAGIC USING silver_categories src
# MAGIC ON tgt.category_id = src.category_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC       tgt.category_name = src.category_name,
# MAGIC       tgt.subcategory_name = src.subcategory_name,
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
# MAGIC     FROM silver_categories src
# MAGIC     LEFT JOIN dim_category tgt
# MAGIC         ON tgt.category_id = src.category_id
# MAGIC     WHERE tgt.category_id IS NULL
# MAGIC ),
# MAGIC 
# MAGIC key_base AS (
# MAGIC     SELECT COALESCE(MAX(category_key), 0) AS max_key
# MAGIC     FROM dim_category
# MAGIC ),
# MAGIC 
# MAGIC numbered AS (
# MAGIC     SELECT
# MAGIC         ROW_NUMBER() OVER (ORDER BY category_id) AS rn,
# MAGIC         *
# MAGIC     FROM new_rows
# MAGIC )
# MAGIC 
# MAGIC INSERT INTO dim_category (
# MAGIC     category_key,
# MAGIC     category_id,
# MAGIC     category_name,
# MAGIC     subcategory_name,
# MAGIC     updated_ts
# MAGIC )
# MAGIC SELECT
# MAGIC     kb.max_key + n.rn,
# MAGIC     n.category_id,
# MAGIC     n.category_name,
# MAGIC     n.subcategory_name,
# MAGIC     current_timestamp()
# MAGIC FROM numbered n
# MAGIC CROSS JOIN key_base kb;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

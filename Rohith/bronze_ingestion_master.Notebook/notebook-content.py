# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e6f809b-9114-472a-9d0a-7e1e3c495e19",
# META       "default_lakehouse_name": "sales_lh",
# META       "default_lakehouse_workspace_id": "f8fe95e1-523b-46a1-b576-c62cc88f15d5",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e6f809b-9114-472a-9d0a-7e1e3c495e19"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from notebookutils import mssparkutils


# -----------------------------
# CONFIG (CHANGE ONCE)
# -----------------------------

storage = "rohithfabric"
container = "source-data"

tables = [
    {
        "table_name": "customers",
        "folder": "customers",
        "bronze": "customers_bronze"
    },
    {
        "table_name": "customer_addresses",
        "folder": "customer_addresses",
        "bronze": "customer_addresses_bronze"
    },
    {
        "table_name": "products",
        "folder": "products",
        "bronze": "products_bronze"
    },
    {
        "table_name": "product_categories",
        "folder": "product_categories",
        "bronze": "product_categories_bronze"
    },
    {
        "table_name": "sales_channels",
        "folder": "sales_channels",
        "bronze": "channels_bronze"
    },
    {
        "table_name": "orders_headers",
        "folder": "orders_headers",
        "bronze": "orders_header_bronze"
    },
    {
        "table_name": "order_line_items",
        "folder": "order_line_items",
        "bronze": "line_items_bronze"
    }
]

# -----------------------------
# LOAD AUDIT TABLE
# -----------------------------

audit_df = spark.read.table("file_audit")

processed_files = set(
    audit_df
    .filter(col("processed_flag") == 1)
    .select("table_name", "file_name")
    .rdd
    .map(lambda r: (r[0], r[1]))
    .collect()
)


# -----------------------------
# PROCESS EACH TABLE
# -----------------------------

for t in tables:

    print(f"Processing {t['table_name']}")

    base_path = f"abfss://{container}@{storage}.dfs.core.windows.net/{t['folder']}/"

    files = mssparkutils.fs.ls(base_path)

    new_files = []

    # Collect only unprocessed files
    for f in files:
        file_name = f.name

        if (t["table_name"], file_name) in processed_files:
            print(f"Skipping {file_name}")
            continue

        new_files.append(file_name)

    if len(new_files) == 0:
        print("No new files")
        continue

    print("New files:", new_files)

    # Read all new files together
    paths = [base_path + f for f in new_files]

    df = spark.read.option("header", True).csv(paths)

    df = df.withColumn("_ingestion_time", current_timestamp())

    # ONE write per table
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable(t["bronze"])

    # Update audit in batch
    for file_name in new_files:

        merge_sql = f"""
        MERGE INTO file_audit t
        USING (
            SELECT
                '{t["table_name"]}' AS table_name,
                '{file_name}' AS file_name,
                1 AS processed_flag,
                current_timestamp() AS processed_time
        ) s
        ON t.table_name = s.table_name
        AND t.file_name = s.file_name

        WHEN MATCHED THEN
        UPDATE SET
            t.processed_flag = 1,
            t.processed_time = current_timestamp()

        WHEN NOT MATCHED THEN
        INSERT (
            table_name,
            file_name,
            processed_flag,
            processed_time
        )
        VALUES (
            s.table_name,
            s.file_name,
            s.processed_flag,
            s.processed_time
        )
        """

        spark.sql(merge_sql)

        processed_files.add((t["table_name"], file_name))

   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from customers_bronze

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

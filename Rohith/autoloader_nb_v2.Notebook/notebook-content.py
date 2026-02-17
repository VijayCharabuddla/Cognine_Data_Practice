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

# PARAMETERS CELL ********************

config_json = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import json

BASE_PATH = "abfss://source-data@rohithfabric.dfs.core.windows.net"
AUDIT_TABLE = "file_audit"

config = json.loads(config_json)
print(f"Total tables to process: {len(config)}")
print(config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for item in config:
    table_name  = item["table_name"]
    folder_name = item["folder_name"]
    bronze_table = item["bronze_table"]
    FOLDER_PATH = BASE_PATH + "/" + folder_name

    print(f"\n========== Processing: {table_name} ==========")

    # Step 1: List all files in folder
    all_files = mssparkutils.fs.ls(FOLDER_PATH)
    all_file_names = [f.name for f in all_files if not f.isDir]
    print(f"Total files found: {all_file_names}")

    # Step 2: Get already processed files from audit
    audit_query = "SELECT file_name FROM " + AUDIT_TABLE + " WHERE table_name = '" + table_name + "' AND processed_flag = 1"
    audit_df = spark.sql(audit_query)
    processed_files = [row.file_name for row in audit_df.collect()]

    # Step 3: Filter to new files only
    new_files = [f for f in all_file_names if f not in processed_files]
    print(f"Already processed: {processed_files}")
    print(f"New files to process: {new_files}")

    if not new_files:
        print(f"No new files for {table_name}. Skipping.")
        continue

    # Step 4: Process each new file
    for file_name in new_files:
        file_path = FOLDER_PATH + "/" + file_name
        print(f"  Processing: {file_path}")

        df = spark.read.option("header", "true") \
                       .option("inferSchema", "false") \
                       .csv(file_path)

        df = df.withColumn("_source_file_name", F.lit(file_name)) \
               .withColumn("_ingestion_timestamp", F.current_timestamp())

        df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(bronze_table)

        print(f"  Written {df.count()} rows to {bronze_table}")

        insert_query = "INSERT INTO " + AUDIT_TABLE + " (table_name, file_name, processed_flag, processed_time) VALUES ('" + table_name + "', '" + file_name + "', 1, current_timestamp())"
        spark.sql(insert_query)
        print(f"  Audit logged for {file_name}")

print("\n========== Autoloader Complete ==========")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

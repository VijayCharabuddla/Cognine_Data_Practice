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

from notebookutils import mssparkutils
from pyspark.sql.functions import *

# Get parameters
folder = mssparkutils.env.getJobParameter("folder")
file = mssparkutils.env.getJobParameter("file")
bronze = mssparkutils.env.getJobParameter("bronze")

# File path
path = f"abfss://source-data@rohithfabric.dfs.core.windows.net/{folder}/{file}"

# Read CSV
df = spark.read.option("header", True).csv(path)

# Add metadata
df = df.withColumn("_file_name", lit(file)) \
       .withColumn("_ingestion_time", current_timestamp())

# Write to bronze
df.write.mode("append").saveAsTable(bronze)


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

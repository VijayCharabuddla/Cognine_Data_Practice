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

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

print("Gold processing started.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: dim_date =====")

# Get min and max order_date from orders_headers_silver
date_range = spark.sql("""
    SELECT 
        MIN(order_date) as min_date,
        MAX(order_date) as max_date
    FROM orders_headers_silver
""").first()

min_date = str(date_range.min_date)
max_date = str(date_range.max_date)

print(f"Date range: {min_date} to {max_date}")

# Generate all dates in range
date_query = "SELECT explode(sequence(to_date('" + min_date + "'), to_date('" + max_date + "'), interval 1 day)) as date"
dates_df = spark.sql(date_query)

# Calculate date attributes
dates_df = dates_df.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")) \
    .withColumn("year", F.year(F.col("date"))) \
    .withColumn("quarter", F.quarter(F.col("date"))) \
    .withColumn("month", F.month(F.col("date"))) \
    .withColumn("month_name", F.date_format(F.col("date"), "MMMM")) \
    .withColumn("day", F.dayofmonth(F.col("date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("date"))) \
    .withColumn("day_name", F.date_format(F.col("date"), "EEEE")) \
    .withColumn("week_of_year", F.weekofyear(F.col("date"))) \
    .withColumn("is_weekend", F.when(F.dayofweek(F.col("date")).isin([1, 7]), True).otherwise(False))

# Merge into dim_date - insert only new dates
DeltaTable.forName(spark, "dim_date").alias("target").merge(
    dates_df.alias("source"),
    "target.date_key = source.date_key"
).whenNotMatchedInsertAll().execute()

print(f"dim_date updated. Total dates: {spark.table('dim_date').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: dim_customer =====")

# Read all customer versions from Silver
customers = spark.table("customers_silver") \
    .select("customer_id", "first_name", "last_name", "segment", "email", "phone", 
            "created_date", "row_hash", "silver_ingestion_timestamp")

# Window to order versions per customer
window_spec = Window.partitionBy("customer_id").orderBy("silver_ingestion_timestamp")

# Build SCD Type 2 structure
customers_scd = customers \
    .withColumn("version_number", F.row_number().over(window_spec)) \
    .withColumn("next_timestamp", F.lead("silver_ingestion_timestamp").over(window_spec)) \
    .withColumn("effective_start_date", 
                F.when(F.col("version_number") == 1, F.col("created_date").cast("timestamp"))
                 .otherwise(F.col("silver_ingestion_timestamp"))) \
    .withColumn("effective_end_date", F.col("next_timestamp")) \
    .withColumn("is_current", F.when(F.col("next_timestamp").isNull(), True).otherwise(False))

# To compare with existing records, we need row_hash from Silver for records already in Gold
existing_gold_customers = spark.table("dim_customer").select("customer_id", "effective_start_date")

# Join existing gold records back to Silver to get their row_hash
existing_with_hash = existing_gold_customers.join(
    spark.table("customers_silver").select("customer_id", "row_hash", 
                                           F.col("silver_ingestion_timestamp").alias("silver_ts")),
    on="customer_id"
).withColumn("eff_start_match",
             F.when(F.col("effective_start_date") == F.col("silver_ts"), True)
              .when(F.col("effective_start_date") == F.col("silver_ts").cast("date").cast("timestamp"), True)
              .otherwise(False)
).filter(F.col("eff_start_match") == True) \
 .select("customer_id", "effective_start_date", "row_hash")

# Find new versions
new_versions = customers_scd.join(
    existing_with_hash,
    on=["customer_id", "effective_start_date", "row_hash"],
    how="left_anti"
)

# Generate surrogate keys for new versions
if new_versions.count() > 0:
    max_key_row = spark.sql("SELECT COALESCE(MAX(customer_key), 0) as max_key FROM dim_customer").first()
    max_key = max_key_row.max_key
    
    new_versions_with_key = new_versions \
        .withColumn("row_num", F.row_number().over(Window.orderBy("customer_id", "effective_start_date"))) \
        .withColumn("customer_key", (F.col("row_num") + F.lit(max_key)).cast("bigint")) \
        .select("customer_key", "customer_id", "first_name", "last_name", "segment", "email", 
                "phone", "created_date", "effective_start_date", "effective_end_date", "is_current")
    
    # Append new versions (row_hash is excluded here)
    new_versions_with_key.write.format("delta").mode("append").saveAsTable("dim_customer")
    print(f"dim_customer: Inserted {new_versions_with_key.count()} new versions.")
else:
    print("dim_customer: No new versions to insert.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

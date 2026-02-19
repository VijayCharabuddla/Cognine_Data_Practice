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

# Build SCD Type 2 structure - use created_date for effective dates
customers_scd = customers \
    .withColumn("version_number", F.row_number().over(window_spec)) \
    .withColumn("next_created_date", F.lead("created_date").over(window_spec)) \
    .withColumn("effective_start_date", F.col("created_date").cast("timestamp")) \
    .withColumn("effective_end_date", F.col("next_created_date").cast("timestamp")) \
    .withColumn("is_current", F.when(F.col("next_created_date").isNull(), True).otherwise(False))

# Find versions not yet in Gold
existing_versions = spark.table("dim_customer").select("customer_id", "effective_start_date")

new_versions = customers_scd.join(
    existing_versions,
    on=["customer_id", "effective_start_date"],
    how="left_anti"
)

# Count before any other operations
new_count = new_versions.count()

if new_count > 0:
    max_key_row = spark.sql("SELECT COALESCE(MAX(customer_key), 0) as max_key FROM dim_customer").first()
    max_key = max_key_row.max_key
    
    new_versions_with_key = new_versions \
        .withColumn("row_num", F.row_number().over(Window.orderBy("customer_id", "effective_start_date"))) \
        .withColumn("customer_key", (F.col("row_num") + F.lit(max_key)).cast("bigint")) \
        .select("customer_key", "customer_id", "first_name", "last_name", "segment", "email", 
                "phone", "created_date", "effective_start_date", "effective_end_date", "is_current")
    
    # Append new versions
    new_versions_with_key.write.format("delta").mode("append").saveAsTable("dim_customer")
    print(f"dim_customer: Inserted {new_count} new versions.")
else:
    print("dim_customer: No new versions to insert.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: dim_product =====")

# Join products with categories and get latest version per product_id
products = spark.table("products_silver").alias("p") \
    .join(spark.table("product_categories_silver").alias("c"), "category_id") \
    .select("p.product_id", "p.product_name", "p.category_id", "c.category_name", 
            "c.category_group", "p.unit_cost", "p.unit_price", "p.status", 
            "p.row_hash", "p.silver_ingestion_timestamp")

# Get latest version per product_id
window_spec = Window.partitionBy("product_id").orderBy(F.desc("silver_ingestion_timestamp"))
latest_products = products \
    .withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank", "silver_ingestion_timestamp")

# Get existing dim_product
existing_dim = spark.table("dim_product")

# Assign stable surrogate keys
# For existing product_ids, reuse their key; for new ones, generate new keys
existing_keys = existing_dim.select("product_id", "product_key")

products_with_keys = latest_products.join(existing_keys, on="product_id", how="left")

# Get max key for new products
max_key = spark.sql("SELECT COALESCE(MAX(product_key), 0) as max_key FROM dim_product").first().max_key

# Assign new keys to products without keys
new_products = products_with_keys.filter(F.col("product_key").isNull())
if new_products.count() > 0:
    new_products = new_products \
        .withColumn("row_num", F.row_number().over(Window.orderBy("product_id"))) \
        .withColumn("product_key", F.col("row_num") + max_key) \
        .drop("row_num")
    
    # Union with existing products that already have keys
    existing_products = products_with_keys.filter(F.col("product_key").isNotNull())
    products_with_keys = existing_products.unionByName(new_products)

# Merge into dim_product
final_products = products_with_keys.select(
    "product_key", "product_id", "product_name", "category_id", "category_name", 
    "category_group", "unit_cost", "unit_price", "status"
)

DeltaTable.forName(spark, "dim_product").alias("target").merge(
    final_products.alias("source"),
    "target.product_id = source.product_id"
).whenMatchedUpdate(
    condition="target.product_name != source.product_name OR " +
              "target.unit_cost != source.unit_cost OR " +
              "target.unit_price != source.unit_price OR " +
              "target.status != source.status OR " +
              "target.category_name != source.category_name OR " +
              "target.category_group != source.category_group",
    set={
        "product_name": "source.product_name",
        "category_id": "source.category_id",
        "category_name": "source.category_name",
        "category_group": "source.category_group",
        "unit_cost": "source.unit_cost",
        "unit_price": "source.unit_price",
        "status": "source.status"
    }
).whenNotMatchedInsertAll().execute()

print(f"dim_product updated. Total products: {spark.table('dim_product').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: dim_channel =====")

# Get latest version per channel_id
channels = spark.table("channels_silver")

window_spec = Window.partitionBy("channel_id").orderBy(F.desc("silver_ingestion_timestamp"))
latest_channels = channels \
    .withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank", "row_hash", "silver_ingestion_timestamp")

# Get existing dim_channel
existing_dim = spark.table("dim_channel")

# Assign stable surrogate keys
existing_keys = existing_dim.select("channel_id", "channel_key")
channels_with_keys = latest_channels.join(existing_keys, on="channel_id", how="left")

# Get max key for new channels
max_key = spark.sql("SELECT COALESCE(MAX(channel_key), 0) as max_key FROM dim_channel").first().max_key

# Assign new keys to channels without keys
new_channels = channels_with_keys.filter(F.col("channel_key").isNull())
if new_channels.count() > 0:
    new_channels = new_channels \
        .withColumn("row_num", F.row_number().over(Window.orderBy("channel_id"))) \
        .withColumn("channel_key", F.col("row_num") + max_key) \
        .drop("row_num")
    
    existing_channels = channels_with_keys.filter(F.col("channel_key").isNotNull())
    channels_with_keys = existing_channels.unionByName(new_channels)

# Merge into dim_channel
final_channels = channels_with_keys.select(
    "channel_key", "channel_id", "channel_name", "channel_type", "region"
)

DeltaTable.forName(spark, "dim_channel").alias("target").merge(
    final_channels.alias("source"),
    "target.channel_id = source.channel_id"
).whenMatchedUpdate(
    condition="target.channel_name != source.channel_name OR " +
              "target.channel_type != source.channel_type OR " +
              "target.region != source.region",
    set={
        "channel_name": "source.channel_name",
        "channel_type": "source.channel_type",
        "region": "source.region"
    }
).whenNotMatchedInsertAll().execute()

print(f"dim_channel updated. Total channels: {spark.table('dim_channel').count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: fact_sales =====")

# Get last watermark
last_watermark_query = "SELECT last_watermark FROM watermark_table WHERE table_name = 'fact_sales'"
last_watermark = spark.sql(last_watermark_query).first().last_watermark

print(f"Last watermark: {last_watermark}")

# Get new orders since last watermark
orders = spark.table("orders_headers_silver") \
    .filter(F.col("silver_ingestion_timestamp") > last_watermark) \
    .select("order_id", "customer_id", "channel_id", "order_date", "order_status", "silver_ingestion_timestamp")

# Get ALL line items for these new orders (not filtered by watermark)
new_order_ids = orders.select("order_id").distinct()

line_items = spark.table("line_items_silver") \
    .join(new_order_ids, on="order_id", how="inner") \
    .select("line_id", "order_id", "product_id", "quantity", "unit_price")

# Join orders with line items
sales = orders.join(line_items, on="order_id", how="inner")

sales_count = sales.count()

if sales_count > 0:
    # Lookup dimension keys
    dim_customer = spark.table("dim_customer").filter(F.col("is_current") == True) \
        .select(F.col("customer_id"), F.col("customer_key"))
    
    dim_product = spark.table("dim_product").select("product_id", "product_key")
    dim_channel = spark.table("dim_channel").select("channel_id", "channel_key")
    dim_date = spark.table("dim_date").select(F.col("date").alias("order_date"), F.col("date_key"))
    
    # Join to get all surrogate keys
    fact = sales \
        .join(dim_customer, on="customer_id", how="inner") \
        .join(dim_product, on="product_id", how="inner") \
        .join(dim_channel, on="channel_id", how="inner") \
        .join(dim_date, on="order_date", how="inner") \
        .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(10,2)")) \
        .select("line_id", "order_id", "product_key", "customer_key", "channel_key", 
                "date_key", "order_status", "quantity", "unit_price", "total_amount")
    
    fact_count = fact.count()
    
    # Append to fact_sales
    fact.write.format("delta").mode("append").saveAsTable("fact_sales")
    print(f"fact_sales: Inserted {fact_count} new records.")
    
    # Update watermark to max timestamp from this batch
    new_watermark = sales.agg(F.max("silver_ingestion_timestamp")).first()[0]
    
    update_query = "UPDATE watermark_table SET last_watermark = CAST('" + str(new_watermark) + "' AS TIMESTAMP) WHERE table_name = 'fact_sales'"
    spark.sql(update_query)
    
    print(f"Watermark updated to: {new_watermark}")
else:
    print("fact_sales: No new records to process.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

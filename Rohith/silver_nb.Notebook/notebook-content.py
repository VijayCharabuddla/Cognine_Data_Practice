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

print("Imports done.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: customers_silver =====")

biz_cols = ["customer_id", "first_name", "last_name", "segment", "email", "phone", "created_date"]

df = spark.table("customers_bronze") \
    .withColumn("first_name",    F.trim(F.col("first_name"))) \
    .withColumn("last_name",     F.trim(F.col("last_name"))) \
    .withColumn("email",         F.trim(F.col("email"))) \
    .withColumn("created_date",  F.to_date(F.col("created_date"))) \
    .filter(F.col("customer_id").isNotNull()) \
    .select(*biz_cols)

hash_cols = ["first_name", "last_name", "segment", "email", "phone", "created_date"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("customers_silver").select("customer_id", "row_hash")
new_versions = df.join(existing, on=["customer_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("customers_silver")
print(f"customers_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: customer_addresses_silver =====")

biz_cols = ["address_id", "customer_id", "street", "city", "state", "country", "pincode"]

df = spark.table("customer_addresses_bronze") \
    .withColumn("street",  F.trim(F.col("street"))) \
    .withColumn("city",    F.trim(F.col("city"))) \
    .withColumn("state",   F.trim(F.col("state"))) \
    .withColumn("country", F.trim(F.col("country"))) \
    .withColumn("pincode", F.col("pincode").cast("integer")) \
    .filter(F.col("address_id").isNotNull()) \
    .select(*biz_cols)

hash_cols = ["customer_id", "street", "city", "state", "country", "pincode"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("customer_addresses_silver").select("address_id", "row_hash")
new_versions = df.join(existing, on=["address_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("customer_addresses_silver")
print(f"customer_addresses_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: products_silver =====")

biz_cols = ["product_id", "product_name", "category_id", "unit_cost", "unit_price", "status"]

df = spark.table("products_bronze") \
    .withColumn("product_name", F.trim(F.col("product_name"))) \
    .withColumn("status",       F.trim(F.col("status"))) \
    .withColumn("unit_cost",    F.col("unit_cost").cast("decimal(10,2)")) \
    .withColumn("unit_price",   F.col("unit_price").cast("decimal(10,2)")) \
    .filter(F.col("product_id").isNotNull()) \
    .select(*biz_cols)

hash_cols = ["product_name", "category_id", "unit_cost", "unit_price", "status"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("products_silver").select("product_id", "row_hash")
new_versions = df.join(existing, on=["product_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("products_silver")
print(f"products_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: product_categories_silver =====")

biz_cols = ["category_id", "category_name", "category_group"]

df = spark.table("product_categories_bronze") \
    .withColumn("category_name",  F.trim(F.col("category_name"))) \
    .withColumn("category_group", F.trim(F.col("category_group"))) \
    .filter(F.col("category_id").isNotNull()) \
    .select(*biz_cols)

hash_cols = ["category_name", "category_group"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("product_categories_silver").select("category_id", "row_hash")
new_versions = df.join(existing, on=["category_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("product_categories_silver")
print(f"product_categories_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: orders_headers_silver =====")

biz_cols = ["order_id", "customer_id", "channel_id", "order_date", "order_status"]

df = spark.table("orders_headers_bronze") \
    .withColumn("order_status", F.trim(F.col("order_status"))) \
    .withColumn("order_date",   F.to_date(F.col("order_date"))) \
    .filter(F.col("order_id").isNotNull()) \
    .dropDuplicates(["order_id"]) \
    .select(*biz_cols)

hash_cols = ["customer_id", "channel_id", "order_date", "order_status"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("orders_headers_silver").select("order_id", "row_hash")
new_versions = df.join(existing, on=["order_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("orders_headers_silver")
print(f"orders_headers_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: line_items_silver =====")

biz_cols = ["line_id", "order_id", "product_id", "quantity", "unit_price"]

df = spark.table("line_items_bronze") \
    .withColumn("quantity",   F.col("quantity").cast("integer")) \
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)")) \
    .filter(F.col("line_id").isNotNull()) \
    .dropDuplicates(["line_id"]) \
    .select(*biz_cols)

hash_cols = ["order_id", "product_id", "quantity", "unit_price"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("line_items_silver").select("line_id", "row_hash")
new_versions = df.join(existing, on=["line_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("line_items_silver")
print(f"line_items_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("===== Processing: channels_silver =====")

biz_cols = ["channel_id", "channel_name", "channel_type", "region"]

df = spark.table("channels_bronze") \
    .withColumn("channel_name", F.trim(F.col("channel_name"))) \
    .withColumn("channel_type", F.trim(F.col("channel_type"))) \
    .withColumn("region",       F.trim(F.col("region"))) \
    .filter(F.col("channel_id").isNotNull()) \
    .select(*biz_cols)

hash_cols = ["channel_name", "channel_type", "region"]
df = df.withColumn("row_hash", F.md5(F.concat_ws("|", *[F.col(c).cast("string") for c in hash_cols]))) \
       .withColumn("silver_ingestion_timestamp", F.current_timestamp())

existing = spark.table("channels_silver").select("channel_id", "row_hash")
new_versions = df.join(existing, on=["channel_id", "row_hash"], how="left_anti")

row_count = new_versions.count()
new_versions.write.format("delta").mode("append").saveAsTable("channels_silver")
print(f"channels_silver: Inserted {row_count} new versions.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n========== Silver Processing Complete ==========")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

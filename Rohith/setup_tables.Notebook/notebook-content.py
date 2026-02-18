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

spark.sql("""
CREATE TABLE IF NOT EXISTS file_audit (
    table_name STRING,
    file_name STRING,
    processed_flag INT,
    processed_time TIMESTAMP
)
USING DELTA
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE customers_bronze (
# MAGIC     customer_id STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     segment STRING,
# MAGIC     email STRING,
# MAGIC     phone STRING,
# MAGIC     created_date STRING,
# MAGIC 
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE customer_addresses_bronze (
# MAGIC     address_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     street STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING,
# MAGIC     pincode STRING,
# MAGIC 
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE line_items_bronze (
# MAGIC     line_id STRING,
# MAGIC     order_id STRING,
# MAGIC     product_id STRING,
# MAGIC     quantity STRING,
# MAGIC     unit_price STRING,
# MAGIC     
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE orders_headers_bronze (
# MAGIC     order_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     channel_id STRING,
# MAGIC     order_date STRING,
# MAGIC     order_status STRING,
# MAGIC     
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE product_categories_bronze (
# MAGIC     category_id STRING,
# MAGIC     category_name STRING,
# MAGIC     category_group STRING,
# MAGIC     
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE products_bronze (
# MAGIC     product_id STRING,
# MAGIC     product_name STRING,
# MAGIC     category_id STRING,
# MAGIC     unit_cost STRING,
# MAGIC     unit_price STRING,
# MAGIC     status STRING,
# MAGIC 
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE channels_bronze (
# MAGIC     channel_id STRING,
# MAGIC     channel_name STRING,
# MAGIC     channel_type STRING,
# MAGIC     region STRING,
# MAGIC 
# MAGIC     _source_file_name STRING,
# MAGIC     _ingestion_timestamp TIMESTAMP
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --drop table customers_bronze;
# MAGIC --drop table customer_addresses_bronze;
# MAGIC --drop table products_bronze;
# MAGIC --drop table product_categories_bronze;
# MAGIC --drop table orders_headers_bronze;
# MAGIC --drop table line_items_bronze;
# MAGIC --drop table channels_bronze;
# MAGIC --drop table file_audit;
# MAGIC 
# MAGIC select * from file_audit

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS customers_silver (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        segment STRING,
        email STRING,
        phone STRING,
        created_date DATE,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_addresses_silver (
        address_id STRING,
        customer_id STRING,
        street STRING,
        city STRING,
        state STRING,
        country STRING,
        pincode INTEGER,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS products_silver (
        product_id STRING,
        product_name STRING,
        category_id STRING,
        unit_cost DECIMAL(10,2),
        unit_price DECIMAL(10,2),
        status STRING,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_categories_silver (
        category_id STRING,
        category_name STRING,
        category_group STRING,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS orders_headers_silver (
        order_id STRING,
        customer_id STRING,
        channel_id STRING,
        order_date DATE,
        order_status STRING,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS line_items_silver (
        line_id STRING,
        order_id STRING,
        product_id STRING,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS channels_silver (
        channel_id STRING,
        channel_name STRING,
        channel_type STRING,
        region STRING,
        row_hash STRING,
        silver_ingestion_timestamp TIMESTAMP
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

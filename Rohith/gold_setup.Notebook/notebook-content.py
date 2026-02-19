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
    CREATE TABLE IF NOT EXISTS watermark_table (
        table_name STRING,
        last_watermark TIMESTAMP
    ) USING DELTA
""")

# Initialize watermark for fact_sales
spark.sql("""
    INSERT INTO watermark_table VALUES ('fact_sales', cast('1900-01-01 00:00:00' as timestamp))
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key BIGINT,
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        segment STRING,
        email STRING,
        phone STRING,
        created_date DATE,
        effective_start_date TIMESTAMP,
        effective_end_date TIMESTAMP,
        is_current BOOLEAN
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key BIGINT,
        product_id STRING,
        product_name STRING,
        category_id STRING,
        category_name STRING,
        category_group STRING,
        unit_cost DECIMAL(10,2),
        unit_price DECIMAL(10,2),
        status STRING
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_channel (
        channel_key BIGINT,
        channel_id STRING,
        channel_name STRING,
        channel_type STRING,
        region STRING
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INT,
        date DATE,
        year INT,
        quarter INT,
        month INT,
        month_name STRING,
        day INT,
        day_of_week INT,
        day_name STRING,
        week_of_year INT,
        is_weekend BOOLEAN
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS fact_sales (
        line_id STRING,
        order_id STRING,
        product_key BIGINT,
        customer_key BIGINT,
        channel_key BIGINT,
        date_key INT,
        order_status STRING,
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2)
    ) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table fact_sales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

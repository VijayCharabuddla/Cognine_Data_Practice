# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f9615ba-f51a-4c46-af28-d03e2dd8e262",
# META       "default_lakehouse_name": "sales_poc",
# META       "default_lakehouse_workspace_id": "efa3bb07-4475-4ad7-9705-502ccae6ecd7",
# META       "known_lakehouses": [
# META         {
# META           "id": "6f9615ba-f51a-4c46-af28-d03e2dd8e262"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_cust = spark.sql("SELECT * FROM sales_poc.dbo.bronze_customers_transformed LIMIT 1000")
display(df_cust)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prod = spark.sql("SELECT * FROM sales_poc.dbo.bronze_products_transformed LIMIT 1000")
display(df_prod)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW gold_kpi_overview AS
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM silver_customers) AS TotalCustomers,
# MAGIC     (SELECT COUNT(*) 
# MAGIC      FROM silver_customers 
# MAGIC      WHERE public_status = 'Active') AS ActiveCustomers,
# MAGIC     (SELECT COUNT(*) FROM silver_products) AS TotalProducts,
# MAGIC     (SELECT COUNT(*) 
# MAGIC      FROM silver_products 
# MAGIC      WHERE product_status = 'Active') AS ActiveProducts;
# MAGIC select * from gold_kpi_overview;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

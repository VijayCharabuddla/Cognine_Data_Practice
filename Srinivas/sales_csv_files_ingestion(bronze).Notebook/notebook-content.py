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

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cust_df = spark.read.format("csv").option("header","true").load("Files/source/customers/part-00000-0fbf6ba6-eabb-4651-b137-ff37c3c4295a-c000.csv")
# df now is a Spark DataFrame containing CSV data from "Files/source/customers/part-00000-0fbf6ba6-eabb-4651-b137-ff37c3c4295a-c000.csv".
cust_df=cust_df.withColumn("signup_date",F.to_date(F.col("signup_date")))\
                .withColumn("last_login",F.to_date(F.col("last_login")))
display(cust_df)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prod_df = spark.read.format("csv").option("header","true").load("Files/source/products/part-00000-6d413a0e-3863-414c-bd84-8b4607b7a105-c000.csv")
# df now is a Spark DataFrame containing CSV data from "Files/source/products/part-00000-6d413a0e-3863-414c-bd84-8b4607b7a105-c000.csv".
prod_df=prod_df.withColumn("created_date",F.to_date(F.col("created_date")))\
                .withColumn("price",F.col("price").cast("int"))
display(prod_df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cust_df.write.mode("overwrite").saveAsTable("bronze_customers")
prod_df.write.mode("overwrite").saveAsTable("bronze_products")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count = cust_df.count()
mssparkutils.notebook.exit(row_count)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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


cust_data=[(101, "John doe", "JOHN@MAIL.COM", "india", "2023-01-10", "A", "2024-12-01"),
    (102, None, "mary@mail.com", "USA", "2023-02-15", "I", None),
    (103, "Ravi kumar", "ravi@mail.com", "india", None, "A", "2024-11-20"),
    (103, "Ravi kumar", "ravi@mail.com", "india", None, "A", "2024-11-20"),
    (104, "Ana Silva", "ANA@MAIL.COM", "brazil", "2023-03-05", "A", "2024-10-02"),
]

cust_df=spark.createDataFrame(cust_data,["customer_id","Full_name","email","country","signup_date","status","last_login"])

cust_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("Files/source/customers")

product_data= [
    ("P101","iphone 14","mobile",70000,"INR","A","2023-01-05"),
    ("P102","galaxy s22","mobile",65000,"INR","A","2023-02-10"),
    ("P103","macbook air","laptop",120000,"INR","I","2022-12-01"),
    ("P104","noise earbuds","accessories",3000,"INR","A","2023-03-15"),
    ("P105","Dell inspiron","LAPTOP",55000,"INR","A",None),
]

prod_df=spark.createDataFrame(product_data,["product_id","Prod_name","Category","price","currency","status ","created_date"])
prod_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("Files/source/products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

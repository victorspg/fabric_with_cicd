# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "eed7a883-5793-4b88-9eed-8f4ef9968e0f",
# META       "default_lakehouse_name": "Sales_LH",
# META       "default_lakehouse_workspace_id": "a08cdefc-c8c2-47db-b151-61a21af1b059",
# META       "known_lakehouses": [
# META         {
# META           "id": "eed7a883-5793-4b88-9eed-8f4ef9968e0f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/sales/sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sales/sales.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").saveAsTable("sales_schema.sales_bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

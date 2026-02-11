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
# META         },
# META         {
# META           "id": "650f0470-1a95-4792-b6f7-499a03443957"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC   "defaultLakehouse": {
# MAGIC     "name": { "parameterName": "lakehouse_name", "defaultValue": "<lakehouse_name>" },
# MAGIC     "id": { "parameterName": "lakehouse_id", "defaultValue": "<lakehouse_id>" },
# MAGIC     "workspaceId": { "parameterName": "workspace_id", "defaultValue": "<workspace_id>" },
# MAGIC   }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load(f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/Sales_LH.Lakehouse/Files/sales/sales.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS sales_schema")

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

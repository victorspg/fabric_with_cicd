# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b125be47-e0cb-4d9e-a92a-c2148460683f",
# META       "default_lakehouse_name": "FoodSales_LH",
# META       "default_lakehouse_workspace_id": "2f495494-a4a9-45b2-8967-2a2d661e2d3b",
# META       "known_lakehouses": [
# META         {
# META           "id": "b125be47-e0cb-4d9e-a92a-c2148460683f"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

environment = "NO_PARAMETER_PASSED"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM FoodSales_LH.dbo.publicholidays LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Environment: {environment}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e153195e-0275-4739-b157-cc7320b85f89",
# META       "default_lakehouse_name": "metachat_bronze",
# META       "default_lakehouse_workspace_id": "fd5b14a2-d05d-4a10-88cf-16b6fd041b96",
# META       "known_lakehouses": [
# META         {
# META           "id": "e153195e-0275-4739-b157-cc7320b85f89"
# META         },
# META         {
# META           "id": "6ea47382-af51-4a80-a44e-e4a620acf7b2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Convert all bronze schemas to gold csv

# MARKDOWN ********************

# ### Import all Packages 

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import json
import random
from datetime import datetime, timedelta


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder.appName("Group_all_tables").getOrCreate()
# list all the tables
tables_df = spark.sql("SHOW TABLES IN metachat_bronze.open_metadata")
# get all table_names
table_names_list = [row["tableName"] for row in tables_df.select("tableName").collect()]
table_names_list = [table for table in table_names_list if 'omd' not in table.lower()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Convert a table from Delta to csv

# MARKDOWN ********************

# 
#     Queries a Delta table and converts the result to a Pandas DataFrame.
# 
#     Parameters:
#     - table (str): The table name to query.
# 
#     Returns:
#     - pd.DataFrame: A Pandas DataFrame with up to 1000 rows from the specified Delta table.
# 
#     Example:
#     --------
#     table_data = delta_csv("customer_data")
#     print(table_data.head())
#     """

# CELL ********************

def delta_csv(table):
    query = f"SELECT * FROM metachat_bronze.open_metadata.{table} LIMIT 1000"
    df = spark.sql(query)
    df = df.toPandas()
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Load tables as csv's

# MARKDOWN ********************

# 
#     Loads datasets as CSV files from a list of table names.
# 
#     This function converts each table in the `tables` list to a CSV file and saves it to a specified Azure Fabric storage path.
# 
#     Parameters:
#     - tables (list): A list of table names to load, convert, and save.
# 
#     Function Workflow:
#     1. For each table name, calls `delta_csv(table)` to load it as a DataFrame.
#     2. Removes the prefix `bronze_` from the table name for storage purposes.
#     3. Initializes a Spark session and converts the DataFrame to Spark format.
#     4. Defines a storage path and saves the DataFrame as a CSV file at that path.
# 
#     Returns:
#     - None: Outputs a confirmation message for each table loaded.
# 
#     Example:
#     --------
#     tables = ["bronze_customers", "bronze_orders"]
#     load_files(tables)
#     # Output: table customers loaded ...
#     #         table orders loaded ...
#     """

# CELL ********************

# load the datasets as csv

def load_files(tables):

    for table in tables:
        df = delta_csv(table)
        table = table.replace("bronze_","")

        # Initialize a Spark session (creates a new Spark context if it doesn't already exist)
        sc = SparkSession.builder.appName('DataDump').getOrCreate()

        # Convert the Pandas DataFrame into a Spark DataFrame
        spark_df = sc.createDataFrame(df)
        spark_df.dropna()
        table_name = f'gold_{table}'
        # Write the DataFrame to a Delta table in overwrite mode

        # Define the storage path
        output_path = f"abfss://metachat@onelake.dfs.fabric.microsoft.com/metachat_gold.Lakehouse/Files/ai-search/{table_name}.csv"

        # Save DataFrame as Parquet
        df.to_csv(output_path,index=False)

        print(f'table {table} loaded ...')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load all data
load_files(table_names_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

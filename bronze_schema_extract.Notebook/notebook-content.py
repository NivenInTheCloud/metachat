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
# META       "default_lakehouse_workspace_id": "fd5b14a2-d05d-4a10-88cf-16b6fd041b96"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Extract all schema data

# MARKDOWN ********************

# #### Import Packages

# CELL ********************

import requests
import pandas as pd
import json
import os
from time import sleep
from pyspark.sql import SparkSession


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Raw data Extraction for the API

# MARKDOWN ********************

# 
#     Sends a GET request to the specified API endpoint to retrieve metadata in JSON format.
#     The request is authenticated using a Bearer token, and errors are handled and reported.
# 
#     Parameters:
#     - path (str): The API endpoint path (e.g., '/metadata').
#     - url (str, optional): The base API URL (default: 'http://13.245.98.138:8585/api/v1').
# 
#     Returns:
#     - dict: Extracted metadata as a dictionary (parsed from the JSON response).
#     """

# CELL ********************

# extract raw metadata
def data_extract(path,url='http://13.245.98.138:8585/api/v1'):
        headers = {
        'Content-Type':'application/json',
        'Authorization': 'Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3Mjg0ODI1NjgsImV4cCI6bnVsbH0.KXYmNaV-aJugdxg5AZY6yfLLdvEnmJVvcttzQE-n3zbZWwAfhljAQKn2zigPHsyMMLBQmwj9o1w8yGlqIR1nNcVwGTdu9lqNZ4Ph8HO_5z7vgMIKl7WIwTw9oCHVLnF25pD2fofFe9J7RNrkr54fmrN84wMckoCZHLGiZi5TBWo1KaSUJoMikcgbAw0jE7EGfOW8zysRKLXuwwhaWMcrtKrGKpNL2DCSM1SxqMCfOZVyfXKE86xVOoAaKxbgvTJo_iJ1p9ZMPdCu633NAorIjAcr0sohsahDWZSRHq3f7cY0W69qZJugjuxqyewsWxcTzKWHkJMeuZduqNGOcHxEUw'}
        api_endpoint = url + path

        try:
            response = requests.get(api_endpoint, headers=headers, verify=False)  # Set verify=True for production

            response.raise_for_status() 

            # Process the response 
            sleep(2)
            metadata = response.json()  # Adjust based on the expected response format
            return metadata

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save as Delta Func

# MARKDOWN ********************

# 
#     This function loads a DataFrame into a Delta Lake table in a Spark environment.
# 
#     Parameters:
#     service (str): The service name to use in the table name (e.g., 'service_name').
#     schema (str): The schema or database in which to save the table (e.g., 'schema_name').
#     table (str): The target table name (e.g., 'target_table').
#     df (pandas.DataFrame): The DataFrame to be loaded into the Delta table.
# 
# """


# CELL ********************

# load the datasets as delta

def data_load(layer,service,table,schema,df):

    # Initialize a Spark session (creates a new Spark context if it doesn't already exist)
    sc = SparkSession.builder.appName('DataDump').getOrCreate()

    # Convert the Pandas DataFrame into a Spark DataFrame
    spark_df = sc.createDataFrame(df)

    table_name = f'open_metadata.{layer}_{table}_{service}_{schema}'
     # Write the DataFrame to a Delta table in overwrite mode

    spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f'table {table} loaded ...')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract Metadata Info (Postgres)

# MARKDOWN ********************

# 
#     Creates a DataFrame from the 'columns' list present in the provided JSON data.
# 
#     Parameters:
#     - json_data (dict): A dictionary containing the 'columns' key, which holds a list of column data.
# 
#     Returns:
#     - pd.DataFrame: A DataFrame created from the 'columns' list in the input JSON data.
#     """

# CELL ********************


# Function to create DataFrame from the columns in JSON
def create_columns_dataframe(json_data):
    # Extract the 'columns' list from the JSON
    columns = json_data.get("columns", [])
    
    # Convert columns list into a DataFrame
    columns_df = pd.DataFrame(columns)
    
    return columns_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
#     Extracts and processes data for a list of tables in the public schema of a PostgreSQL database.
# 
#     This function loops over a predefined list of table names in the public schema, extracts metadata 
#     for each table using the `data_extract` function, and creates a DataFrame from the table's columns 
#     using the `create_columns_dataframe` function. The resulting DataFrames are collected and returned 
#     as a list.
# 
#     Parameters:
#     - url (str): The base URL for the API endpoint (passed to `data_extract`).
# 
#     Returns:
#     - list of pd.DataFrame: A list of DataFrames representing the column data for each table in the public schema.
#     """

# CELL ********************

def extract_public_schema_data(public,path,service):
    
    # Loop over each table name in the public schema
    for table_name in public:
      
        json_data = data_extract(path=f'/tables/name/{path}.{table_name}/')
        
        df_columns = create_columns_dataframe(json_data)

        data_load('bronze',service,table_name,'schema',df_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Call the extraction function & load data in delta lake

# CELL ********************

public = [
        "cards",
        "users",
        "user_behavior",
        "cellular_transactions",
        "credit_card_transactions",
        "customer_data",
        "device_metadata",
        "distinct_operating_systems",
        "electronics_table",
        "pg_stat_statements",
        "pg_stat_statements_info",
        "revenue"
    ]
extract_public_schema_data(public,path='postgres.mydatabase.public',service='postgres')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Extract DynamoDB Schemas

# CELL ********************

public = [
    "insurance_data",
    "usage_data"]
extract_public_schema_data(public,path='dynamodb.default.default',service='dynamodb')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract Athena

# CELL ********************

public = [
    "network_traffic_logs"]
extract_public_schema_data(public,path='athena.openmetadata.openmetadata',service='athena')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# #### Extract all the team information

# MARKDOWN ********************

# #### Import all packages 

# CELL ********************

import requests
import pandas as pd
import json
import os
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Raw data Extraction for the API

# MARKDOWN ********************

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

#  
#     This function loads a DataFrame into a Delta Lake table in a Spark environment.
# 
#     Parameters:
#     service (str): The service name to use in the table name (e.g., 'service_name').
#     schema (str): The schema or database in which to save the table (e.g., 'schema_name').
#     table (str): The target table name (e.g., 'target_table').
#     df (pandas.DataFrame): The DataFrame to be loaded into the Delta table.


# CELL ********************

# load the datasets as delta

def data_load(layer,service,table,schema,raw_json):

    # Initialize a Spark session (creates a new Spark context if it doesn't already exist)
    sc = SparkSession.builder.appName('DataDump').getOrCreate()

    # Convert the Pandas DataFrame into a Spark DataFrame
    spark_df = sc.createDataFrame([raw_json])

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

# #### Load data into delta tables

# CELL ********************


# load teams and user data
users_raw = data_extract(path=f'/users/')
team_raw = data_extract(path=f'/teams/')

# Save the DataFrame to OneLake
data_load(layer='bronze',service='omd',table='users',schema='data',raw_json=users_raw)
data_load(layer='bronze',service='omd',table='teams',schema='data',raw_json=team_raw)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

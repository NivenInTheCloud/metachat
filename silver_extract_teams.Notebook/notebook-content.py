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
# META           "id": "66f92996-bd1e-4af5-b23b-af8ec074559b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Extract the team information

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM metachat_silver.open_metadata.silver_domain_teams_omd_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Transform users and teams into a dataframe

# MARKDOWN ********************

# #### Import all packages

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import json
import pandas as pd
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract all user data from the lakehouse

# CELL ********************

df_users = spark.sql("SELECT * FROM metachat_bronze.open_metadata.bronze_users_omd_data LIMIT 1000")

df_teams = spark.sql("SELECT * FROM metachat_bronze.open_metadata.bronze_teams_omd_data LIMIT 1000")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Convert spark to json for dataframes

# MARKDOWN ********************

# 
#     Extracts and converts the data from the 'data' column of a Spark DataFrame into a JSON object.
# 
#     Parameters:
#     - df (pyspark.sql.DataFrame): A DataFrame with a 'data' column containing dictionaries.
# 
#     Returns:
#     - dict: The first dictionary from the 'data' column, parsed from a JSON string.
# 
#     Example:
#     --------
#     df = spark.createDataFrame([{"data": {"name": "John", "age": 30}}])
#     result = spark_tojson(df)
#     print(result)  # Output: {'name': 'John', 'age': 30}
#     """

# CELL ********************

def spark_tojson(df):

    # Collect the data from the 'data' column
    users = df.select('data').collect()
    
    # Convert the list of Row objects into a list of dictionaries
    users_data = [row['data'] for row in users]

    # Serialize the list of dictionaries to JSON
    json_data = json.dumps(users_data)

    # Parse the JSON string back into a Python object (list of dictionaries)
    json_data = json.loads(json_data)[0]

    return json_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
#     Transforms a Spark DataFrame into a Pandas DataFrame by extracting specified attributes.
# 
#     Parameters:
#     - att (list): List of attribute names to extract.
#     - df (pyspark.sql.DataFrame): Input Spark DataFrame.
# 
#     Returns:
#     - pd.DataFrame: A Pandas DataFrame with the selected attributes.
# 
#     Example:
#     --------
#     attributes = ['name', 'age']
#     df = spark.createDataFrame([{"data": {"name": "John", "age": 30}}])

# CELL ********************

def team_transform(att,df):
    # store all dics
    data_store = []
    # load data per item
    loads = {}
    # call json
    json_data =spark_tojson(df)

    for row in json_data:
        loads = {}
        for item in att:
            loads[item] = row.get(item,None)
        data_store.append(loads)
    user_main = pd.DataFrame(data_store)
    return user_main

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def stringto_json(data):
    cleaned_data = data.replace('{', '').replace('}', '').replace('[', '').replace(']', '')
    split_data = cleaned_data.split(',')

    # Step 2: Create a list to hold dictionaries
    result = []

    # Step 3: Process each part of the split data and create key-value pairs
    temp_dict = {}
    for part in split_data:
        # Skip empty parts or malformed parts
        if '=' in part:
            key_value = part.split('=')
            if len(key_value) == 2:  # Ensure it's a valid key-value pair
                key, value = key_value
                temp_dict[key.strip()] = value.strip()
    result.append(temp_dict)
    # Add the final dictionary to the result list
    return  result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#     Converts a string of key-value pairs into a list of dictionaries.
# 
#     Parameters:
#     - data (str): A string with key-value pairs, separated by `=`.
# 
#     Returns:
#     - list: A list containing one dictionary with the parsed key-value pairs.
# 
#     Example:
#     --------
#     data = "{name=John, age=30, city=New York}"
#     result = stringto_json(data)
#     print(result)  # Output: [{'name': 'John', 'age': '30', 'city': 'New York'}]
#     """

# MARKDOWN ********************

# ### Extract Domains for teams and users

# MARKDOWN ********************

#    
#     Loads a Pandas DataFrame as a Delta table in Spark with a specified name.
# 
#     Parameters:
#     - service (str): Name of the service or database.
#     - table (str): Table name to create or overwrite.
#     - schema (str): Schema name for the table.
#     - df (pd.DataFrame): Data to load.
# 
#     Outputs:
#     - None: Prints a message confirming the table load.
#     
#     Example:
#     --------
#     data_load("finance", "transactions", "public", pandas_df)
#     # Output: table finance_public_transactions loaded ...
#     


# CELL ********************

def extract_domain_data(data):
    # Initialize a list to hold all data frames
    extracted_info = []

    # Loop through each item in the data list
    for i in range(len(data)):
        try:
            # Extract domain information based on the action type
          
            domains = data[i]['domains']
            domains = stringto_json(domains)
            if domains:  # Ensure domains data is not None
                # domains = json.loads(domains) if isinstance(domains, str) else domains
                df = pd.DataFrame(domains)
                df['Action_ID'] = data[i].get('id')
                extracted_info.append(df)  # Append the DataFrame to the list
        except Exception as e:
            print(f"No domain item at index {i}: {e}")  # Handle exceptions

    # Concatenate all data frames into a single DataFrame
    return pd.concat(extracted_info, ignore_index=True) if extracted_info else pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load files to delta lake

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

keys_list = [
    'id',
    'teamType',
    'name',
    'fullyQualifiedName',
    'description',
    'version',
    'updatedAt',
    'href',
    'isJoinable',
    'deleted'
]

team_info = team_transform(att=keys_list,df = df_teams)
data_load(layer='silver',service='omd',table='team_info',schema='data',df=team_info)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load Data into delta tables

# MARKDOWN ********************

# ##### Load Team and User tables

# CELL ********************

#### Load F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load the datasets as delta

def data_load(layer,service,table,schema,df):

    # Initialize a Spark session (creates a new Spark context if it doesn't already exist)
    sc = SparkSession.builder.appName('DataDump').getOrCreate()

    # Convert the Pandas DataFrame into a Spark DataFrame
    spark_df = sc.createDataFrame(df)

    table_name = f'metachat_silver.open_metadata.{layer}_{table}_{service}_{schema}'
     # Write the DataFrame to a Delta table in overwrite mode

    spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f'table {table} loaded ...')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Extract & Load Domain Info

# MARKDOWN ********************

# ###### Extra user domain data

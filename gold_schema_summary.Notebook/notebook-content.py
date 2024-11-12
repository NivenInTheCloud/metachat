# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6ea47382-af51-4a80-a44e-e4a620acf7b2",
# META       "default_lakehouse_name": "metachat_gold",
# META       "default_lakehouse_workspace_id": "fd5b14a2-d05d-4a10-88cf-16b6fd041b96",
# META       "known_lakehouses": [
# META         {
# META           "id": "6ea47382-af51-4a80-a44e-e4a620acf7b2"
# META         },
# META         {
# META           "id": "e153195e-0275-4739-b157-cc7320b85f89"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Get all schema infomation

# MARKDOWN ********************

# ### Import all packages 

# CELL ********************

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

# MARKDOWN ********************

# ### Extract all packages

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder.appName("Group_all_tables").getOrCreate()
# list all the tables
tables_df = spark.sql("SHOW TABLES IN metachat_bronze.open_metadata")
# Display all tables
tables_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Clean the table and filter out unwanted tables

# MARKDOWN ********************

# 
#     Splits and cleans data in the Spark DataFrame by extracting service and schema names from the 'tableName' column.
# 
#     Parameters:
#     - df (pyspark.sql.DataFrame): Input Spark DataFrame containing a 'tableName' column.
# 
#     Returns:
#     - pyspark.sql.DataFrame: A new DataFrame with two additional columns:
#       - 'Schema_name': Extracted schema name, cleaned of service identifiers.
#       - 'Service': The specific service name (e.g., dynamo, postgres, athena) extracted from 'tableName'.
# 
#     Function Workflow:
#     1. Extracts schema names by splitting `tableName` and removing any `_schema` suffixes.
#     2. Filters specific services (dynamo, postgres, athena) and extracts the service name.
#     3. Removes service name suffixes from schema names for further cleaning.
# 
#     Example:
#     --------
#     cleaned_df = clean_gold(input_df)
#     cleaned_df.show()
#     # Output columns: tableName, Schema_name, Service
#     """

# CELL ********************

# Split into service and table

def clean_gold(df) :

    tables_df = df.withColumn("Schema_name", f.split(df["tableName"], "_", 2).getItem(1))
    tables_df = tables_df.withColumn("Schema_name", f.regexp_replace(f.col("Schema_name"), "_schema", ""))
    # target specific databases
    databases = ['dynamo', 'postgres', 'athena']
    regex_pattern = '|'.join(databases)  # This creates a regex pattern 'dynamo|postgres|athena'
    # Create the 'services' column by extracting the service name from 'tableName'
    tables_df = tables_df.withColumn(
    'Service', 
    f.regexp_extract(f.col('tableName'), "(?i)(dynamodb|athena|postgres)", 1))
    #Extract schema_names
    tables_df = tables_df.withColumn(
    "Schema_name",
    f.regexp_replace(f.col("Schema_name"), "(?i)(_dynamodb|_athena|_postgres)", ""))
    return tables_df

main_table = clean_gold(tables_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Link Description Table

# MARKDOWN ********************

# 
#     Create a DataFrame with schema names and their respective descriptions.
# 
#     :param schema_names: List of schema names
#     :param descriptions: List of descriptions corresponding to schema names
#     :return: PySpark DataFrame with two columns - 'Schema_name' and 'Table_Description'
#     """

# CELL ********************

# listed tables
def table_disc(schema_names,description):
        columns = ["Table_Description","Schema_name"]
        # Create DataFrame
        df = spark.createDataFrame(list(zip(description,schema_names)), schema=columns)
        return df



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Join descriptions and main table

# CELL ********************

schema_names = [
            "network_traffic_logs",
            "insurance_data",
            "usage_data",
            "cards",
            "cellular_transactions",
            "credit_card_transactions",
            "customer_data",
            "device_metadata",
            "distinct_operating_systems",
            "electronics_table",
            "pg_stat_statements",
            "pg_stat_statements_info",
            "revenue",
            "user_behavior",
            "users"
        ]

# Define the descriptions

description = [
            "athena_schema_network_traffic_logs: This schema is designed for analyzing network traffic data, providing insights into the flow and patterns of information across the network. It helps identify usage trends, pinpoint anomalies, and support security monitoring. The schema allows for examining attributes like IP addresses, connection times, and data transfer volumes to understand network health, optimize infrastructure, and detect possible security threats.",
            
            "dynamodb_schema_insurance_data: This schema manages data specifically tied to insurance operations, capturing details around policy management, claims processing, and customer risk profiles. It enables efficient access to structured insurance data, supporting tasks like customer inquiries, claims assessments, and regulatory reporting by providing a central repository for policy-related information.",
            
            "dynamodb_schema_usage_data: Dedicated to tracking product or service usage metrics, this schema records data that reflects how customers or devices interact with various offerings. It enables detailed usage analysis, supporting business intelligence functions that monitor customer engagement, detect trends in consumption, and help in optimizing product offerings based on usage patterns.",
            
            "postgres_schema_cards: This schema handles all aspects of card-related data, supporting systems that need to manage various types of cards such as credit, debit, or membership cards. It stores essential details about card issuance, status, usage, and potentially financial transactions, ensuring secure and efficient management of cardholder information and aiding in risk management and transaction monitoring.",
            
            "postgres_schema_cellular_transactions: Designed to store information on cellular network activities, this schema captures transactional data associated with telecom services, including calls, messaging, and data usage. It supports billing, usage analytics, and helps in understanding customer activity patterns, network demand, and potential service issues within the telecom infrastructure.",
            
            "postgres_schema_credit_card_transactions: Focused on financial transactions involving credit cards, this schema is crucial for recording and analyzing purchase activities. It supports financial reconciliation, transaction history tracking, and fraud detection efforts by organizing detailed records of credit card transactions.",
            
            "postgres_schema_customer_data: This schema contains core customer information such as profiles, demographics, and account details. It serves as the foundation for customer relationship management (CRM) activities, enabling data-driven personalization, service optimization, and providing valuable insights for marketing and customer service initiatives.",
            
            "postgres_schema_device_metadata: This schema organizes metadata about devices within an organization’s ecosystem. It contains device attributes, specifications, and configurations, essential for device lifecycle management, compliance with technology standards, and troubleshooting. This schema is critical for inventory management and ensuring compatibility across devices.",
            
            "postgres_schema_distinct_operating_systems: This schema keeps a catalog of distinct operating systems in use within the organization’s environment, enabling effective asset tracking and security management. It supports system maintenance and security patching by providing clear visibility into the operating systems in use and their distribution across the network.",
            
            "postgres_schema_electronics_table: Managing data on electronics and related equipment, this schema acts as an inventory repository, organizing information on various electronic items, their specifications, and availability. It supports asset management, procurement, and helps maintain an up-to-date record of electronic resources.",
            
            "postgres_schema_pg_stat_statements: This schema is specialized for gathering and analyzing SQL query performance statistics within PostgreSQL. It collects data on query executions, allowing database administrators to monitor resource usage and optimize query performance to enhance the overall efficiency of the database system.",
            
            "postgres_schema_pg_stat_statements_info: Complementing the basic statistics schema, this schema offers advanced insights on SQL performance, such as locking and execution efficiency. It enables fine-grained performance analysis, providing detailed diagnostic information to improve database operations and minimize resource bottlenecks.",
            
            "postgres_schema_revenue: This schema organizes revenue-related data to facilitate financial tracking and analysis. It supports financial reporting, profitability analysis, and budgeting by organizing revenue streams, income categories, and temporal revenue data, aiding in strategic planning and financial health monitoring.",
            
            "postgres_schema_user_behavior: This schema captures data related to user interactions with products or systems, documenting behavior such as actions taken, engagement patterns, and feature usage. It supports user experience analysis, helping teams understand how users interact with their services, what features are most valuable, and where improvements can be made.",
            
            "postgres_schema_users: Central to user account management, this schema holds data on individual users, including profile details, roles, and permissions. It supports identity and access management, ensuring secure user authentication, role-based access control, and maintains records of user activity for compliance and auditing purposes."
        ]

desc_table = table_disc(schema_names,description)

# join the main table and description table

main_table = main_table.join(desc_table,on="Schema_name",how="full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Create a Spark DataFrame to link schema names with their corresponding domains.

# MARKDOWN ********************

# 
#     Parameters:
#     - schema_names (list): A list of schema names.
#     - departments (list): A list of department names representing domains, aligned with schema names.
# 
#     Returns:
#     - pyspark.sql.DataFrame: A DataFrame with columns "Schema_name" and "Domain", pairing each schema with a department.
# 
#     Example:
#     --------
#     schema_names = ["finance_schema", "marketing_schema"]
#     departments = ["Finance", "Marketing"]
#     domain_df = domain(schema_names, departments)
#     domain_df.show()
#     # Out

# CELL ********************

#### Link Domain info

# Create DataFrame
def domain(schema_names,departments):
    columns = ["Schema_name","Domain"]
    df = spark.createDataFrame(list(zip(schema_names,departments)), schema=columns)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_tables = [
    "network_traffic_logs",
    "insurance_data",
    "usage_data",
    "cards",
    "cellular_transactions",
    "credit_card_transactions",
    "customer_data",
    "device_metadata",
    "distinct_operating_systems",
    "electronics_table",
    "pg_stat_statements",
    "pg_stat_statements_info",
    "revenue",
    "user_behavior",
    "users"
]


departments = [
    "Network Infrastructure",
    "Regulatory & Compliance",
    "Marketing",
    "Finance",
    "Finance",
    "Finance",
    "Marketing",
    "Regulatory & Compliance",
    "Finance",
    "Finance",
    "Finance",
    "Finance",
    "Finance",
    "Marketing",
    "Marketing"
]

domain_table = domain(data_tables,departments)
main_table = main_table.join(domain_table,on="Schema_name",how="full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get table owners

# MARKDOWN ********************

# 
#     Creates a Spark DataFrame to associate schema names with their respective owners.
# 
#     Parameters:
#     - schema_names (list): A list of schema names.
#     - owners (list): A list of owner names, aligned with the schema names.
# 
#     Returns:
#     - pyspark.sql.DataFrame: A DataFrame with columns "Schema_name" and "Owner", linking each schema to its owner.
# 
#     Example:
#     --------
#     schema_names = ["finance_schema", "marketing_schema"]
#     owners = ["John Doe", "Jane Smith"]
#     owner_df = owner(schema_names, owners)
#     owner_df.show()
#     # Output columns: Schema_name, Owner
#     """

# CELL ********************

def owner(schema_names,owners):
    columns = ["Schema_name","Owner"]
    df = spark.createDataFrame(list(zip(schema_names,owners)), schema=columns)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


schema_names = ['network_traffic_logs',
 'insurance_data',
 'usage_data',
 'cards',
 'cellular_transactions',
 'credit_card_transactions',
 'customer_data',
 'device_metadata',
 'distinct_operating_systems',
 'electronics_table',
 'pg_stat_statements',
 'pg_stat_statements_info',
 'revenue',
 'user_behavior',
 'users']
owners = [
    "David", "Alex", "Bailey", "Karabo", 
    "Casey", "John", "James", "Jordan", 
    "Mellisa", "David", "Mellisa", "David", 
    "Morgan", "Sophie", "Riley", "David"]

owner_table = owner(schema_names,owners)

main_table = main_table.join(owner_table,on="Schema_name",how="full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get the time a table was updated

# MARKDOWN ********************

# 
#     Parameters:
#     - days (int): The number of days in the past to define the date range.
# 
#     Returns:
#     - list: A list of 15 random Unix timestamps (float) between the start and end date range.
# 
#     Function Workflow:
#     1. Defines a date range based on the current date and the number of days specified.
#     2. Generates 15 random timestamps within this range.
#     3. Converts each timestamp to Unix format and adds it to the list.
# 
#     Example:
#     --------
#     timestamps = last_updated(30)
#     print(timestamps)
#     # Output: [1672489587.23, 1672531047.84, ...]  # List of Unix timestamps
#     """

# CELL ********************

def last_updated(days):
  

    # Define the date range
    end_date = datetime.today()
    start_date = end_date - timedelta(days)
    # Generate a random timestamp between the start and end date
    date_store = []
    for i in range(15): 
        random_timestamp = start_date + (end_date - start_date) * random.random()
        unix_timestamp = random_timestamp.timestamp()
        date_store.append(unix_timestamp)
    return date_store


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_up = last_updated(8)

schema_names = ['network_traffic_logs',
 'insurance_data',
 'usage_data',
 'cards',
 'cellular_transactions',
 'credit_card_transactions',
 'customer_data',
 'device_metadata',
 'distinct_operating_systems',
 'electronics_table',
 'pg_stat_statements',
 'pg_stat_statements_info',
 'revenue',
 'user_behavior',
 'users']

columns = ["Schema_name","Last_Updated"]
df = spark.createDataFrame(list(zip(schema_names,last_up)), schema=columns)
main_table = main_table.join(df,on="Schema_name",how="full")
main_table = main_table.withColumn('Last_Updated', f.from_unixtime('Last_Updated'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Create Email Column

# CELL ********************

main_table = main_table.withColumn('Email',f.concat(f.lower('Owner'),f.lit('@open-metadata.org')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Rename all column and save the Gold data for open_search

# CELL ********************

main_table = main_table.withColumnRenamed('tableName','Delta_Name')\
          .withColumnRenamed('Schema_name','Table_Name')\
          .select('Table_Name','Email',\
          'Service','Table_Description','Domain','Owner','Last_Updated').dropna(subset='Owner')
main_table = main_table.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Load all the tables to abfss

# CELL ********************

abfss_path = "abfss://metachat@onelake.dfs.fabric.microsoft.com/metachat_gold.Lakehouse/Files/ai-search/gold_all_tables.csv"
# write data to ai-search
main_table.to_csv(abfss_path,index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

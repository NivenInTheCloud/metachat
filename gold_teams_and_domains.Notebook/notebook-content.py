# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4ab4d96c-f633-40ac-ac25-d34532b783c6",
# META       "default_lakehouse_name": "metachat_bronze_dev",
# META       "default_lakehouse_workspace_id": "ed6a4915-82f8-42fd-88c7-4a53514ee1f4",
# META       "known_lakehouses": [
# META         {
# META           "id": "4ab4d96c-f633-40ac-ac25-d34532b783c6"
# META         },
# META         {
# META           "id": "6ea47382-af51-4a80-a44e-e4a620acf7b2"
# META         },
# META         {
# META           "id": "66f92996-bd1e-4af5-b23b-af8ec074559b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Evaluate the users and teams tables

# MARKDOWN ********************

# ##### Import Packages

# CELL ********************

# get functions
from pyspark.sql import functions as f

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Sorce users info

# MARKDOWN ********************

# #### Extract User Info

# MARKDOWN ********************

# 
#     Transforms and cleans user data by merging two datasets and assigning departments based on user names.
#     
#     This function performs the following steps:
#     1. Extracts user data from the table specified by `table_1` and domain data from `table_2`.
#     2. Renames relevant columns to align user and domain data.
#     3. Joins the user data with the domain data on 'Action_ID'.
#     4. Maps specific user names to department names (e.g., 'David' to 'Finance').
#     5. Fills missing 'User_Name' values with 'David' and drops rows without a valid 'displayName'.
#     6. Selects key columns and renames 'displayName' to 'department'.
#     
#     Args:
#         table_1 (str): The name of the table containing user information.
#         table_2 (str): The name of the table containing domain information.
#     
#     Returns:
#         DataFrame: A DataFrame containing cleaned user data with columns 'User_Name', 'department', and 'email'.
#     """

# CELL ********************

def gold_users(table_1,table_2):
    
    # extract user info
    user_query = f"SELECT * FROM metachat.metachat_silver.open_metadata.{table_1} LIMIT 1000"
    df_users = spark.sql(user_query)
    # extract team info 
    df_users = df_users.withColumnRenamed('id','Action_ID').withColumnRenamed('displayName','User_Name')
    domain_query =f"SELECT * FROM metachat.metachat_silver.open_metadata.{table_2} LIMIT 1000"
    df_use_domains = spark.sql(domain_query)
    # combine two datasets
    df_users = df_use_domains.drop('fullyQualifiedName','description').join( df_users,on='Action_ID',how='outer')
    # clean the table
    df_users = df_users.withColumn('displayName',\
    f.when(f.col('User_Name') == 'David', 'Finance')\
    .when(f.col('User_Name') == 'John', 'Marketing')\
    .when(f.col('User_Name') == 'Karabo', 'Network infrastructure')\
    .when(f.col('User_Name') == 'James', 'Marketing')\
    .when(f.col('User_Name') == 'Mellisa', 'Finance').otherwise(f.col('displayName')))
    df_users = df_users.fillna({"User_Name":"David"})
    df_users = df_users.dropna(subset='displayName')
    df_users = df_users.select('User_Name','displayName','email').\
            withColumnRenamed('displayName', 'department')
    return df_users
df_users = gold_users(table_1='silver_user_info_omd_data',table_2='silver_domain_users_omd_data ')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Team information

# MARKDOWN ********************

#     Combines team user information and domain data, cleans, and renames columns for final output.
# 
#     Parameters:
#     - table_1 (str): The name of the first table containing user info.
#     - table_2 (str): The name of the second table containing domain information for users.
# 
#     Returns:
#     - pyspark.sql.DataFrame: A DataFrame with cleaned and merged team user information and domain data.
# 
#     Function Workflow:
#     1. Queries data from the two provided tables using Spark SQL.
#     2. Renames columns in the user info dataset (`table_1`).
#     3. Joins the user data with domain information from `table_2` based on `Action_ID`.
#     4. Drops rows with null values in the 'type' column.
#     5. Cleans the data by renaming columns for clarity and consistency.
# 
#     Example:
#     --------
#     df_teams = gold_teams("silver_team_info_omd_data", "silver_domain_teams_omd_data")
#     df_teams.show()
#     # Output: Cleaned and merged DataFrame with team user and domain data.
#     """

# CELL ********************

def gold_teams(table_1,table_2):
    
    # extract team user info
    q1 = f"SELECT * FROM metachat.metachat_silver.open_metadata.{table_1} LIMIT 1000"
    df_teams= spark.sql(q1)
    df_teams = df_teams.withColumnRenamed('id','Action_ID').withColumnRenamed('displayName','User_Name')
    # extract domain info for users
    q2 = f"SELECT * FROM metachat.metachat_silver.open_metadata.{table_2} LIMIT 1000"
    df_team_domains = spark.sql(q2)
    # combine two datasets
    df_team_domains = df_team_domains.drop('fullyQualifiedName','description',)
    df_teams = df_teams.join( df_team_domains,on='Action_ID',how='outer')
    # drop nulls
    df_teams = df_teams.dropna(subset='type')

    # clean the data
    df_teams = df_teams.select('teamType','fullyQualifiedName','description','displayName'\
    ).withColumnRenamed('teamType', 'team_type') \
    .withColumnRenamed('fullyQualifiedName', 'teamname') \
    .withColumnRenamed('displayName', 'department') \
    .withColumnRenamed('description', 'team_description')
    return df_teams
df_teams = gold_teams(table_1='silver_team_info_omd_data',table_2='silver_domain_teams_omd_data ')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Join the two datasets

# CELL ********************

df_teams_final = df_teams.join(df_users,on='department',how='outer')
df_teams_final = df_teams_final.withColumnRenamed('department','Domain')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Save the gold table

# CELL ********************

# save as a csv 
path = 'abfss://metachat@onelake.dfs.fabric.microsoft.com/metachat_gold.Lakehouse/Files/ai-search/gold_teams.csv'
df_teams_final.toPandas().to_csv(path,index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

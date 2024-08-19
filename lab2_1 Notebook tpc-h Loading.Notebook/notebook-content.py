# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d8b07883-20af-4902-95af-b7c0932273ee",
# META       "default_lakehouse_name": "lh_tpch",
# META       "default_lakehouse_workspace_id": "8fe40bbb-ca75-4743-948c-f527f765f011",
# META       "known_lakehouses": [
# META         {
# META           "id": "d8b07883-20af-4902-95af-b7c0932273ee"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # tpc-h Loading
# 
# Load tpc-h data from pipe-separated files stored in ADLS Gen 2 into a Lakehouse in Fabric.
# 
# Approximate run times for scale:  
# 1: ~1 min  
# 10: ~3 mins  
# 100: ~7 mins  
# 1000: ~43 mins  
# 
# **Agenda**  
# Intro to Notebooks; language; startup time; markdown; table of contents; parameters; magics; frozen cell; parallel run; charting;  
# variables; shortcuts; notebook resources; refresh table view; max workers; struct; dictionary; schema datatypes  
# collaboration; Settings: About; change name; endorsement; scheduling, open in VS Code  
# !!TODO add Resources, collaboration, scheduling, Data Wrangler, mermaid

# MARKDOWN ********************

# ## Setup

# MARKDOWN ********************

# ### Parameters
# file_path - the path to the tpch files to be loaded into the lakehouse
# 
# Examples:  
# Local files: "Files/scale 1"  
# Shortcut files: "Files/tpch csv/scale 100"

# PARAMETERS CELL ********************

file_path = "Files/tpc-h/scale1000/csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Disable v-order
# Disable this temporarily for fast load

# CELL ********************

# MAGIC %%pyspark
# MAGIC spark.conf.set('spark.sql.parquet.vorder.enabled', 'false')
# MAGIC 
# MAGIC # scale 100 with vorder enable: 8min 38s
# MAGIC # scale 100 with vorder enable: 4min 32s

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Imports
# Define any imports that will be used in the notebook

# CELL ********************

from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Functions
# Define the functions that will be used in the notebook

# CELL ********************

# This function gets the pipe-separated text files, loads them to a dataframe with the input schema and then loads to a lakehouse table
def get_and_load_tpch_file(file_path, table_name, schema):
    df = spark.read.option("sep", "|").csv(f"{file_path}/{table_name}.tbl", header=False, schema=schema)
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)


# Get the pipe-separated text file load to a dataframe with the given schema and return that dataframe
def get_tpch_file(file_path, table_name, schema):
    df = spark.read.option("sep", "|").csv(f"{file_path}/{table_name}.tbl", header=False, schema=schema)
    return df


# Save the input dataframe as a delta table in the lakehouse
def save_tpch_file(df, table_name):
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reset
# Delete all tables from the database

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS customer;
# MAGIC DROP TABLE IF EXISTS lineitem;
# MAGIC DROP TABLE IF EXISTS nation;
# MAGIC DROP TABLE IF EXISTS orders;
# MAGIC DROP TABLE IF EXISTS part;
# MAGIC DROP TABLE IF EXISTS partsupp;
# MAGIC DROP TABLE IF EXISTS region;
# MAGIC DROP TABLE IF EXISTS supplier;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Schemas
#   Define the schemas for all tables to be imported.  
# This code is using a dictionary to hold the table name and table schema.
# 
# 5.5 Dictionaries  
# https://docs.python.org/3/tutorial/datastructures.html
# 
# Dictionaries are similar to arrays, but contain key:value pairs, where keys must be unique within that dictionary.
# This dictionary (dict_schemas) holds the table name (key) and a struct containing the schema, including column name, datatype and nullability.

# CELL ********************

# Create a dictionary holding the table name and accompanying schema
dict_schemas = {
    'customer': StructType([
        StructField("custkey", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("address", StringType(), nullable=False),
        StructField("nationkey", IntegerType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("acctbal", FloatType(), nullable=True),
        StructField("mktsegment", StringType(), nullable=True),
        StructField("comment", StringType(), nullable=True)
    ]),
    'lineitem': StructType([
        StructField("orderkey", IntegerType(), nullable=False),
        StructField("partkey", IntegerType(), nullable=False),
        StructField("suppkey", IntegerType(), nullable=False),
        StructField("linenumber", IntegerType(), nullable=False),
        StructField("quantity", FloatType(), nullable=True),
        StructField("extendedprice", FloatType(), nullable=True),
        StructField("discount", FloatType(), nullable=True),
        StructField("tax", FloatType(), nullable=True),
        StructField("returnflag", StringType(), nullable=False),
        StructField("linestatus", StringType(), nullable=False),
        StructField("shipdate", DateType(), nullable=False),
        StructField("commitdate", DateType(), nullable=False),
        StructField("receiptdate", DateType(), nullable=False),
        StructField("shipinstruct", StringType(), nullable=False),
        StructField("shipmode", StringType(), nullable=False),
        StructField("comment", StringType(), nullable=True)
    ]),
    'nation': StructType([
        StructField("nationkey", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("regionkey", IntegerType(), nullable=False),
        StructField("comment", StringType(), nullable=True)
    ]),
    'orders': StructType([
        StructField("orderkey", IntegerType(), nullable=False),
        StructField("custkey", IntegerType(), nullable=False),
        StructField("orderstatus", StringType(), nullable=False),
        StructField("totalprice", FloatType(), nullable=True),
        StructField("orderdate", DateType(), nullable=False),
        StructField("orderpriority", StringType(), nullable=False),
        StructField("clearkdate", StringType(), nullable=False),
        StructField("shippriority", IntegerType(), nullable=False),
        StructField("comment", StringType(), nullable=True)
    ]),
    'part': StructType([
        StructField("partkey", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("mfgr", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("size", IntegerType(), nullable=True),
        StructField("container", StringType(), nullable=True),
        StructField("retailprice", FloatType(), nullable=True),
        StructField("comment", StringType(), nullable=True)
    ]),
    'partsupp': StructType([
        StructField("partkey", IntegerType(), nullable=False),
        StructField("suppkey", IntegerType(), nullable=False),
        StructField("availqty", IntegerType(), nullable=False),
        StructField("supplycost", FloatType(), nullable=True),
        StructField("comment", StringType(), nullable=True)
    ]),
    'region': StructType([
        StructField("regionkey", StringType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("comment", StringType(), nullable=True)
    ]),
    'supplier': StructType([
        StructField("suppkey", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("address", StringType(), nullable=False),
        StructField("nationkey", IntegerType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("acctbal", FloatType(), nullable=True),
        StructField("comment", StringType(), nullable=True)
    ])
}


for table_name, schema in dict_schemas.items():
    print(table_name, '\r\n', schema, '\r\n')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main
# Load all the tables into the database

# MARKDOWN ********************

# ### Serial load

# CELL ********************

for table_name, schema in dict_schemas.items():
    print(table_name, '\r\n', schema, '\r\n')
    get_and_load_tpch_file(file_path, table_name, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parallel load
# Use concurrent.futures to loop through the schema dictionary in parallel

# CELL ********************

# Loop through the schemas dictionary in parallel
from concurrent.futures import ThreadPoolExecutor

def load_table(table_name):
    schema = dict_schemas[table_name]
    get_and_load_tpch_file(file_path, table_name, schema)


# Use a ThreadPoolExecutor to run the tasks in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(load_table, dict_schemas.keys())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Report how many CPUs the worker has
import os
print(os.cpu_count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Checks
# Report the rowcounts to confirm how many rows have been loaded into each table

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'customer' AS source, COUNT(*) AS records FROM customer
# MAGIC UNION ALL
# MAGIC SELECT 'lineitem' AS source, COUNT(*) AS records FROM lineitem
# MAGIC UNION ALL
# MAGIC SELECT 'nation	' AS source, COUNT(*) AS records FROM nation
# MAGIC UNION ALL
# MAGIC SELECT 'orders	' AS source, COUNT(*) AS records FROM orders
# MAGIC UNION ALL
# MAGIC SELECT 'part	' AS source, COUNT(*) AS records FROM part
# MAGIC UNION ALL
# MAGIC SELECT 'partsupp' AS source, COUNT(*) AS records FROM partsupp
# MAGIC UNION ALL
# MAGIC SELECT 'region	' AS source, COUNT(*) AS records FROM region
# MAGIC UNION ALL
# MAGIC SELECT 'supplier' AS source, COUNT(*) AS records FROM supplier;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

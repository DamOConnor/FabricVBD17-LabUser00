# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69011f2b-c4d3-42b0-ada3-46b6dfb392d6",
# META       "default_lakehouse_name": "lh_pole",
# META       "default_lakehouse_workspace_id": "8fe40bbb-ca75-4743-948c-f527f765f011",
# META       "known_lakehouses": [
# META         {
# META           "id": "69011f2b-c4d3-42b0-ada3-46b6dfb392d6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # POLE Working
# Based on neo4j sample data available here:  
# https://github.com/neo4j-graph-examples/pole
# 
# Some code uses mssparkutils:  
# https://learn.microsoft.com/en-us/fabric/data-engineering/microsoft-spark-utilities

# MARKDOWN ********************

# ## Setup

# MARKDOWN ********************

# ### Imports

# CELL ********************

from notebookutils import mssparkutils
mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Parameters

# PARAMETERS CELL ********************

pFilepath = "Files/pole/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Functions

# MARKDOWN ********************

# Filenames are like this:
# 
# - crime-investigation.nodes.Vehicle.csv
# - crime-investigation.relationships.CALLED.csv
# 
# Clean them up so they can become table names

# CELL ********************

def generate_clean_table_name(file_name):
    # Split the file name by '.' and extract the required parts
    parts = file_name.split('.')
    if 'nodes' in parts:
        # Remove the "nodes" and "relationships" parts and concatenate with prefix
        clean_name = 'nod_' + parts[2].capitalize()
    elif 'relationships' in parts:
        clean_name = 'rel_' + parts[2].upper()
    else:
        clean_name = ''.join(parts)
    
    return clean_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Column names should not contain special characters to make them easy to query in the lakehouse

# CELL ********************

# Function to clean up column names
def clean_column_names(df):
    for col_name in df.columns:
        clean_name = col_name.strip().replace(' ', '_').replace(',', '_').replace(';', '_') \
                    .replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_') \
                    .replace('\n', '_').replace('\t', '_').replace('=', '_').replace(':', '_')
        clean_name = clean_name.lower()  # Convert to lowercase for consistency
        df = df.withColumnRenamed(col_name, clean_name)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# More Pythonic method but ended up with duplicate columns
import re
from pyspark.sql import DataFrame

# Function to clean up column names
def clean_column_names(df: DataFrame) -> DataFrame:
    # Create a mapping of original column names to cleaned column names
    cleaned_columns = {col: re.sub(r'\W+', '_', col).strip('_').lower() for col in df.columns}

    # Rename columns using the mapping
    return df.toDF(*[cleaned_columns[col] for col in df.columns])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main

# CELL ********************

# MAGIC %%sql
# MAGIC # Drop all tables
# MAGIC SHOW TABLES

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get a clean file list

# CELL ********************

# Get list of files to load - can't use wildcards here
files = mssparkutils.fs.ls(f"{pFilepath}")

# Filter the list to include only CSV files
csv_files = [file for file in files if file.name.endswith('.csv')]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Drop existing tables

# CELL ********************

# Loop through the files
for file in csv_files:
    clean_filename = generate_clean_table_name(file.name)
    print(f"Processing file: {file.name} -> Clean table name: {clean_filename}")

    # Drop table if it already exists
    spark.sql(f"DROP TABLE IF EXISTS {clean_filename}")


print(" ")
print("---- DROP TABLES END ----")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load the csv files to the lakehouse, cleaning up table and column names

# CELL ********************

# Loop through the files
for file in csv_files:
    clean_filename = generate_clean_table_name(file.name)
    print(f"Processing file: {file.name} -> Clean table name: {clean_filename}")
  
    # Load the table to a dataframe
    df = spark.read.format("csv").option("header","true").load(f"Files/pole/{file.name}")
    
    # Clean up the column names
    df = clean_column_names(df)

    # Save to the database
    df.write.format("delta").mode("overwrite").save(f"Tables/{clean_filename}")


print(" ")
print("---- LOAD TABLES END ----")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load one table - Area

# CELL ********************

spark.sql(f"DROP TABLE IF EXISTS nod_Area")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/pole/crime-investigation.nodes.Area.csv")

# df now is a Spark DataFrame containing CSV data from "Files/pole/crime-investigation.nodes.Area.csv".
display(df)

df.write.format("delta").mode("overwrite").save("Tables/nod_area")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/pole/crime-investigation.nodes.Crime.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Queries

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * 
# MAGIC FROM lh_pole.nod_area
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT `:ID`, areaCode, `:LABEL`
# MAGIC FROM lh_pole.nod_area
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- After column cleanup (backticks not required)
# MAGIC SELECT _ID, areaCode, _LABEL
# MAGIC FROM lh_pole.nod_area
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM lh_pole.nod_Postcode p
# MAGIC WHERE code = "M1 1LU"
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM lh_pole.rel_OCCURRED_AT LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM lh_pole.nod_Crime c
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM lh_pole.nod_Postcode p
# MAGIC     INNER JOIN lh_pole.rel_OCCURRED_AT o ON p._id = o._id
# MAGIC WHERE code = "M1 1LU"
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# MATCH (l:Location {address:$address})<-[r:OCCURRED_AT]-(c:Crime)
# RETURN c.date as crimeDate

# CELL ********************

# MAGIC %%sql 
# MAGIC -- Relate location (nod_Location, Crime (nod_Crime) via OCCURRED_AT (rel_OCCURRED_AT)
# MAGIC SELECT c.*, NULL, l.*, NULL, o.*
# MAGIC FROM nod_Crime c
# MAGIC     INNER JOIN rel_OCCURRED_AT o ON c._id = o._start_id 
# MAGIC         INNER JOIN nod_Location l ON o._end_id = l._id
# MAGIC WHERE postcode = 'M1 1LU'
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC -- Relate location (nod_Location, Crime (nod_Crime) via OCCURRED_AT (rel_OCCURRED_AT)
# MAGIC SELECT l.*, NULL, o.*
# MAGIC FROM nod_Location l
# MAGIC     INNER JOIN rel_OCCURRED_AT o ON l._id Between o._start_id AND o._end_id
# MAGIC     INNER JOIN nod_Crime c ON 
# MAGIC LIMIT 100;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

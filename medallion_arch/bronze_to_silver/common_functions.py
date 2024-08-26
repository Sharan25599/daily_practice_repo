# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,mounting layer
def mounting_from_adf(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

# DBTITLE 1,Function to read the file
def read_file(file_format,file_path,custom_schema,**options):
    return spark.read.format(file_format).schema(custom_schema).options(**options).load(file_path)
    

# COMMAND ----------

# DBTITLE 1,Changing the datatype of a column
def type_cast(df,column_name,cast_type):
    return df.withColumn(column_name,col(column_name).cast(cast_type))

# COMMAND ----------

# DBTITLE 1,Converting columns to snake_case
def snake_case(x):
    result=''
    prev_char=''
    for char in x:
         if char.isspace():
            result += '_'
         elif char.isupper() and prev_char.islower():
            result += '_'+char.lower()
         else:
            result += char.lower()
         prev_char = char
    return result


# COMMAND ----------

# DBTITLE 1,Function to drop the null values
def drop_null_values(df):
    return df.dropna()

# COMMAND ----------

# DBTITLE 1,Function to drop duplicates
def drop_duplicates(df):
    return df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,Function to write the source file to delta table
def write_to_delta(df, table_format, mode, path):
    df.write.format(table_format).mode(mode).save(path)
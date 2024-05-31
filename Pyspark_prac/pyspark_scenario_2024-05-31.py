# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[(101,'ABC','Bangalore'),
      (102,'PQR','Chennai'),
      (103,'XYZ','Nellore'),
      (104,'MNV','Coimbatore'),
      (105,'PRS','Salem')]
columns = ['id','name','city']
sample_df = spark.createDataFrame(data,columns)
sample_df.display()

# COMMAND ----------

# DBTITLE 1,converted id column to list
list_id = sample_df.rdd.map(lambda x:x.id).collect()
print(list_id)

# COMMAND ----------

# DBTITLE 1,Filtered the id  column is present int hte list
filtered_df = sample_df.filter(col('id').isin(list_id))
filtered_df.display()

# COMMAND ----------

# DBTITLE 1,To check random id present in the list
id_to_check = 103
is_present = id_to_check in list_id
print(is_present)


# COMMAND ----------

# DBTITLE 1,Function to write the file using file path
def write(df,file_format,mode,file_path):
    return df.write.format(file_format).mode(mode).save(file_path)

# COMMAND ----------

file_format='delta'
mode='overwrite'
file_path='dbfs:/FileStore/tables/demo_df'
write(sample_df,file_format,mode,file_path)

# COMMAND ----------

# DBTITLE 1,Function to read the file
def read(file_format,file_path):
    return spark.read.format(file_format).load(file_path)

# COMMAND ----------

file_format='delta'
file_path='dbfs:/FileStore/tables/demo_df'
df = read(file_format,file_path)
df.display()

# COMMAND ----------

# DBTITLE 1,Function to write the file as table
def write_file_to_table(df,file_format,mode,table_name):
    return df.write.format(file_format).mode(mode).saveAsTable(table_name)

# COMMAND ----------

file_format ='delta'
mode='overwrite'
table_name = 'sample_table'
write_file_to_table(df,file_format,mode,table_name)

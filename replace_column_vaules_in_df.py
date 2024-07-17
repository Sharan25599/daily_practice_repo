# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

address = [
    (1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")
    ]
columns = ["id","address","state"]
df =spark.createDataFrame(address,columns)
df.display()

# COMMAND ----------

# DBTITLE 1,Replaced column value using regexp_replace function
replace_value_df = df.withColumn('address',regexp_replace('address','Rd','Road'))\
                     .withColumn('address',regexp_replace('address','St','Street'))\
                     .withColumn('address',regexp_replace('address','Ave','Avenue'))
replace_value_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Replaced column value using when_otherwise
replace_value_df1 = df.withColumn('address',when(df.address.endswith('Rd'),regexp_replace('address','Rd','Road'))\
                       .when(df.address.endswith('St'),regexp_replace('address','St','Street'))\
                       .when(df.address.endswith('Ave'),regexp_replace('address','Ave','Avenue'))\
                       .otherwise(df.address))
replace_value_df1.display()
replace_value_df1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Replaced column value using translate function
replace_value_df3 = df.withColumn('address', translate('address', '123', 'ABC'))
replace_value_df3.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Replaced column value using columns address and state by overlay function
replace_value_df3 = df.select(overlay('address','state',15).alias('overlayed'))
replace_value_df3.display()

# COMMAND ----------

data=[
    ("ABCDE_XYZ", "FGH")
    ]
columns=["col1", "col2"]
sample_df = spark.createDataFrame(data,columns)

sample_df.select(overlay("col1", "col2", 7).alias("overlayed")).display()


# Databricks notebook source
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

custom_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("data", ArrayType(data_schema), True),
    StructField("support", MapType(StringType(), StringType()), True)
])



# COMMAND ----------

response_data = requests.get('https://reqres.in/api/users?page=2')
json_data = response_data.json()

# COMMAND ----------

df = spark.createDataFrame([json_data],custom_schema)
# df.display()

# COMMAND ----------

df1= df.withColumn('data',explode('data'))\
       .select('page','per_page', 'total', 'total_pages',col('data.*'),'support.url','support.text')
df1.display()


# COMMAND ----------

response_data = requests.get('https://pokeapi.co/api/v2/pokemon?limit=5&offset=10')
json_data1 = response_data.json()

# COMMAND ----------

pokeman_df = spark.createDataFrame([json_data1])
pokeman_df.display()

# COMMAND ----------



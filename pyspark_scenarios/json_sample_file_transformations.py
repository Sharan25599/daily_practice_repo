# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def read(file_format, file_path, **options):
    return spark.read.format(file_format).options(**options).load(file_path)

file_format = 'json'
file_path = 'dbfs:/FileStore/sample_files/new_2.json'
options = {'multiLine': True}

df = read(file_format, file_path, **options)
df.display()

# COMMAND ----------

# DBTITLE 1,flattened  the  json file
explode_df=df.select('batters.*','id','name','ppu','topping','type')\
             .withColumn('batter',explode('batter'))\
             .withColumn('topping',explode('topping'))\
             .select(col('batter.id').alias('batter_id'),col('batter.type').alias('batter_type'),'id','name','ppu',col('topping.id').alias('topping_id'),col('topping.type').alias('topping_type'),'type')
explode_df.display()

# COMMAND ----------

# DBTITLE 1,filtered by batter_type and topping type
filtered_by_chocolate_df=explode_df.filter((explode_df.batter_type == 'Chocolate') & (explode_df.topping_type == "Maple"))
filtered_by_chocolate_df.display()

# COMMAND ----------

unique_toppings_df = explode_df.select("topping_type").distinct()
unique_toppings_df.display()

# COMMAND ----------

# DBTITLE 1,change the specified batter_type
change_value_of_batter_type_df=explode_df.withColumn('batter_type',\
                       when(explode_df.batter_type == "Devil's Food", 'Spicy Food') \
                      .when(explode_df.batter_type == "Blueberry", 'Raspberry ')\
                      .otherwise(col('batter_type')))
change_value_of_batter_type_df.display()

# COMMAND ----------

grouped_df = df.groupBy("id", "type","name").count()
grouped_df.display()

# Databricks notebook source
data=[(1,'virat'),(2,'dhoni')]
columns=['id','name']
df=spark.createDataFrame(data,columns)
df.display()


# COMMAND ----------

from pyspark.sql.functions import *

df.withColumn('name',upper(df.name)).display()

# COMMAND ----------

df.select('id',upper(df.name).alias('name')).display()

# COMMAND ----------

df.createOrReplaceTempView('person')
# createOrReplaceTempView -- it creates temporary table or 
spark.sql('select id, upper(name) as name from person').display()

# COMMAND ----------

def uppercase(df):
    return df.withColumn('name',upper(df.name))

df.transform(uppercase).display()

# COMMAND ----------


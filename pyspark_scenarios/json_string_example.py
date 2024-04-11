# Databricks notebook source
from pyspark.sql.functions import *

json_string='{"Id":"2","Name":"sharan"kumar","City":"Bengaluru"}'
data=[(2,json_string)]
column=["col1","col2"]
df=spark.createDataFrame(data,column)
# df.display()

df1=df.withColumn("col3",split(col("col2"),'"Name":"')[0])\
      .withColumn("col4",lit('Name":"'))\
      .withColumn("col5",split(col("col2"),'"Name":"')[1])
df1.display()

df2 = df1.withColumn("col6", split(col("col5"), '"', 2)) \
        .withColumn("col7", concat_ws("", col("col6")))

# df2.display()

df3=df2.withColumn("col8",concat(col("col3"),col("col4"),col("col7"))).select(col('col2'),col('col8'))
df3.display()

# COMMAND ----------


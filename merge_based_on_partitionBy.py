# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/sample_date_csv.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

convert_timestamp_df = df.withColumn('date',to_timestamp(col('date')))
convert_timestamp_df.display()

# COMMAND ----------

split_year_df = convert_timestamp_df.withColumn('year',year(col('date')))
split_year_df.display()

# COMMAND ----------

split_year_df.write.format('delta').partitionBy('year').save('dbfs:/FileStore/tables/sample_date_csv')

# COMMAND ----------

new_df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/sample_date_csv2.csv')

# COMMAND ----------

display(new_df)

# COMMAND ----------

convert_timestamp_df2 = new_df.withColumn('date',to_timestamp(col('date')))
convert_timestamp_df2.display()

# COMMAND ----------

split_year_df2 = convert_timestamp_df2.withColumn('year',year(col('date')))
split_year_df2.display()

# COMMAND ----------

delta_table_path = 'dbfs:/FileStore/tables/sample_date_csv'
target_table = DeltaTable.forPath(spark,delta_table_path)

# COMMAND ----------

target_table.alias('target').merge(
                            split_year_df2.alias('source'),
                            "target.id = source.id and target.date = source.date"
                                ).whenMatchedUpdateAll(
                                ).whenNotMatchedInsertAll(
                                ).execute()

# COMMAND ----------

new_data_df = target_table.toDF()
new_data_df.display()

# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data= [
    (1,"2024-06-04 12:00:00"),
    (2,"2024-06-04 15:30:00")
       ]
columns = ['id','time_stamp']
df = spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

# DBTITLE 1,To convert Timestamp to UTC based on Timezone
timestamp_to_utc_df = df.withColumn("timestamp_to_utc", to_utc_timestamp(col("time_stamp"), "America/New_York"))
timestamp_to_utc_df.display()

# COMMAND ----------

# DBTITLE 1,To convert Timestamp to UTC based on Timezone
timestamp_to_utc_df2 = df.withColumn("timestamp_to_utc1", to_utc_timestamp(col("time_stamp"), "Asia/Kolkata"))
timestamp_to_utc_df2.display()

# COMMAND ----------

data=[(1,'1716584482639')]
columns = ['id','utc']
utc_df = spark.createDataFrame(data,columns)
utc_df.display()

# COMMAND ----------

# DBTITLE 1,converted UTC from milliseconds to seconds
df_seconds = utc_df.withColumn("timestamp_seconds", col("utc") / 1000)
df_seconds.display()
# Since UNIX timestamps are typically in seconds, we divide the milliseconds value by 1000.

# COMMAND ----------

# DBTITLE 1,Convereted UTC from seconds to Timestamp
timestamp_df = df_seconds.withColumn("timestamp", from_unixtime(col("timestamp_seconds")))
timestamp_df.display()

# COMMAND ----------

data = [('id','PRODUCT_UPDATE_1716584482639')]
columns = ['id','file_name']
file_name_df = spark.createDataFrame(data,columns)
file_name_df.display()



# COMMAND ----------

# DBTITLE 1,Extracted the UTC(millisecond) from a file_name
split_df = file_name_df.withColumn('utc',split('file_name','_').getItem(2))
split_df.display()

# COMMAND ----------

# DBTITLE 1,Converted UTC from milliseconds to seconds
seconds_df = split_df.withColumn("timestamp_seconds", col("utc") / 1000)
seconds_df.display()

# COMMAND ----------

# DBTITLE 1,Converted UTC seconds to Timestamp
timestamp_df = seconds_df.withColumn("timestamp", from_unixtime(col("timestamp_seconds")))
timestamp_df.display()

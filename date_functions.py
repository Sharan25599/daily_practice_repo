# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[(101,'ABC'),(102,'XYZ'),(103,'PQR')]
df= spark.createDataFrame(data, ["id","name"])
df.display()

# COMMAND ----------

# It gives the current date and format in default dateformat (yyyy-MM-dd)
df1 = df.withColumn('currentDate', current_date())
df1.display()

# COMMAND ----------

# It will convert the date format to a specified format but the datetype will be string
# date_format('col_name','format')
df2 = df1.withColumn('currentDate', date_format(df1.currentDate, 'dd.MM.yyyy'))
df2.display()


# COMMAND ----------

# It will convert date from stringtype to datetype,but in this format(yyyy-MM-dd)
# to_date('col_name','format')
df3=df2.withColumn('currentDate',to_date(df2.currentDate,'dd.MM.yyyy'))
df3.display()

# COMMAND ----------

df=spark.createDataFrame([('2023-10-19','2023-11-19')],['d1','d2'])
df.display()

# COMMAND ----------

# It gives the diff between the dates(in days)
# datediff(end,start)
df.withColumn('dateDiff',datediff(df.d2,df.d1)).display()

# COMMAND ----------

# It gives the diff between the months(1 month for above specified dates)
# months_between(end,start)
df.withColumn('monthsBetween',months_between(df.d2,df.d1)).display()

# COMMAND ----------

# It gives year from a specified date
df.withColumn('year',year(df.d2)).display()

# It gives month from a specified date
df.withColumn('month',month(df.d2)).display()

# It gives day from a specified date
df.withColumn('day',day(df.d2)).display()

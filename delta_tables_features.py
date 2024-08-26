# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

df= spark.read.format('csv').option('header',True).load('dbfs:/FileStore/employees.csv')

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('employees_table')

# COMMAND ----------

data = [
    ('11','jackson','105','CN','25000','25','01-06-2024'),
    ('12','Robert','102','IN','48000','27','06-06-2024'),
]
columns = ['employee_id','employee_name','department','country','salary','age','date_col']

df2 = spark.createDataFrame(data,columns)
df2.display()

# COMMAND ----------

df2.write.format('delta').mode('append').saveAsTable('employees_table')

# COMMAND ----------

read_df = spark.read.table('employees_table')
read_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC update employees_table set salary = 80000 where employee_id = '12'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from employees_table where employee_id = '10'

# COMMAND ----------

deltaTable  = DeltaTable.forName(spark,'employees_table')  
deltaTable_history = deltaTable.history()
display(deltaTable_history)

# COMMAND ----------

timestamp_df = spark.read.format('delta').option('timestampASof','2024-08-26T12:32:08.000+00:00').table('employees_table')
timestamp_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees_table TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees_table

# COMMAND ----------

version_df = spark.read.format('delta').option('versionAsof', 3).table('employees_table')
version_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize employees_table

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize employees_table zorder by (employee_id)

# COMMAND ----------



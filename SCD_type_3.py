# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

data=[
      (101,'John Doe','sales','1-1-2023','30-06-2024','null','null'),
      (102,'John Smith','marketing','15-03-2023','25-07-2024','null','null'),
      (103,'Tom curise','IT','11-04-2023','null','null','null')
      ]
columns = ['emp_id','emp_name','department','start_date','end_date','previous_dept','previous_start_date']
employee_df = spark.createDataFrame(data,columns)
employee_df.display()

# COMMAND ----------

employee_df.write.format('delta').mode('overwrite').saveAsTable('employee_table')

# COMMAND ----------

target1=DeltaTable.forName(spark,'employee_table')

# COMMAND ----------

data=[
      (102,'John Smith','sales','15-03-2023','25-07-2024','marketing','15-03-2023'),
      (103,'Tom curise','marketing','11-07-2024','null','IT','11-04-2023')
      ]
columns = ['emp_id','emp_name','department','start_date','end_date','previous_dept','previous_start_date']
employee_newdata_df = spark.createDataFrame(data,columns)
employee_newdata_df.display()

# COMMAND ----------

target1.alias('target').merge(
    source=employee_newdata_df.alias('source'),
    condition='target.emp_id = source.emp_id'
    ).whenMatchedUpdate(
        set={
            'previous_dept':'source.previous_dept',
            'previous_start_date':'source.previous_start_date',
            
        }
    ).whenNotMatchedInsert(
        values={
            'emp_id':'source.emp_id',
            'previous_dept':'source.previous_dept',
            'previous_start_date':'source.previous_start_date',
            

        }
    ).execute()

# COMMAND ----------

new_data_df = target1.toDF()
new_data_df.display()

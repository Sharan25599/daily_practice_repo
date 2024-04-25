# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,function to read the csv file
def read(file_format, file_path, **options):
    return spark.read.format(file_format).options(**options).load(file_path)

file_format = 'csv'
file_path = 'dbfs:/FileStore/sample_files/sales_customer.csv'
options = {'header': True}

employee_df = read(file_format, file_path, **options)
employee_df.display()

# COMMAND ----------

# DBTITLE 1,converted columns to snake_case
def snake_case(x):
    result=''
    prev_char=''
    for char in x:
        if char.isspace():
            result += '_'
        elif char.isupper() and prev_char.islower():
            result += '_'+ char.lower()
        else:
            result += char.lower()
        prev_char=char
    return result
snake_case_udf=udf(snake_case,StringType())
lst=list(map(lambda x: snake_case(x),employee_df.columns))
employee_df=employee_df.toDF(*lst)
employee_df.display()

# COMMAND ----------

# DBTITLE 1,splitted the email_id column to new columns as user and domain
split_email_df = employee_df.withColumn('user', split('email_id', '@').getItem(0)) \
                             .withColumn('domain', split('email_id', '@').getItem(1))
split_email_df.display()

# COMMAND ----------

# DBTITLE 1,Average age of employee based on job
avg_age_df=employee_df.groupBy('job').agg(avg('age').alias('avg_age_of_employees'))
avg_age_df.display()

# COMMAND ----------

# DBTITLE 1,find the total amout spent when is_married is equal to TRUE
total_amt_spent_df=employee_df.filter(employee_df.is_married == 'TRUE').agg(sum('spent').alias('total_amt_spent')) 
total_amt_spent_df.display()

# COMMAND ----------

# DBTITLE 1,Total number of orders placed by each employee
total_num_of_orders_df=employee_df.groupBy('employee_id','name').agg(sum('orders').alias('total_orders_by_employee'))
total_num_of_orders_df.display()

# COMMAND ----------

# DBTITLE 1,created a new column age_group based on age
age_group_df=employee_df.withColumn('age_group',when((employee_df.age >= 20) & (employee_df.age <= 29),'20-30')
                                  .when((employee_df.age >= 30) & (employee_df.age <= 39),'30-40')
                                  .when((employee_df.age >= 400) & (employee_df.age <= 49),'40-50')
                                  .otherwise('50+'))
age_group_df.display()

# COMMAND ----------



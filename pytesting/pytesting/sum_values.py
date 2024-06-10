# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def calculate_sum(df, column_name):
    total_sum = df.agg(sum(column_name))
    return total_sum

# COMMAND ----------


# result.display()

# COMMAND ----------


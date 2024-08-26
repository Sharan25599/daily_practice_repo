# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,read the file
def read(file_format, table_path):
   return spark.read.format(file_format).load(table_path)

# COMMAND ----------

# DBTITLE 1,separating the month column from order_date column
def month_column(df, actual_column, new_column):
    return df.withColumn(new_column, month(col(actual_column)))

# COMMAND ----------

# DBTITLE 1,Revenue by Month and Country
def total_revenue(df,new_column,price_col,quantity_col,month_col,country_col,alias_col):
    revenue = df.withColumn(new_column, col(price_col)*col(quantity_col))
    return revenue.groupBy(month_col,country_col).agg(sum(new_column).alias(alias_col))

# COMMAND ----------

def total_revenue1(df,new_column,price_col,quantity_col,month_col,alias_col):
    revenue = df.withColumn(new_column, col(price_col)*col(quantity_col))
    return revenue.groupBy(month_col).agg(sum(new_column).alias(alias_col))

# COMMAND ----------

def total_revenue2(df,new_column,price_col,quantity_col,country_col,alias_col):
    revenue = df.withColumn(new_column, col(price_col)*col(quantity_col))
    return revenue.groupBy(country_col).agg(sum(new_column).alias(alias_col))

# COMMAND ----------

# DBTITLE 1,Customer Based on Country Level i.e count distinct
def customer_based_on_country(df,group_by_column, aggregate_column, alias_column):
    return df.groupBy(group_by_column).agg(countDistinct(aggregate_column).alias(alias_column))

# COMMAND ----------

# DBTITLE 1,Average Delivery Time
def avg_delivery_time(df, delivery_time_column, delivery_date_column, order_date_column):
    df = df.withColumn(delivery_time_column, datediff(col(delivery_date_column), col(order_date_column)))
    return df.select(avg(col(delivery_time_column)).alias("avg_delivery_time"))

# COMMAND ----------

# DBTITLE 1,Average Order Price
def avg_order_price(df,groupby_column,agg_column, alias_column):
    return df.groupBy(groupby_column).agg(avg(agg_column).alias(alias_column))

# COMMAND ----------

# DBTITLE 1,write to delta table
def write_to_delta(df,file_format,mode,file_path):
    df.write.format(file_format).mode(mode).save(file_path)
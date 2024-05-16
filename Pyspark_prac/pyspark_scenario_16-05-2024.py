# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[(1,'Apple'),(2,'Samsung'),(3,'Redmi'),(4,'Nokia'),(5,'Oneplus')]
columns=["prod_id","prod_name"]
product_df = spark.createDataFrame(data,columns)
product_df.display()

# COMMAND ----------

data=[(101,"John"),(102,"Peter"),(103,"Micheal")]
columns= ["vendor_id","vendor_name"]
vendor_df = spark.createDataFrame(data,columns)
vendor_df.display()

# COMMAND ----------

data=[(1001,1,101,20000),(1002,4,103,25000),(1003,2,102,30000)]
columns=["purchase_id","prod_id","vendor_id","purchase_price"]
purchase_df = spark.createDataFrame(data,columns)
purchase_df.display()

# COMMAND ----------

data=[(11,"Sharan",1,2,55000),(22,"Rahul",1,2,60000),(33,"Barath",5,2,40000)]
columns=["sales_id","customer_name","prod_id","quantity","sales_price"]
sales_df = spark.createDataFrame(data,columns)
sales_df.display()

# COMMAND ----------

join_df = product_df.join(purchase_df, product_df.prod_id == purchase_df.prod_id)\
                    .join(sales_df,product_df.prod_id == sales_df.prod_id)\
                    .select(product_df.prod_id,product_df.prod_name,purchase_df.purchase_id,purchase_df.vendor_id,purchase_df.purchase_price,sales_df.sales_id,sales_df.customer_name,sales_df.quantity,sales_df.sales_price)
join_df.display()

# COMMAND ----------

profit_df = join_df.groupBy('vendor_id').agg(sum((col('sales_price') * col('quantity')) - col('purchase_price')).alias('total_profit'))
profit_df.display()

# COMMAND ----------

def write(df,file_format,mode,col_name,table_name,**options):
    return df.write.format(file_format).mode(mode).partitionBy(col_name).opion(options).table(table_name)

# COMMAND ----------

# for initial load
file_format = 'delta'
mode='append'
table_name='ordered_table'
col_name='sales_id'
options ={'replaceWhere' : "prod_id > 2",'mergeSchema': True}
initial_load_df = write(join_df,file_format,mode,col_name,table_name,**options,)
initial_load_df.display()

# COMMAND ----------

# for incremental load 
file_format = 'delta'
mode='overwrite'
table_name='ordered_table'
col_name = 'sales_id'
options ={'replaceWhere' : "product_name == 'samsung'" }
initial_load_df = write(join_df,file_format,mode,col_name,table_name,**options,)
initial_load_df.display()

# COMMAND ----------



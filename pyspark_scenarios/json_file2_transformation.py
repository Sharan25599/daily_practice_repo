# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

country_schema = StructType([
    StructField("Customer ID", IntegerType(), True),
    StructField("Order ID", IntegerType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Price", FloatType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Order Date", DateType(), True),
    StructField("Delivery Date", DateType(), True),
    StructField("Payment Method", StringType(), True),
    StructField("Country", StringType(), True)
])

# COMMAND ----------

def read(file_format, file_path, **options):
    return spark.read.format(file_format).options(**options).load(file_path)

file_format = 'json'
file_path = 'dbfs:/FileStore/sample_files/sample_json.json'
options = {'multiLine': True}

country_df = read(file_format, file_path, **options)
country_df.display()

# COMMAND ----------

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
lst=list(map(lambda x: snake_case(x),country_df.columns))
country_df=contry_df.toDF(*lst)
country_df.display()

# COMMAND ----------

# DBTITLE 1,find total revenue based on price*quantity
total_revenue_df = country_df.withColumn("price", col("price").cast("float"))\
       .withColumn("quantity", col("quantity").cast("int"))\
       .withColumn("TotalRevenue", col("price") * col("quantity"))
total_revenue_df.display()

# COMMAND ----------

# DBTITLE 1,to calculate the order duration
order_duration_df=country_df.withColumn('order_duration',datediff('delivery_date','order_date'))
order_duration_df.display()

# COMMAND ----------

country_df.groupBy('product_name').agg(avg('price')).display()

# COMMAND ----------

# DBTITLE 1,To find the most popular payment
popular_payment_methods_df = country_df.groupBy("payment_method").count().orderBy(col("count").desc()).limit(1)
popular_payment_methods_df.display()
# limit(1) -- it gives only the first record/row(to find most popular Payment)

# COMMAND ----------

# DBTITLE 1,Total payment done by customer using credit card
country_df.filter(country_df.payment_method == 'credit_card').agg(sum('price').alias('total_price_using_credit_card')).display()

# COMMAND ----------



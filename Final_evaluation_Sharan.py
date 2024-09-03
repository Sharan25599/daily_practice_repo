# Databricks notebook source
# MAGIC %md
# MAGIC ###Question: I have a data frame. You have some 10 rows are having some URL's.
# MAGIC ###I want to make a an API call by using the 10 URLs.
# MAGIC ###So could you please write your code on how you will extract the URL alone from the data frame by using  pyspark,
# MAGIC ###Then give some API generator to pass that URL into collecting the API, getting the data from the API.
# MAGIC ###In this case, I want you to justify which one as a very good performance and where my code will run this spark driver or the spark executer?

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType
import requests

# COMMAND ----------

# DBTITLE 1,Created Dataframe using 10 url's
data = [("https://jsonplaceholder.typicode.com/posts",), 
        ("https://jsonplaceholder.typicode.com/todos/1",), 
        ("https://fake-json-api.mock.beeceptor.com/users",),
        ("http://example.com/4",), 
        ("http://example.com/5",), 
        ("http://example.com/6",),
        ("http://example.com/7",), 
        ("http://example.com/8",), 
        ("http://example.com/9",),
        ("http://example.com/10",)]

schema = StructType([StructField("url", StringType(), True)])
df = spark.createDataFrame(data, schema)
df.display()

# I have taken some 3 sample json API url's to check with my result and other url's i have taken as an example.

# COMMAND ----------


def fetch_api_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return str(e)
    
# The function "fetch_api_data" attempts to make a GET request to the provided URL.
# If the request is successful (status code 200), it returns the JSON content of the response.
# If the request is unsuccessful, it returns None.
# If an exception occurs during the request, it catches the exception and returns the error message as a string.

# COMMAND ----------

fetch_api_data_udf = udf(fetch_api_data, StringType())
df_with_api_data = df.withColumn("api_response", fetch_api_data_udf(col("url")))
df_with_api_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Performance Justification
# MAGIC
# MAGIC ####Parallel Processing: 
# MAGIC Spark distributes tasks across multiple nodes. Each executor runs in parallel, allowing multiple API calls to be made simultaneously. 
# MAGIC This can reduce the total time taken compared to a serial approach that is one by one.
# MAGIC
# MAGIC ####Executor vs Driver: 
# MAGIC The actual API calls will be made by the executors and the driver coordinates the tasks and collects the results. This separation allows for better resource utilization and scalability.

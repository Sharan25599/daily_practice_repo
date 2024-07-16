# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

sample_data1 = [
    ("John    D.",30),
    ("Alice   G.", 25),
    ("Bob  T.", 35),
    ("Eve   A.", 28)
    ]
columns = ["name","age"]

df1= spark.createDataFrame(sample_data1,columns)
df1.display()

# COMMAND ----------

sample_data2 = [
    ("John    D.",30),
    ("Alice   G.", 25),
    ("Bob  T.", 35),
    ("Eve   A.", 28)
    ]
columns = ["name","age"]

df2= spark.createDataFrame(sample_data2,columns)
df2.display()

# COMMAND ----------

# DBTITLE 1,Function with same dataframe contents
def assertDataFrameEqual(df1,df2):
    assert df1.schema == df2.schema, "Schemas are not equal"
    assert df1.collect() == df2.collect(), "DataFrame contents are not equal"

# Example DataFrames
df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 2000), ("2", 4000)], schema=["id", "amount"])

# Compare DataFrames using the custom function
try:
    assertDataFrameEqual(df1, df2)
    print("DataFrames are identical")
except AssertionError as e:
    print(f"AssertionError: {e}")

# COMMAND ----------

# DBTITLE 1,Function without same dataframe contents
def assertDataFrameEqual(df1,df2):
    assert df1.schema == df2.schema, "Schemas are not equal"

# Example DataFrames
df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 2000), ("2", 4000)], schema=["id", "amount"])

# Compare DataFrames using the custom function
try:
    assertDataFrameEqual(df1, df2)
    print("DataFrames are identical")
except AssertionError as e:
    print(f"AssertionError: {e}")

# COMMAND ----------



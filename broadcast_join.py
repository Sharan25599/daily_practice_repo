# Databricks notebook source
# MAGIC %md
# MAGIC ###Example 1

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

large_df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/tables/customers_1000.csv')

# COMMAND ----------

display(large_df)

# COMMAND ----------

data=[
    ("1","dE014d010c7ab0c","Andrew","Goodman","Stewart-Flynn","Rowlandberg","Macao",25),
    ("2","2B54172c8b65eC3",	"Alvin","Lane","Terry,Proctor and Lawrence","Bethside","Papua New Guinea",36),
    ("3","d794Dd48988d2ac","Jenna","Harding","Bailey Group","Moniquemouth","China",29)
]
columns=["Index","Customer Id","First Name","Last Name","Company","City","Country","age"]
smaller_df = spark.createDataFrame(data,columns)
smaller_df.display()

# COMMAND ----------

broadcast_join = large_df.join(broadcast(smaller_df),large_df.Index == smaller_df.Index,'inner')
broadcast_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example 2

# COMMAND ----------

data1 = [
    (1, "Alice", "Math"),
    (2, "Bob", "Physics"),
    (3, "Cathy", "Chemistry"),
    (4, "David", "Biology")
]

columns1 = ["student_id", "name", "major"]

df1 = spark.createDataFrame(data1, columns1)
df1.display()

# COMMAND ----------

data2 = [
    (1, "Math"),
    (2, "Physics")
]

columns2 = ["student_id", "subject"]

df2 = spark.createDataFrame(data2, columns2)
df2.display()

# COMMAND ----------

result_df = df1.join(broadcast(df2), df1.student_id == df2.student_id, "inner")
result_df.display()

# Databricks notebook source
# MAGIC %md
# MAGIC ###Example 1 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data = [
    (1, "Alice", [1, 2, 3, 4]),
    (2, "Bob", [5, 6, 7]),
    (3, "Charlie", [8, 9]),
    (4, "David", []),
    (5, "Eve", [10, 11, 12, 13, 14])
]
columns = ["id", "name", "numbers"]
df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

# DBTITLE 1,To Find the value contains in the column 'numbers' using array_contains()
array_contains_df = df.withColumn("arraycontains",array_contains(col("numbers"),3))
array_contains_df.display()

# COMMAND ----------

# DBTITLE 1,To Find the size of an array using size()
array_length_df = df.withColumn("arraylength",size(col("numbers")))
array_length_df.display()

# COMMAND ----------

# DBTITLE 1,To Find the length of an column
df.withColumn('size_of_name',length(col('name'))).display()

# COMMAND ----------

# DBTITLE 1,To Find the position of an element in an array column using array_position()
array_position_df = df.withColumn("arrayposition",array_position(col("numbers"),6))
array_position_df.display()

# COMMAND ----------

# DBTITLE 1,To Remove an element from an array_column using array_remove()
remove_array_df = df.withColumn("arrayremove",array_remove(col("numbers"),3))
remove_array_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example 2

# COMMAND ----------

car_list = [
  {'Brand': 'Hyundai', 'Year': 2021, 'Colors': ['Red', 'Blue', 'While', 'Black']},
  {'Brand': 'Maruthi', 'Year': 2022, 'Colors': ['Red', 'Blue', 'While', 'Green']},
  {'Brand': 'TATA', 'Year': 2022, 'Colors': ['Orange', 'Blue', 'While', 'Black']}
]
columns = ['Brand','Year','Colors']
car_df = spark.createDataFrame(car_list,columns)
car_df.display()

# COMMAND ----------

# DBTITLE 1,To Find the value is 'Green' from the array column 'Colors' and filter to display column 'Brand' is 'Maruthi'
array_contains_df = car_df.withColumn('array_contains',array_contains(col('year'),'Green'))\
                          .filter(car_df.Brand == 'Maruthi')
array_contains_df.display()

# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

data=[(100,'sharan','IT','KA',25000,24),
      (101,'sathya','sales','KA',35000,25),
      (102,'barath','IT','KA',30000,24),
      (103,'yeshwanth','HR','KA',38000,27),
      ]

# COMMAND ----------

schema=StructType([
    StructField("Employee id",IntegerType(),True),
    StructField("Employee name",StringType(),True),
    StructField("Department", StringType(), True),
    StructField("State",StringType(), True),
    StructField("Salary",IntegerType(), True),
    StructField("Age",IntegerType(), True)])

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

def rename_columns(df, column_mapping):
    for old_col, new_col in column_mapping.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df
# The function iterates over each key-value pair in the column_mapping dictionary. For each pair, it uses the withColumnRenamed method (assuming this is PySpark DataFrame) to rename the corresponding column in the DataFrame df. It then updates the DataFrame df with the renamed column.

column_mapping = {
    "Employee id": "employee_id",
    "Employee name": "employee_name",
    "Department": "department_name",
    "State": "employee_state",
    "Salary": "salary",
    "Age": "age"
}

employee_df = rename_columns(df, column_mapping)
employee_df.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

data = [
    ('John', 'Jane', 'Alice'), ('Doe', 'Eve', 'Catherine'), ('Smith', 'Doe', 'Johnson')
]
columns = ('first_name', 'middle_name', 'last_name')

# Creating DataFrame
df = spark.createDataFrame(data, columns)

def concat_col(x, y, z):
    return str(x) + ' ' + str(y) + ' ' + str(z)

concat = udf(concat_col, StringType())

df.withColumn('full_name', concat(df.first_name, df.middle_name, df.last_name)).display()

# COMMAND ----------

data = [('2024-04-11',),  
        ('2024-04-12',),
        ('2024-04-13',)]
columns = ['date_column']

df = spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

data = [(123,),  
        (124,),
        (125,)]
columns = ['ID']  
df1 = spark.createDataFrame(data, columns)
df1.display()

# COMMAND ----------

data = [(125,),  
        (126,),
        (127,)]
columns = ['ID']  
df2 = spark.createDataFrame(data, columns)
df2.display()

# COMMAND ----------

# left_anti -- Returns all rows from the left DataFrame, where there is no match in the right DataFrame.
result_df = df1.join(df2, df1["ID"] == df2["ID"], "left_anti")
result_df.display()

# COMMAND ----------

# left-semi --- Returns all rows from the left DataFrame where there is match in the right DataFrame.
result_df1 = df1.join(df2, df1["ID"] == df2["ID"], "left_semi")
result_df1.display()

# COMMAND ----------

# left_join --- Returns all rows from the left DataFrame and matching rows from the right DataFrame.
result_df2 = df1.join(df2, df1["ID"] == df2["ID"], "left")
result_df2.display()

# COMMAND ----------

# right_join --- Returns all rows from the right DataFrame and matching rows from the left DataFrame.
result_df3 = df1.join(df2, df1["ID"] == df2["ID"], "right")
result_df3.display()

# COMMAND ----------

# inner_join --- Returns only the rows with matching in both DataFrames.
result_df4 = df1.join(df2, df1["ID"] == df2["ID"], "inner")
result_df4.display()

# COMMAND ----------

# full_outer_join --- Returns all rows from both DataFrames, including matching and non-matching rows.
result_df5 = df1.join(df2, df1["ID"] == df2["ID"], "full")
result_df5.display()

# COMMAND ----------

from pyspark.sql.functions import current_date, datediff, col

data = [
    (1, 'Male', '1990-05-15'),
    (2, 'Female', '1985-10-25'),
    (3, 'Male', '1988-03-07')]

columns = ['customer_id', 'gender', 'date_of_birth']
df = spark.createDataFrame(data, columns)

df = df.withColumn("age", (datediff(current_date(), col("date_of_birth")) / 365).cast("int"))
df.display()


# COMMAND ----------



# COMMAND ----------



# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Function to read the file
def read(file_format, file_path, **options):
    return spark.read.format(file_format).options(**options).load(file_path)

file_format = 'json'
file_path = 'dbfs:/FileStore/sample_files/Nested_json_file__1_.json'
options = {'multiLine': True}

df = read(file_format, file_path, **options)
df.display()

# COMMAND ----------

# DBTITLE 1,Flatten the data
exploded_df = df.withColumn('employees', explode('employees'))\
                .select('id', 'properties.*', 'employees.*')
exploded_df.display()

# COMMAND ----------

# DBTITLE 1,converting snake_case to camel_case
def snake_case(x):
    result = ''
    prev_char = ''
    for char in x:
        if char.isspace():
            result += '_'
        elif char.isupper() and prev_char.islower():
            result += '_' + char.lower()
        else:
            result += char.lower()
        prev_char = char
    return result

snake_case_udf = udf(snake_case, StringType())
lst = list(map(lambda x: snake_case(x), exploded_df.columns))
to_snake_case_df = exploded_df.toDF(*lst)
to_snake_case_df.display()


# COMMAND ----------

# DBTITLE 1,adding current_date as new column
add_date_col_df=to_snake_case_df.withColumn('current_date',current_date())
add_date_col_df.display()

# COMMAND ----------

# DBTITLE 1,find the employee id is 1003 and starts with name 'D'
filter_by_id_name_df = add_date_col_df.filter((add_date_col_df.emp_id == 1003) & (add_date_col_df.emp_name.like('D%')))
filter_by_id_name_df.display()

# COMMAND ----------

# DBTITLE 1,find employee whose store size is medium
df_filtered = df.filter(to_snake_case_df["store_size"] == "Medium")
df_filtered.display()

# COMMAND ----------

# DBTITLE 1,find number of employees based on store_size
employee_count_df = df.groupBy("store_size").agg(count("*").alias("number_of_employees"))
employee_count_df.display()

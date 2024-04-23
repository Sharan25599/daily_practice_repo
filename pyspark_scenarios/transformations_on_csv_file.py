# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

employee_schema=StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

# COMMAND ----------

def read_file(file_format, file_path,custom_schema, **options):
   return spark.read.format(file_format).schema(custom_schema).options(**options).load(file_path)

file_format='csv'
file_path = "dbfs:/FileStore/sample_files/Employee_Q1.csv"
options={'header':True}
custom_schema=employee_schema
student_df=read_file(file_format, file_path,custom_schema, **options)
student_df.display()

# COMMAND ----------

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
lst = list(map(lambda x: snake_case(x), employee_df.columns))
to_snake_case_df = employee_df.toDF(*lst)
to_snake_case_df.display()


# COMMAND ----------

# DBTITLE 1,find average salary based on each department
avg_sal_df=to_snake_case_df.groupBy('department').agg(avg('salary').alias('avg_sal_by_dept'))
avg_sal_df.display()

# COMMAND ----------

# DBTITLE 1,find whose department id is D101 and working in country India(IN)
filter_based_on_id_country_df=to_snake_case_df.filter((to_snake_case_df.department == 'D101') & (to_snake_case_df.country == 'IN'))
filter_based_on_id_country_df.display()

# COMMAND ----------

# DBTITLE 1,Add a new column salary_grade
def salary_grade(salary):
    if salary < 50000:
        return "Low"
    elif salary >= 50000 and salary < 75000:
        return "Medium"
    else:
        return "High"

salary_grade_udf = udf(salary_grade, StringType())
to_snake_case_df = to_snake_case_df.withColumn("salary", col("salary").cast(IntegerType()))
salary_grade_df = to_snake_case_df.withColumn("salary_grade", salary_grade_udf(col("salary")))
salary_grade_df.display()

# COMMAND ----------

# DBTITLE 1,find average salary by department

from pyspark.sql.window import Window

window = Window.partitionBy("country").orderBy("department")
avg_df = to_snake_case_df.withColumn("avg_salary_by_dept_country", avg("salary").over(window))
avg_df.display()

# COMMAND ----------

# DBTITLE 1,To find name starts with 'M' and his is country 'SA ' and salary=8000
filtered_df=to_snake_case_df.filter((to_snake_case_df.employee_name.like('M%')) & (to_snake_case_df.country == 'SA') & (to_snake_case_df.salary == 8000))
filtered_df.display()

# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data = [(1, "John", 30, "Sales", 50000.0),
    (2, "Alice", 28, "Marketing", 60000.0),
    (3, "Bob", 32, "Finance", 55000.0),
    (4, "Sarah", 29, "Sales", 52000.0),
    (5, "Mike", 31, "Finance", 58000.0)]

schema = StructType([
 StructField("id", IntegerType(), nullable=False),
 StructField("name", StringType(), nullable=False),
 StructField("age", IntegerType(), nullable=False),
 StructField("department", StringType(), nullable=False),
 StructField("salary", DoubleType(), nullable=False)
 ])

employee_df=spark.createDataFrame(data,schema)
employee_df.display()

# COMMAND ----------

avg_sal_df=employee_df.groupBy('department').agg(avg('salary').alias('avg_salary_by_dept'))
avg_sal_df.display()

# COMMAND ----------

bonus_sal_df=employee_df.withColumn('bonus',employee_df.salary * 0.1)
bonus_sal_df.display()

# COMMAND ----------

# Group the data by department and find the employee with the highest salary in each department
from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec=Window.partitionBy('department').orderBy('salary')
highest_sal_of_dept=employee_df.withColumn('row_num',row_number().over(windowSpec))
highest_sal_of_dept.filter(highest_sal_of_dept.row_num == 1).display()

# COMMAND ----------

from pyspark.sql.functions import max, col

highest_salary_by_department = employee_df.groupBy("department") \
    .agg(max("salary").alias("max_salary")) \
    .join(employee_df, on="department") \
    .where(col("max_salary") == col("salary"))
highest_salary_by_department.display()

# COMMAND ----------

# Find the top 3 departments with the highest total salary.
top_3_dept_df=employee_df.groupBy('department').agg(sum('salary').alias('highest_total_sal')).orderBy(desc('highest_total_sal'))
top_3_dept_df.display()

# COMMAND ----------

#Find the top most department having highest salary

top_most_dept_df=employee_df.groupBy('department').agg(sum('salary').alias ('total_sal_by_dept'))

windowSpec=Window.orderBy(top_most_dept_df.total_sal_by_dept.desc())

dept_highest_sal_df=top_most_dept_df.withColumn('row_num',row_number().over(windowSpec))

result=dept_highest_sal_df.filter(dept_highest_sal_df.row_num == 1).select('department')

result.display()

# COMMAND ----------

#Filter the DataFrame to keep only employees aged 30 or above and working in the "Sales" department

employee_above_30=employee_df.filter((employee_df.age >= 30) & (employee_df.department == 'Sales'))
employee_above_30.display()

# COMMAND ----------

#  Calculate the difference between each employee's salary and the average 
# salary of their respective department

windowSpec=Window.partitionBy('department')
employee_df=employee_df.withColumn('avg_sal_by_dept',avg(col('salary')).over(windowSpec))
diff_of_salary_df=employee_df.withColumn('diff_sal_by_dept',col('salary') - col('avg_sal_by_dept'))
diff_of_salary_df.display()

# COMMAND ----------

# Calculate the sum of salaries for employees whose names start with the letter "J".

sum_of_sal_df=employee_df.filter(employee_df.name.startswith('J')).agg(sum('salary').alias('total_sal'))
sum_of_sal_df.display()

# COMMAND ----------

# Sort the DataFrame based on the "age" column in ascending order and then by "salary" column in descending order

sort_age_salary_df=employee_df.orderBy('age',desc('salary'))
sort_age_salary_df.display()

# COMMAND ----------

# Replace the department name "Finance" with "Financial Services" in the DataFrame:

replace_dept_df=employee_df.withColumn('department',when(col('department') == 'Finance','Financial Services')\
    .otherwise(col('department')))
replace_dept_df.display()

# COMMAND ----------

# Calculate the percentage of total salary each employee contributes to their respective department.

wndowSpec=Window.partitionBy('departmment')

employee_df=employee_df.withColumn('total_dept_sal',sum('salary').over(windowSpec))

emp_percentage_contribute_df=(col('salary')/col('total_dept_sal'))*100

employee_df = employee_df.withColumn('percentage_contibuted',round(emp_percentage_contribute_df,2))

employee_df.display()

# COMMAND ----------



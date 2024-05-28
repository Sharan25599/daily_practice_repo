# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

data = [(1, "Peter", 30, "Sales", 50000.0),
    (2, "Parker", 28, "Marketing", 60000.0),
    (3, "Boshton", 32, "Finance", 55000.0),
    (4, "Katherine", 29, "Sales", 52000.0),
    (5, "Mitchel", 31, "Finance", 58000.0)]

schema = StructType([
 StructField("EmployeeID", IntegerType(), nullable=False),
 StructField("EmployeeName", StringType(), nullable=False),
 StructField("Age", IntegerType(), nullable=False),
 StructField("Department", StringType(), nullable=False),
 StructField("Salary", DoubleType(), nullable=False)
 ])

employee_df=spark.createDataFrame(data,schema)
employee_df.display()

# COMMAND ----------

# DBTITLE 1,Function to rename multiple columns
def rename_column(df,col_mapping):
    for old_col,new_col in col_mapping.items():
        df = df.withColumnRenamed(old_col,new_col)
    return df

# COMMAND ----------

# DBTITLE 1,Column Mapping
col_mapping={
    'EmployeeID':'employee_id',
    'EmployeeName':'employee_name',
    'Age':'age',
    'Department':'department',
    'Salary':'salary'
}
rename_column_df = rename_column(employee_df,col_mapping)
rename_column_df.display()

# COMMAND ----------

# DBTITLE 1,Highest Salary by Department
max_df = employee_df.groupBy('department').agg(max('salary').alias('total_max_salary_by_dept'))
max_df.display()

# COMMAND ----------

windowSpec=Window.partitionBy('department').orderBy('salary')
highest_sal_of_dept=employee_df.withColumn('row_num',row_number().over(windowSpec))
highest_sal_of_dept.filter(highest_sal_of_dept.row_num == 2).display()

# COMMAND ----------

# DBTITLE 1,Highest salary by employee in each department
highest_salary_by_department = employee_df.groupBy("department") \
    .agg(max("salary").alias("max_salary")) \
    .join(employee_df, on="department") \
    .where(col("max_salary") == col("salary"))
    
df5 =highest_salary_by_department.select('id','name','age','department','max_salary')
df5.display()

# COMMAND ----------

windowSpec= Window.partitionBy('department').orderBy('salary')
highest_sal_df = employee_df.withColumn('rank',rank().over(windowSpec))
highest_sal_df.filter(highest_sal_df.rank == 1)
highest_sal_df.display()


# COMMAND ----------

top_3_dept_df=employee_df.groupBy('department').agg(sum('salary').alias('highest_total_sal')).orderBy(col('highest_total_sal').desc())
top_3_dept_df.display()

# COMMAND ----------

# DBTITLE 1,Top most department having highest salary
top_most_dept_df=employee_df.groupBy('department').agg(sum('salary').alias ('total_sal_by_dept'))
windowSpec=Window.orderBy(top_most_dept_df.total_sal_by_dept.desc())
dept_highest_sal_df=top_most_dept_df.withColumn('row_num',row_number().over(windowSpec))
result=dept_highest_sal_df.filter(dept_highest_sal_df.row_num == 1)
result.display()

# COMMAND ----------

# DBTITLE 1,Total salary whose name starts with name 'P'
sum_of_sal_df=employee_df.filter(employee_df.name.startswith('P')).agg(sum('salary').alias('total_sal'))
sum_of_sal_df.display()

# COMMAND ----------

# DBTITLE 1,Added column name email
added_col_df = employee_df.withColumn('email',lit('user@gmail.com'))
added_col_df.display()

# COMMAND ----------

# DBTITLE 1,splitted the email column into separate columns user and domain
split_email_df = added_col_df.withColumn('user',split(col('email'),'@').getItem(0))\
                             .withColumn('domain',split(col('email'),'@').getItem(1))\
                             .drop('email')
split_email_df.display()                            

# COMMAND ----------

# MAGIC %md
# MAGIC DATE FUNCTION SCENARIOS

# COMMAND ----------

data = [
    ("2023-04-01", 1, 10, 1000),
    ("2023-06-02", 2, 5, 5000),
    ("2023-08-03", 3, 8, 12000),
    ("2024-03-05", 4, 6, 7500),
    ("2024-04-10", 5, 5, 8000),
    ("2024-11-12", 6, 10, 15000),
    ("2025-04-15", 7, 3, 6000),
    ("2025-05-01", 8, 6, 9000),
    ("2025-05-03", 9, 8, 11000),
]
columns = ["date", "customer_id", "product_id", "amount"]
sales_df = spark.createDataFrame(data,columns)
sales_df.display()

# COMMAND ----------

# DBTITLE 1,Changed the date string datatype to date datatype
changed_date_datatype = sales_df.withColumn('date',to_date('date','yyyy-MM-dd'))
changed_date_datatype.display()
# to_date('column_name','date_format as it is in dataframe')

# COMMAND ----------

# DBTITLE 1,Changed the date format
date_format_df = changed_date_datatype.withColumn('date',date_format('date','dd-MM-yyyy'))
date_format_df.display()

# COMMAND ----------

# DBTITLE 1,Extracted day,month and year from the date column
extracted_date_df =changed_date_datatype.withColumn('day',dayofmonth(col('date')))\
              .withColumn('month',month(col('date')))\
              .withColumn('year',year(col('date')))
extracted_date_df.display()

# COMMAND ----------

changed_date_datatype.withColumn('year',split(col('date'),'-').getItem(0))\
                     .withColumn('month',split(col('date'),'-').getItem(1))\
                     .withColumn('day',split(col('date'),'-').getItem(2)).display()

# COMMAND ----------

# DBTITLE 1,Monthly Sales based on year and month
monthly_sales_df = extracted_date_df.groupBy('year','month').agg(sum('amount').alias('total_sales'))
monthly_sales_df.display()

# COMMAND ----------

# DBTITLE 1,Total sales based on product_id
total_sales_df = extracted_date_df.groupBy('product_id').agg(sum('amount').alias('total_sales_by_products')).orderBy('product_id')
total_sales_df.display()

# COMMAND ----------

data = [
    (1, 'Male', '1990-05-15'),
    (2, 'Female', '1985-10-25'),
    (3, 'Male', '1988-03-07')]

columns = ['customer_id', 'gender', 'date_of_birth']
demo_df = spark.createDataFrame(data, columns)
demo_df.display


# COMMAND ----------

# DBTITLE 1,To find Customer age based on DOB
demo_df.withColumn('age',(datediff(current_date(),'date_of_birth')/365).cast('int')).display()
        

# COMMAND ----------



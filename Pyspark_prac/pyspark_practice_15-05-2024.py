# Databricks notebook source
data = [('Sharan','Kumar','M',24000),
  ('Harish','Kumar','F',50000),
  ('Sabarish','Kumar','M',40000)]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

rdd2=df.rdd.map(lambda x: (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2))
df2=rdd2.toDF(["name","gender","new_salary"])
df2.display() 

# COMMAND ----------

# flatMap()
data = ["hello world", "how are you"]
rdd = spark.sparkContext.parallelize(data)

flat_mapped_rdd = rdd.flatMap(lambda sentence: sentence.split(" "))
result = flat_mapped_rdd.collect()
print(result)

# COMMAND ----------

# filter()
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = spark.sparkContext.parallelize(data)


filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
result = filtered_rdd.collect()
print(result)

# COMMAND ----------

data = [("apple", 3), ("banana", 2), ("apple", 5), ("banana", 7), ("apple", 9)]
rdd = spark.sparkContext.parallelize(data)

# Applied groupByKey() to group values by key
grouped_rdd = rdd.groupByKey()
result = grouped_rdd.collect()
for key, values in result:
    print("Key:", key, "Values:", list(values))
  

# COMMAND ----------

data=[('chocolate',2),('icecream',3),('chocolate',4),('icecream',3),('chocolate',7)]
rdd=spark.sparkContext.parallelize(data)

# Applied reduceByKey() to reduces the values by key
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)
result=reduced_rdd.collect()
for key, values in result:
    print("Key:", key, "Values:", values)

# COMMAND ----------



# COMMAND ----------

data=[('a',1),('b',2),('c',3),('d',4)]
columns=['letter','number']
sample_df = spark.createDataFrame(data,columns)
sample_df.display()

# COMMAND ----------

sample_df.write.format("delta").saveAsTable('replaceWhere_table')

# COMMAND ----------

data=[('x',7),('y',8),('c',9)]
columns=['letter','number']
sample_df2 = spark.createDataFrame(data,columns)
sample_df2.display()

# COMMAND ----------

# Function write the delta table
def write_delta_table(df,file_format,mode,table_name,**options):
    return df.write.format(file_format).mode(mode).option(options).saveAsTable(table_name)

# COMMAND ----------

file_format= 'delta'
mode='overwrite'
options={'replaceWhere' : 'number>2'}
table_name='replaceWhere_table'
write_delta_table(sample_df2,file_format,mode,table_name,**options)

# COMMAND ----------

sample_df2.write.format("delta").mode("overwrite").option('replaceWhere','number>2').saveAsTable('replaceWhere_table')

# COMMAND ----------

# function to read the delta table
def read_delta_table(file_format,table_name):
    return spark.read.format(file_format).table(table_name)

# COMMAND ----------

file_format= 'delta'
table_name='replaceWhere_table'
read_df1 = read_delta_table(file_format,table_name)
read_df1.display()

# COMMAND ----------

df_read = spark.read.format('delta').table('replaceWhere_table')
df_read.display()

# COMMAND ----------

data = [("Ernesto", "Guevara", "Argentina", "South America"),
    ("Wolfgang", "Manche", "Germany", "Europe"),
    ("Soraya", "Jala", "Germany", "Europe"),
    ("Jasmine", "Terrywin", "Thailand", "Asia"),
    ("Janneke", "Bosma", "Belgium", "Europe"),
    ("Hamed", "Snouba", "Lebanon", "Asia"),
    ("Bruce", "Lee", "China", "Asia"),
    ("Jack", "Ma", "China", "Asia")]
columns= ['first_name','last_name','country','continent']
student_df=spark.createDataFrame(data,columns)
student_df.display()

# COMMAND ----------

student_df.write.format('delta').saveAsTable('student_delta_table')

# COMMAND ----------

data = [("Jasmine", "T2rryw3n", "Thailand", "Asia"),
("Hamed", "Sn45b1", "Lebanon", "Asia"),
("Bruce", "L22", "China", "Asia"),
("Jack", "M1", "China", "Asia")]
columns= ['first_name','last_name','country','continent']
student_df2 = spark.createDataFrame(data,columns)
student_df2.display()

# COMMAND ----------

student_df2.write.format("delta").mode("overwrite").option('replaceWhere',"continent == 'Asia'").saveAsTable('student_delta_table')

# COMMAND ----------

read_student_table_df = spark.read.format('delta').table('student_delta_table')
read_student_table_df.display()

# COMMAND ----------

data = [("Ernesto", "Guevara", "Argentina", "South America"),
    ("Wolfgang", "Manche", "Germany", "Europe"),
    ("Soraya", "Jala", "Germany", "Europe"),
    ("Jasmine", "Terrywin", "Thailand", "Asia"),
    ("Janneke", "Bosma", "Belgium", "Europe"),
    ("Hamed", "Snouba", "Lebanon", "Asia"),
    ("Bruce", "Lee", "China", "Asia"),
    ("Jack", "Ma", "China", "Asia")]
columns= ['first_name','last_name','country','continent']
country_df=spark.createDataFrame(data,columns)
country_df.display()

# COMMAND ----------

country_df.write.format('delta').saveAsTable('country_delta_table')

# COMMAND ----------

data = [("Jasmine", "T2rryw3n", "Thailand", "Asia"),
("Hamed", "Sn45b1", "Lebanon", "Asia"),
("Bruce", "L22", "China", "Asia"),
("Jack", "M1", "China", "Asia")]
columns= ['first_name','last_name','country','continent']
country_df2 = spark.createDataFrame(data,columns)
country_df2.display()

# COMMAND ----------

country_df2.write.format("delta").mode("append").option('replaceWhere',"country == 'china'").saveAsTable('country_delta_table')

# COMMAND ----------

read_country_table_df = spark.read.format('delta').table('country_delta_table')
read_country_table_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC JOINS

# COMMAND ----------

emp_data = [(1,"Smith",-1,"2018","10","M",3000), 
(2,"Rose",1,"2010","20","M",4000), 
(3,"Williams",1,"2010","10","M",1000), 
(4,"Jones",2,"2005","10","F",2000), 
(5,"Brown",2,"2010","40","",-1), 
(6,"Brown",2,"2010","50","",-1)]
emp_columns = ["emp_id","name","superior_emp_id","year_joined", "emp_dept_id","gender","salary"]
emp_df = spark.createDataFrame(emp_data,emp_columns)
emp_df.display()

# COMMAND ----------

dept_data = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
dept_columns = ["dept_name","dept_id"]

dept_df = spark.createDataFrame(dept_data, dept_columns)
dept_df.display()

# COMMAND ----------

# Inner Join - Returns only the rows with matching keys in both DataFrames.
inner_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'inner')
inner_join_df.display()

# COMMAND ----------

def join_dataframes(df1,df2,col_name1,col_name2,join_condition):
    return df1.join(df2,df1.col_name1 == df2.col_name2, join_condition)

# COMMAND ----------

# Left_Join - Returns all the rows from the left dataframe and matching rows from the right dataframe
left_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'left')
left_join_df.display()

# COMMAND ----------

# Right join - Returns all rows from the right dataframe
right_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'right')
right_join_df.display()

# COMMAND ----------

# full_outer_join - Returns all rows from both the dataframes, including matching and non matching rows
# we can use full, outer, full_outer as keyword for joining dataframes
full_outer_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'full')
full_outer_join_df.display()

# COMMAND ----------

# left_semi_join - Returns all the rows from the left Dataframes where there is match in right Dataframe
left_semi_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'leftsemi')
left_semi_join_df.display()

# COMMAND ----------

left_anti_join_df = emp_df.join(dept_df,emp_df.emp_dept_id == dept_df.dept_id,'leftanti')
left_anti_join_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE AND UPDATE to write data

# COMMAND ----------

data = [("John", 25), ("Alice", 30), ("Bob", 35)]
columns = ["name", "age"]
df1 = spark.createDataFrame(data, columns)
df1.display()

# COMMAND ----------

df1.write.format("delta").mode("overwrite").saveAsTable("target_delta_data")

# COMMAND ----------

new_data = [("Peter", 26), ("Cristiano", 36), ("Mbappe", 30)]
columns = ["name", "age"]
df2 = spark.createDataFrame(new_data, columns)
df2.display()

# COMMAND ----------

df2.createOrReplaceTempView("new_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO target_delta_data AS t
# MAGIC     USING new_data AS n
# MAGIC     ON t.name = n.name
# MAGIC     WHEN MATCHED THEN
# MAGIC         UPDATE SET t.age = n.age
# MAGIC     WHEN NOT MATCHED THEN
# MAGIC         INSERT (name, age) VALUES (n.name, n.age)

# COMMAND ----------

merged_df = spark.read.format("delta").table("target_delta_data")
merged_df.display()

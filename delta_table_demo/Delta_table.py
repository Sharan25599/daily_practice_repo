# Databricks notebook source
data = [
    ("Alice", 34, "2022-01-01"),
    ("Bob", 45, "2022-01-02"),
    ("Charlie", 28, "2022-01-03")
]
columns = ["Name", "Age", "Date"]

# COMMAND ----------

df= spark.createDataFrame(data, columns)
df.display()

# COMMAND ----------

data = [
    ("John", 34, "2022-01-01"),
    ("Micheal", 45, "2022-01-02"),
    ("Peter", 28, "2022-01-03")
]
columns = ["Name", "Age", "Date"]
new_df=spark.createDataFrame(data, columns)
new_df.display()

# COMMAND ----------

new_df.write.format("delta").mode("overwrite").save("/FileStore/tables/partitioned_delta_table")

# COMMAND ----------

new_df.display()

# COMMAND ----------

new_df1.write.format("delta").partitionBy("Date").save("/FileStore/tables/delta_table")

# COMMAND ----------

data = [
    ("Sara", 34, "2022-01-01"),
    ("Jacks", 45, "2022-01-02"),
    ("Miller", 28, "2022-01-03")
]
columns = ["Name", "Age", "Date"]
add_new_df=spark.createDataFrame(data, columns)
add_new_df.display()

# COMMAND ----------

add_new_df.write.format("delta").mode("append").save("/FileStore/tables/delta_table")

# COMMAND ----------

add_new_df.display()

# COMMAND ----------

data = [
    ("Dhoni", 34, "2022-01-01",'India'),
    ("Cristiano", 45, "2022-01-02",'Portugal'),
    ("Mbappe", 28, "2022-01-03",'France')
]
columns = ["name", "age", "date","country"]
df1=spark.createDataFrame(data, columns)
df1.display()

# COMMAND ----------

df1.write.format("delta").partitionBy("country").save("/FileStore/tables/delta_table2")

# COMMAND ----------

data = [("Dhoni", 34, "2022-01-01",'India'),
    ("Cristiano", 45, "2022-01-02",'Portugal'),
    ("Mbappe", 28, "2022-01-03",'France')]
columns = ("name", "age", "date","country")
players_df=spark.createDataFrame(data, columns)
players_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write Delta table using function

# COMMAND ----------

# write delta table using table_name
def write_delta_table(df,file_format,mode,table_name):
    return df.write.format(file_format).mode(mode).saveAsTable(table_name)

# COMMAND ----------

file_format= 'delta'
mode='overwrite'
table_name='players_table'
write_delta_table(players_df,file_format,mode,table_name)

# COMMAND ----------

# write delta table using file path
def write_delta_table2(df,file_format,mode,file_path):
    return df.write.format(file_format).mode(mode).save(file_path)

# COMMAND ----------

file_format= 'delta'
mode='overwrite'
file_path='/FileStore/tables/players_table'
write_delta_table2(players_df,file_format,mode,file_path)

# COMMAND ----------

# Read delta table using table_name
def read_delta_table(table_name):
    return spark.read.table(table_name)

# COMMAND ----------

table_name='players_table'
df_players=read_delta_table(table_name)
df_players.display()

# COMMAND ----------

# Read delta table using file path 
def read_using_path(file_format,file_path):
    return spark.read.format(file_format).load(file_path)

# COMMAND ----------

file_format='delta'
file_path='/FileStore/tables/players_table'
df_players2=read_using_path(file_format,file_path)
df_players2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write 2 Delta table simultaneously

# COMMAND ----------

data1 = [("Nithesh", 34,'M',20000), ("Avinash", 45,'M',25000), ("Sathya", 28,'M',30000)]
columns = ("name", "age","gender","salary")

employee_df11 = spark.createDataFrame(data1, columns)

# COMMAND ----------

data2 = [("Rahul", 32,'M',30000), ("Barath", 41,'M',35000), ("Balaji", 50,'M',40000)]
columns = ("name", "age",'gender','salary')

employee_df22 = spark.createDataFrame(data2, columns)

# COMMAND ----------

# Write DataFrames to Delta tables simultaneously
employee_df1.write.format("delta").saveAsTable("employee_11")
employee_df2.write.format("delta").saveAsTable("employee_22")

#  Each write operation will be treated in a sequential manner(independently) of both Delta tables

# COMMAND ----------

data=[('Cristiano','Real Madrid',1),
      ('Messi','Barcelona',2),
      ('Lewandowski','Bayern Munich',3)]
columns=('name','team','top_scorers')

top_scorers_df=spark.createDataFrame(data, columns)
top_scorers_df.display()

# COMMAND ----------

def write(df,file_format,file_path):
    return df.write.format(file_format).saveAsTable(table_name)

# COMMAND ----------

file_format= 'delta'
table_name='top_scorers'
write(top_scorers_df,file_format,table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into top_scorers values ('Benzema','Real Madrid',2)

# COMMAND ----------

top_scorers_df1 = spark.read.table('top_scorers')
top_scorers_df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY top_scorers

# COMMAND ----------

# revert back the data using version
df_at_previous_version = spark.read.format("delta").option("versionAsOf", 0).table("top_scorers")
df_at_previous_version.display()

# COMMAND ----------

# revert back the data using timestamp
df_at_previous_version2 = spark.read.format("delta").option("timestampAsOf", '2024-05-13T09:32:35.000+0000').table("top_scorers")
df_at_previous_version2.display()

# COMMAND ----------



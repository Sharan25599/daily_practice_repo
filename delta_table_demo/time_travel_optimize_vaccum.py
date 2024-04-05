# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table Demo(
# MAGIC                             pk1 INT,
# MAGIC                             pk2 string,
# MAGIC                             dim1 INT,
# MAGIC                             dim2 INT,
# MAGIC                             dim3 INT,
# MAGIC                             dim4 INT,
# MAGIC                             active_status string,
# MAGIC                             start_date timestamp,
# MAGIC                             end_date timestamp)
# MAGIC                       USING DELTA
# MAGIC                       LOCATION 'dbfs:/FileStore/tables/demo_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Demo values(111,'Unit1', 200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into Demo values(222,'Unit2', 900,Null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into Demo values(333,'Unit3', 300,900,250,650,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into Demo values(444,'Unit4', 100,Null,700,300,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history Demo;

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1: Pyspark - Timestamp + Table

# COMMAND ----------

df= spark.read.format('delta').option('timestampASof','2024-04-05T04:51:51.000+0000').table('Demo')
df.display()
# quering 2nd version:- when we created the table at that point of time, what was the state  it will display

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2: Pyspark - Timestamp + Path

# COMMAND ----------

df= spark.read.format('delta').option('timestampASof','2024-04-05T04:51:51.000+0000').load('dbfs:/FileStore/tables/demo_table')
df.display()

# COMMAND ----------

# Method 3: Version + Path
df= spark.read.format('delta').option('versionASof', 4).load('dbfs:/FileStore/tables/demo_table')
df.display()

# COMMAND ----------

# Method 4 : version + table
df= spark.read.format('delta').option('versionASof', 3).table('Demo')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Method 5: SQL - version + table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Demo version as of 3

# COMMAND ----------

# MAGIC %md
# MAGIC Method 6: SQL -Timestamp + Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Demo timestamp as of '2024-04-05T04:51:59.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Demo values(666,'Unit6', 200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into Demo values(777,'Unit7', 900,Null,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into Demo values(888,'Unit8', 300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from Demo where pk1=777

# COMMAND ----------

# MAGIC %sql
# MAGIC update Demo 
# MAGIC set dim1=100 where pk1=666

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Demo

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum Demo 
# MAGIC -- it will delete the files which are invalidated 7 days before

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE Demo ZORDER BY (pk1)

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data=[(1000,'Michael','columbus','USA',987654321)]
schema=StructType([
    StructField('emp_id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('city',StringType(),True),
    StructField('country',StringType(),True),
    StructField('contact_no',IntegerType(),True)
])
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table employee(
# MAGIC       emp_id int,
# MAGIC       name string,
# MAGIC       city string,
# MAGIC       country string,
# MAGIC       contact_no int
# MAGIC     )
# MAGIC   using delta
# MAGIC   location "dbfs:/FileStore/tables/delta_merge"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

#converting dataframe into table or view
df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

--target table
%sql
select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source 
# MAGIC ON target.emp_id=source.emp_id
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC target.name=source.name,
# MAGIC target.city=source.city,
# MAGIC target.country=source.country,
# MAGIC target.contact_no= source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (emp_id, name, city, country, contact_no) VALUES (emp_id, name, city, country, contact_no)

# COMMAND ----------

data=[(1000,'Michael','chicago','USA',987654321),(2000,'Nancy','New York','USA',879654321)]
df=spark.createDataFrame(data,schema)
df.display()


# COMMAND ----------

df.createOrReplaceTempView("source_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO employee as target
# MAGIC USING source_view as source 
# MAGIC ON target.emp_id=source.emp_id
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC target.name=source.name,
# MAGIC target.city=source.city,
# MAGIC target.country=source.country,
# MAGIC target.contact_no= source.contact_no
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (emp_id, name, city, country, contact_no) VALUES (emp_id, name, city, country, contact_no)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

# MAGIC %md
# MAGIC USING PYSPARK

# COMMAND ----------

data=[(2000,'Sara','New york','USA',765843292),(3000,'Atlanta','New York','USA',870965432)]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# MAGIC %pip install delta-tables

# COMMAND ----------

# MAGIC %pip install delta-tables

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from delta.table import *
delta_df=DeltaTable.forpath(spark,"dbfs:/FileStore/tables/delta_merge")

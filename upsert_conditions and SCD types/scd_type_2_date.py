# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable

# COMMAND ----------

data=[(1,'sri','sri.ram@example.com','01-06-2024',None,'yes'),
    (2,'varsha','varsha.varshu@example.com','07-01-2024',None,'yes')]
schema=StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('mail',StringType(),True),
    StructField('start_date',StringType(),True),
    StructField('end_date',StringType(),True),
    StructField('status',StringType(),True)
])
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('mail_table2')

# COMMAND ----------

delta_table=DeltaTable.forName(spark,'mail_table2')

# COMMAND ----------

data1=[
    (1,'sri','sriram@mi.com','06-08-2024',None,'yes'),
    (2,'varsha','varshu@mi.com','08-08-2024',None,'yes'),
    (3,'mi','mi@mi.com','07-09-2024',None,'yes')
       ]
schema1=StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('mail',StringType(),True),
    StructField('start_date',StringType(),True),
    StructField('end_date',StringType(),True),
    StructField('status',StringType(),True)
])
df1=spark.createDataFrame(data1,schema1)
df1.display()

# COMMAND ----------

delta_table.alias('old').merge(
    df1.alias('new'),
    'old.id = new.id'
).whenMatchedUpdate(
    condition='new.start_date > old.start_date AND old.end_date IS NULL',
    set={
        'end_date': 'new.start_date',
        'status': lit('no')
    }
).whenNotMatchedInsert(
    values={
        'id': 'new.id',
        'name': 'new.name',
        'mail': 'new.mail',
        'start_date': 'new.start_date',
        'end_date': 'new.end_date',
        'status': lit('yes')
    }
).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mail_table2

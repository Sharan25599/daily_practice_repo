# Databricks notebook source
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

player_df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/player_data.csv')


# COMMAND ----------

player_df.display()

# COMMAND ----------

player_df.write.format('delta').mode('overwrite').saveAsTable('player_data_source')

# COMMAND ----------

target1=DeltaTable.forName(spark,'player_data_source')

# COMMAND ----------

data = [(1,"Cristiano", 37,"M",80000), 
        (2,"Messi", 35,"M",70000),
        (6,"Mbappe", 28,"M",75000)
        ]
columns = ["id","name", "age","gender","salary"]
player_updated_df = spark.createDataFrame(data, columns)
player_updated_df.display()

# COMMAND ----------

target1.alias("target").merge(
    source=player_updated_df.alias("source"),
    condition="target.id = source.id"
).whenMatchedUpdate(
    set={
                    "id" : "source.id",
                    "name" : "source.name",
                     "age" :"source.age",
                    "gender":"source.gender",
                   "salary":"source.salary"
                    }
).whenNotMatchedInsert(values={
                     "id" : "source.id",
                    "name" : "source.name",
                     "age" :"source.age",
                    "gender":"source.gender",
                   "salary":"source.salary"
        }).execute()

# COMMAND ----------

new_data=target1.toDF()
new_data.display()

# COMMAND ----------

data=[('1','30-05-2024'),('2','31-05-2024')]
columns = ['id','date']
df= spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

to_date_df = df.withColumn('date',to_date(col('date'),'dd-MM-yyyy'))
to_date_df.display()

# COMMAND ----------

data=[('1 2 3 4','30-05-2024'),('2 4 6 7','31-05-2024')]
columns = ['id','date']
demo_df= spark.createDataFrame(data,columns)
demo_df.display()

# COMMAND ----------

replaced_df = demo_df.withColumn('id', regexp_replace('id', ' ', ''))
replaced_df.display()

# COMMAND ----------

data=[
    (101,'    Samsung'),
    (102,'  Apple '),
    (103,'Oneplus  ')
    ]
columns = ['prod_id','prod_name']
product_df = spark.createDataFrame(data,columns)
product_df.display()

# COMMAND ----------

length_df= product_df.withColumn('char_count',length('prod_name'))
length_df.display()

# COMMAND ----------

trim_by_column_df = product_df.withColumn('prod_name',trim(col('prod_name')))
trim_by_column_df.display()

# COMMAND ----------

length_df2= trim_by_column_df.withColumn('char_count_after_trim',length('prod_name'))
length_df2.display()

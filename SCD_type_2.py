# Databricks notebook source
# MAGIC %md
# MAGIC from delta.tables import *
# MAGIC The delta.tables module is part of the Delta Lake library and provides classes and methods to work with Delta tables.
# MAGIC
# MAGIC from pyspark.sql.functions import *
# MAGIC The pyspark.sql.functions module provides a wide range of functions that can be used to perform operations on DataFrame columns

# COMMAND ----------

# DBTITLE 1,Import Functions
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Players dataframe with respected teams
data = [
    (1,'Cristiano','RealMadrid'),
    (2,'Messi','Barcelona'),
    (3,'Mbappe','PSG'),
]
columns = ['play_id','name','team']
player_df =spark.createDataFrame(data,columns)
player_df.display()

# COMMAND ----------

# DBTITLE 1,Added status column to players dataframe with default value as false
add_column_df = player_df.withColumn('status',lit('true'))
add_column_df.display()


# COMMAND ----------

# DBTITLE 1,write the Dataframe to Table
add_column_df.write.format('delta').mode('overwrite').saveAsTable('player_data1')

# COMMAND ----------

# MAGIC %md
# MAGIC 'DeltaTable' is a class provided by the Delta Lake library in Apache Spark.
# MAGIC It represents a Delta table and provides various methods for managing and manipulating data within the Delta Lake.
# MAGIC
# MAGIC 'forName' is a static method of the DeltaTable class. 
# MAGIC  It is used to create a DeltaTable object representing an existing Delta table by its name.
# MAGIC
# MAGIC  'spark': The SparkSession object.
# MAGIC   This provides the necessary context and configurations for interacting with Spark and Delta Lake.
# MAGIC
# MAGIC   'player_data1': The name of the Delta table want for the reference. 
# MAGIC    This is the table name as it is registered in the Spark catalog.
# MAGIC
# MAGIC    target1 = DeltaTable.forName(spark, 'player_data1') 
# MAGIC    It retrieves a Delta table named 'player_data1' and assigns it to the variable target1, allowing for further manipulation and management of this table.

# COMMAND ----------

target1=DeltaTable.forName(spark,'player_data1')


# COMMAND ----------

# DBTITLE 1,Players new dataframe with respected teams
data = [
    (1,'Cristiano','Alnassar'),
    (2,'Messi','Intermiami'),
    (4,'Benzama','Realmadrid')
     ]
columns = ['play_id','name','team']
player_newdata_df =spark.createDataFrame(data,columns)
player_newdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Aliase creation:
# MAGIC 1.target1 and player_newdata_df are two DataFrames.
# MAGIC 2.target1 is given an alias "target".
# MAGIC 3.player_newdata_df is given an alias "source".
# MAGIC
# MAGIC Merge Condition:
# MAGIC The merge is performed based on the condition that the play_id column in the "target" DataFrame matches the play_id column in the "source" DataFrame.
# MAGIC
# MAGIC When Matched (Update Operation):
# MAGIC If a match is found the existing row in "target" is updated.
# MAGIC The team column in the "target" DataFrame is updated to the value of the team column in the "source" DataFrame.
# MAGIC The status column in the "target" DataFrame is set to true.
# MAGIC
# MAGIC When Not Matched (Insert Operation):
# MAGIC If no match is found a new row is inserted into the "target" DataFrame.
# MAGIC The new row will have values for play_id, name, team, and status taken from the corresponding columns in the "source" DataFrame.
# MAGIC The status column for the new row is set to false.
# MAGIC

# COMMAND ----------

target1.alias("target").merge(
                                source=player_newdata_df.alias("source"),condition="target.play_id = source.play_id"
                            )\
                        .whenMatchedUpdate(
                         set={
                                "team" :"source.team",
                                "status": "true"
                            }
                        )\
                        .whenNotMatchedInsert(values={
                                "play_id" : "source.play_id",
                                "name" : "source.name",
                                "team" :"source.team",
                                "status": "false"
                                
                    }).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC 'target1' is assumed to be a Delta table object, which was likely obtained using the DeltaTable.forName(spark, 'player_data1') method as described earlier.
# MAGIC
# MAGIC The toDF() method is called on the target1 Delta table object to convert it into a PySpark DataFrame.
# MAGIC The resulting DataFrame is assigned to the variable new_data_df.

# COMMAND ----------

new_data_df = target1.toDF()
new_data_df.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

data=[
    (101,'Airasia',"04-06-2024"),
    (102,'Emirates',"05-06-2024"),
    (103,'Indigo',"06-06-2024")
      ]
schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('flight_name',StringType(),True),
    StructField('date',StringType(),True)
])

flight_df = spark.createDataFrame(data,schema)
flight_df.display()

# COMMAND ----------

add_column_df1 = flight_df.withColumn('status',lit('active'))
add_column_df1.display()

# COMMAND ----------

add_column_df1.write.format('delta').mode('overwrite').saveAsTable('flight_data')

# COMMAND ----------

target_table = DeltaTable.forName(spark,'flight_data')

# COMMAND ----------

data=[
    (101,'Airasia',"06-06-2024"),
    (102,'Emirates',"06-06-2024"),
    (104,'British airways',"06-06-2024")
    ]
schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('flight_name',StringType(),True),
    StructField('date',StringType(),True)
])
flight_new_data_df = spark.createDataFrame(data,schema)
flight_new_data_df.display()

# COMMAND ----------

added_col_df = flight_new_data_df.withColumn('status',lit('active'))
added_col_df.display()

# COMMAND ----------

target_table.alias('target').merge(
                                   source=added_col_df.alias('source'), condition='target.id = source.id AND target.date = source.date'
                                   )\
                                .whenMatchedUpdate(
                                            set={
                                                'date': 'source.date',
                                                'status':when((col("target.status") == "active"),lit("inactive"))\
                                                        .otherwise(col("target.status"))
                                                # 'status':when(col('target.date') < '06-06-2024', lit('inactive'))\
                                                        #  .otherwise(lit('active'))
                                                
                                            }
                                            )\
                                .whenNotMatchedInsert(
                                            values={
                                                'id': 'source.id',
                                                'flight_name': 'source.flight_name',
                                                'date': 'source.date',
                                                'status':when(col('source.date') > '05-06-2024', lit('inactive'))
                                                        .otherwise(lit('inactive'))

                                            }
                                        ).execute()

# COMMAND ----------

new_data_df1 = target_table.toDF()
new_data_df1.display()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC ###SCD Type 1
# MAGIC #####In SCD Type 1, the old data is overwritten with the new data. This method does not keep any history of changes.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

data = [
    (1010,'continental','Blue'),
    (2010,'Jawa','Black'),
    (3010,'Ducati','Red')
]
columns = ['Bike_id','Bike_name','colour']
bike_df = spark.createDataFrame(data,columns)
bike_df.display()

# COMMAND ----------

bike_df.write.format("delta").mode("overwrite").save('dbfs:/FileStore/upsert_folder/bike_table')

# COMMAND ----------

data = [
    (1010,'continental','Green'),
    (3010,'Himalayan','Black')
]
columns = ['Bike_id','Bike_name','colour']
bike_df2 = spark.createDataFrame(data,columns)
bike_df2.display()

# COMMAND ----------

deltaTableBike = DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/bike_table')

# COMMAND ----------

deltaTableBike.alias('target').merge(
                                        bike_df2.alias('source'),
                                        'target.Bike_id = source.Bike_id'
                                        ).whenMatchedUpdate(
                                            set={
                                        'target.Bike_id': 'source.Bike_id',
                                        'target.Bike_name': 'source.Bike_name',
                                        'target.colour': 'source.colour'
                                        }
                                        ).whenNotMatchedInsert(
                                            values={
                                        'target.Bike_id': 'source.Bike_id',
                                        'target.Bike_name': 'source.Bike_name',
                                        'target.colour': 'source.colour'
                                        }
                                        ).execute()

# COMMAND ----------

result_bike_df = deltaTableBike.toDF()
result_bike_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD Type 2
# MAGIC #####In SCD Type 2, a new row is added for each change. This method keeps the history of changes by adding new records.

# COMMAND ----------

data=[
    (101,'Airasia',"04-06-2024",'inactive'),
    (102,'Emirates',"05-06-2024",'inactive'),
    (103,'Indigo',"05-06-2024",'inactive')
      ]
schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('flight_name',StringType(),True),
    StructField('date',StringType(),True),
    StructField('status',StringType(),True)
])

flight_df = spark.createDataFrame(data,schema)
flight_df.display()

# COMMAND ----------

flight_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/fight_data')

# COMMAND ----------

data=[
    (101,'Airasia',"06-06-2024",'active'),
    (102,'Emirates',"06-06-2024",'active'),
    (104,'British airways',"06-06-2024",'active')
    ]
schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('flight_name',StringType(),True),
    StructField('date',StringType(),True),
    StructField('status',StringType(),True)
])
flight_new_data_df = spark.createDataFrame(data,schema)
flight_new_data_df.display()

# COMMAND ----------

target_table1 = DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/fight_data')

# COMMAND ----------

target_table1.alias('target').merge(
                                        source=flight_new_data_df.alias('source'),
                                        condition='target.id = source.id AND target.date = source.date'
                                    ) \
                                    .whenMatchedUpdate(
                                        set={
                                            'date': 'source.date',
                                            'status': when(col('target.date') == '06-06-2024', lit('active'))
                                                    .otherwise(lit('inactive'))
                                        }
                                    ) \
                                    .whenNotMatchedInsert(
                                        values={
                                            'id': 'source.id',
                                            'flight_name': 'source.flight_name',
                                            'date': 'source.date',
                                            'status': when(col('source.date') == '06-06-2024', lit('active'))
                                                    .otherwise(lit('inactive'))
                                        }
                                    ).execute()


# COMMAND ----------

new_data_df1 = target_table1.toDF()
new_data_df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####SCD Type 3
# MAGIC #####In SCD Type 3, a new column is added to keep track of the previous value of the changing column.

# COMMAND ----------

data=[
      (101,'John Doe','sales','1-1-2023','30-06-2024','null','null'),
      (102,'John Smith','marketing','15-03-2023','25-07-2024','null','null'),
      (103,'Tom curise','IT','11-04-2023','null','null','null')
      ]
columns = ['emp_id','emp_name','department','start_date','end_date','previous_dept','previous_start_date']
employee_df = spark.createDataFrame(data,columns)
employee_df.display()

# COMMAND ----------

employee_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/scdtype_3')

# COMMAND ----------

target1=DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/scdtype_3')

# COMMAND ----------

data=[
      (102,'John Smith','sales','15-03-2023','25-07-2024','marketing','15-03-2023'),
      (103,'Tom curise','marketing','11-07-2024','null','IT','11-04-2023'),
      (104,'Tom curran','HR','12-09-2024','null','null','null')
      ]
columns = ['emp_id','emp_name','department','start_date','end_date','previous_dept','previous_start_date']
employee_newdata_df = spark.createDataFrame(data,columns)
employee_newdata_df.display()

# COMMAND ----------

target1.alias('target').merge(
                                source=employee_newdata_df.alias('source'),
                                condition='target.emp_id = source.emp_id'
                                ).whenMatchedUpdate(
                                    set={
                                        'department': 'source.department',
                                        'end_date': 'source.end_date',
                                        'previous_dept': 'source.previous_dept',
                                        'previous_start_date': 'source.previous_start_date'
                                    }
                                ).whenNotMatchedInsert(
                                    values={
                                        'emp_id': 'source.emp_id',
                                        'emp_name': 'source.emp_name',
                                        'department': 'source.department',
                                        'start_date': 'source.start_date',
                                        'end_date': 'source.end_date',
                                        'previous_dept': 'source.previous_dept',
                                        'previous_start_date': 'source.previous_start_date'
                                    }
                                ).execute()

# COMMAND ----------

new_data_df = target1.toDF()
new_data_df.display()

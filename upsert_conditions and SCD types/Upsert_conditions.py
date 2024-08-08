# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delete from a table

# COMMAND ----------

data = [
    (1, "Sara", 25),
    (2, "Avinash", 27),
    (3, "Janani", 24),
    (4, "Shekar", 23),
    (5, "Nethra", 21)
]
columns = ["id", "name", "age"]
delete_df = spark.createDataFrame(data, columns)
delete_df.display()

# COMMAND ----------

delete_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/delete_table')

# COMMAND ----------

# DBTITLE 1,Delete the data within a table
deltaTable = DeltaTable.forPath(spark, 'dbfs:/FileStore/upsert_folder/delete_table')
deltaTable.delete("age > '25'")

# COMMAND ----------

read_delete_df = spark.read.format('delta').load('dbfs:/FileStore/upsert_folder/delete_table')
read_delete_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Update a table

# COMMAND ----------

data = [
    (1, "Sara", "F"),
    (2, "Avinash", "M"),
    (3, "Janani", "F"),
    (4, "Shekar", "M"),
    (5, "Nethra", "F")
]
columns = ["id", "name", "gender"]

update_df = spark.createDataFrame(data, columns)
update_df.display()

# COMMAND ----------

update_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/update_table')

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, 'dbfs:/FileStore/upsert_folder/update_table')
deltaTable.update(
  condition = (col('gender') == 'F') | (col('gender') == 'M'),
  set = {
      "gender": when(col('gender') == 'F', lit('Female'))
                .when(col('gender') == 'M', lit('Male'))
  }
)

# COMMAND ----------

read_update_df = spark.read.format('delta').load('dbfs:/FileStore/upsert_folder/update_table')
read_update_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Upsert into a table using merge

# COMMAND ----------

data=[
    (1,'vinay','kumar','male',101,20000),
    (2,'Harish','kumar','male',102,23000),
    (3,'Sathish','kumar','male',103,25000),
    (4,'Sabarish','kumar','male',104,27000),
]
columns = ['id','first_name','last_name','gender','ssn','salary']
employee_df =  spark.createDataFrame(data,columns)
employee_df.display()

# COMMAND ----------

employee_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/employee_table')

# COMMAND ----------

data=[
    (2,'Harish','kumar','male',102,35000),
    (5,'Sharan','','male',105,30000)
]
columns = ['id','first_name','last_name','gender','ssn','salary']
employee_df2 =  spark.createDataFrame(data,columns)
employee_df2.display()

# COMMAND ----------

deltaTableEmployee = DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/employee_table')

# COMMAND ----------

deltaTableEmployee.alias('target').merge(
                                        employee_df2.alias('source'),
                                        'target.id = source.id'
                                        )\
                                        .whenMatchedUpdate(set =
                                            {
                                            "target.id": "source.id",
                                            "target.first_name": "source.first_name",
                                            "target.last_name": "source.last_name",
                                            "target.gender": "source.gender",
                                            "target.ssn": "source.ssn",
                                            "target.salary": "source.salary"
                                            }
                                        ) \
                                        .whenNotMatchedInsert(values =
                                            {
                                            "target.id": "source.id",
                                            "target.first_name": "source.first_name",
                                            "target.last_name": "source.last_name",
                                            "target.gender": "source.gender",
                                            "target.ssn": "source.ssn",
                                            "target.salary": "source.salary"
                                            }
                                        ) \
                                        .execute()

# COMMAND ----------

result_df = deltaTableEmployee.toDF()
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Delta merge logic whenMatchedDelete

# COMMAND ----------

data = [
    ('Java', 10000),  
    ('PHP', 10000),
    ('Scala', 50000),
    ('Python', 10000)
]
columns = ['language','users_count']
language_df = spark.createDataFrame(data,columns)
language_df.display()

# COMMAND ----------

language_df.write.format("delta").mode("overwrite").save('dbfs:/FileStore/upsert_folder/language_table')

# COMMAND ----------

data = [
    ('Java', "10000"), 
    ('PHP', '10000'),
    ('Scala', '50000')
]
columns = ['language','users_count']
language_df2 = spark.createDataFrame(data,columns)
language_df2.display()


# COMMAND ----------

deltaTableLanguage = DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/language_table')

# COMMAND ----------

deltaTableLanguage.alias("t").merge(language_df2.alias("s"),
                                   "t.language = s.language"
                                   ).whenMatchedDelete(condition = "s.users_count = 10000")\
                                   .whenMatchedUpdateAll()\
                                   .whenNotMatchedInsertAll()\
                                   .execute()

# COMMAND ----------

final_df= deltaTableLanguage.toDF()
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD Type 1
# MAGIC #####In SCD Type 1, the old data is overwritten with the new data. This method does not keep any history of changes.

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
                                    )\
                                    .whenMatchedUpdate(set=
                                                        {
                                                        'target.Bike_id' : 'source.Bike_id',
                                                        'target.Bike_name' : 'source.Bike_name',
                                                        'target.colour' : 'source.colour'
                                                        })\
                                    .whenNotMatchedInsert(values={
                                                                  'target.Bike_id' : 'source.Bike_id',
                                                                  'target.Bike_name' : 'source.Bike_name',
                                                                  'target.colour' : 'source.colour'
                                                                 })\
                                    .execute()

# COMMAND ----------

result_bike_df = deltaTableBike.toDF()
result_bike_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD Type 2
# MAGIC #####In SCD Type 2, a new row is added for each change. This method keeps the history of changes by adding new records.

# COMMAND ----------

data=[
    (101,'Airasia',"04-06-2024"),
    (102,'Emirates',"05-06-2024"),
    (103,'Indigo',"05-06-2024")
      ]
schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('flight_name',StringType(),True),
    StructField('date',StringType(),True)
])

flight_df = spark.createDataFrame(data,schema)
flight_df.display()

# COMMAND ----------

add_column_df = flight_df.withColumn('status',lit('inactive'))
add_column_df.display()

# COMMAND ----------

add_column_df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/upsert_folder/fight_data')

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

target_table1 = DeltaTable.forPath(spark,'dbfs:/FileStore/upsert_folder/fight_data')

# COMMAND ----------

target_table1.alias('target').merge(
                                        source=added_col_df.alias('source'),
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

target_table1.alias('target').merge(
                                    added_col_df.alias('source'),
                                    condition='target.id = source.id and target.date = source.date'
                                ).whenMatchedUpdate(
                                    set={
                                        'status': when(col('target.date') < '06-06-2024', lit('inactive')).otherwise(lit('active'))
                                    }
                                ).whenNotMatchedInsert(
                                    values={
                                        'id': 'source.id',
                                        'flight_name': 'source.flight_name',
                                        'date': 'source.date',
                                        'status': when(col('source.date') == '06-06-2024', lit('active')).otherwise(lit('inactive'))
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

# COMMAND ----------



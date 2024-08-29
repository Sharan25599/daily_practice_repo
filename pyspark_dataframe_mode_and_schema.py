# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite Condition

# COMMAND ----------

#old data
data=[(1,"Refrigerator",10505,'2024-08-12'),
      (2,"Washing_machine",15323,'2024-08-13')]

column=["transaction_id","product","amount","date"]

old_data=spark.createDataFrame(data,column)
old_data.display()

# COMMAND ----------

old_data.write.format('delta').saveAsTable("electronic_sales")

# COMMAND ----------

#new data
data =[(3, 'Dishwasher', 700, '2024-08-23'),
    (4, 'Microwave', 120, '2024-08-23')]

column=['transaction_id', 'product', 'amount', 'date']

new_data=spark.createDataFrame(data,column)
new_data.display()

# COMMAND ----------

new_data.write.mode("overwrite").format('delta').saveAsTable("electronic_sales")

# COMMAND ----------

spark.read.table("electronic_sales").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite with mergeSchema

# COMMAND ----------

data = ([
    (1, 'Refrigerator', 800, '2024-08-22'),
    (2, 'Washing Machine', 600, '2024-08-22')
])
columns = ['transaction_id', 'product', 'amount', 'date']


old_data = spark.createDataFrame(data, columns)
old_data.display()


# COMMAND ----------

old_data.write.mode("overwrite").saveAsTable('electronic_sales_mergeSchema')

# COMMAND ----------

data2 = [
    (3, 'Dishwasher', 700, '2024-08-23', '2 years'),
    (4, 'Microwave', 120, '2024-08-23', '1 year')
]
columns2 = ['transaction_id', 'product', 'amount', 'date', 'warranty']

new_data_with_schema_change = spark.createDataFrame(data2, columns2)
new_data_with_schema_change.display()



# COMMAND ----------

new_data_with_schema_change.write.mode('overwrite').option('mergeSchema',True).saveAsTable('electronic_sales_mergeSchema')

# COMMAND ----------

spark.read.table('electronic_sales_mergeSchema').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite with overwrite schema

# COMMAND ----------

data3 = [
    (1, 'Refrigerator', 800, '2024-08-22'),
    (2, 'Washing Machine', 600, '2024-08-22')
]
columns3 = ['transaction_id', 'product', 'amount', 'date']

old_data = spark.createDataFrame(data3, columns3)
old_data.display()
old_data.printSchema()


# COMMAND ----------

old_data.write.format('delta').mode("overwrite").saveAsTable('electronic_sales_overwriteSchema')

# COMMAND ----------

data4 = [
    (3, 'Dishwasher', 700, '2024-08-23', 'Home Appliances'),
    (4, 'Microwave', 120, '2024-08-23', 'Kitchen Appliances')
]

schema = StructType([StructField('transaction_id', IntegerType(), False), 
                     StructField('product', StringType(), False), 
                     StructField('amount', IntegerType(), False), 
                     StructField('date', StringType(), False), 
                     StructField('category', StringType(), False)])

new_data_with_updated_schema = spark.createDataFrame(data4, schema)
new_data_with_updated_schema.display()

# COMMAND ----------

new_data_with_updated_schema.write.fromat('delta').mode('overwrite').option('overwriteSchema',True)\
.saveAsTable('electronic_sales_overwriteSchema')

# COMMAND ----------

new_data_with_updated_schema.display()
new_data_with_updated_schema.printSchema()

# COMMAND ----------

df = spark.read.table('electronic_sales_overwriteSchema')
df.display()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite with partitionBy

# COMMAND ----------

data5 =[
    (1, 'Refrigerator', 800, '2024-08-22'),
    (2, 'Washing Machine', 600, '2024-08-22'),
    (3, 'Dishwasher', 700, '2024-08-23'),
    (4,'chimney',1000,'2024-08-24')
]
columns5 = ['transaction_id', 'product', 'amount', 'date']

old_data = spark.createDataFrame(data5, columns5)
old_data.display()


# COMMAND ----------

old_data.write.format('delta').partitionBy('date').mode("overwrite").saveAsTable('electronic_sales_partitioned')

# COMMAND ----------

data6= [
    (4, 'Microwave_and_chimney', 120, '2024-08-24')
]
columns6 = ['transaction_id', 'product', 'amount', 'date']

new_data_partitioned = spark.createDataFrame(data6, columns6)
new_data_partitioned.display()


# COMMAND ----------

# Overwrite data in the partition for '2024-08-24'
new_data_partitioned.write.format('delta').partitionBy('date').mode('overwrite').saveAsTable('electronic_sales_partitioned')

# COMMAND ----------

spark.read.table('electronic_sales_partitioned').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Overwrite with replacewhere

# COMMAND ----------

data7 = [
    (1, 'Refrigerator', 800, '2024-08-22'),
    (2, 'Washing Machine', 600, '2024-08-22'),
    (3, 'Dishwasher', 700, '2024-08-23'),
    (4,'chimney',1000,'2024-08-24')
]

columns7 = ['transaction_id', 'product', 'amount', 'date']


old_data = spark.createDataFrame(data7, columns7)
old_data.display()

# COMMAND ----------

old_data.write.format('delta').partitionBy('date').mode("overwrite").saveAsTable('electronic_sales_replacewhere')

# COMMAND ----------

data8 = [
    (4, 'Microwave', 1500, '2024-08-24')
]
columns8 = ['transaction_id', 'product', 'amount', 'date']


new_data_partitioned = spark.createDataFrame(data8, columns8)
new_data_partitioned.display()



# COMMAND ----------

# Overwrite data in the partition for '2024-08-24'
new_data_partitioned.write.format('delta').partitionBy('date').option("replaceWhere","date='2024-08-24'").mode('overwrite').saveAsTable('electronic_sales_replacewhere')

# COMMAND ----------

spark.read.table('electronic_sales_replacewhere').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###overwrite with mergeSchema,overwriteSchema,partitionBy and replacewhere all together combine examples
# MAGIC

# COMMAND ----------

data = [
    (1, 'Refrigerator', 800, '2024-08-22'),
    (2, 'Washing Machine', 600, '2024-08-22'),
    (3, 'mixie',1000,'2024-08-23')
]
columns= ['transaction_id', 'product', 'amount', 'date']

old_data = spark.createDataFrame(data, columns)
old_data.display()


# COMMAND ----------

old_data.write.format('delta').mode("overwrite").partitionBy('date').saveAsTable('electronic_sales_allcombine')

# COMMAND ----------

data = [
    (3, 'Dishwasher', 700, '2024-08-23', '2 years'),
    (4, 'Microwave', 120, '2024-08-23', '1 year')
]
columns = ['transaction_id', 'product', 'amount', 'date', 'warranty']

new_data= spark.createDataFrame(data, columns)
new_data.display()

# COMMAND ----------

new_data.write.mode("overwrite").option("mergeSchema","true").option("overwriteSchema","true").option("replacewhere","date='2024-08-23'").partitionBy('date')\
.saveAsTable('electronic_sales_allcombine')

# COMMAND ----------

spark.read.table('electronic_sales_allcombine').display()

# COMMAND ----------



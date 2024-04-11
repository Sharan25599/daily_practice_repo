# Databricks notebook source
data=[(1,'Sharan','male',165000),(2,'Barath','male',150000)]
columns=['id','name','gender','salary']
df=spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

print(df.schema.fieldNames())
# fieldNames() -- it gives all the column names in list

# COMMAND ----------

fieldNames = df.schema.fieldNames()
if fieldNames.count('id')>0:
    print('id column is present in Dataframe')
else:
    print('id column is not present in Dataframe')
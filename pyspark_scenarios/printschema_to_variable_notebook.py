# Databricks notebook source
data=data=[(1,'Rahul','male'),(2,'Hemanth','male')]
columns=['id','name','gender']
df=spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

a= df.schema.simpleString()
print(a)

# COMMAND ----------

b=df.schema.json()
print(b)

# COMMAND ----------

c=df.schema.jsonValue()
print(c)

# COMMAND ----------


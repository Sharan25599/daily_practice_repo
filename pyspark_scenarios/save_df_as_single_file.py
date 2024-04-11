# Databricks notebook source
data=[(1,'virat',2000),(2,'dhoni',3000),(3,'rohith',4000),(4,'sachin',5000)]
columns=['id','name','salary']
df=spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

df.write.format('csv').option('header', True).save('dbfs:/FileStore/sample')

# COMMAND ----------

# writing a csv as single part file in a temp location
df.coalesce(1).write.format('csv').option('header', True).save('dbfs:/FileStore/sample1csv')

# COMMAND ----------

# finding csv part file name
filenames = dbutils.fs.ls('dbfs:/FileStore/sample1csv')
name=''
for filename in filenames:
    if filename.name.endswith('.csv'):
         name = filename.name

# copy csv part file to desired location  
dbutils.fs.cp('dbfs:/FileStore/sample1csv/'+ name, 'dbfs:/FileStore/sample_csv/persons.csv')

# COMMAND ----------

df=spark.read.format('csv').option('header',True).load('dbfs:/FileStore/sample_csv/persons.csv')
df.display()

# COMMAND ----------

# delete temp folder along with part files
dbutils.fs.rm('dbfs:/FileStore/sample1csv', True)

# COMMAND ----------


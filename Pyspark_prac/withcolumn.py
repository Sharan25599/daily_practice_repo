from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, concat_ws, when

spark=SparkSession.builder.appName("example").getOrCreate()

data = [('James','1991-04-01','M',3000,'james@gmail.com'),
  ('Michael','2000-05-19','M',4000,'michael@yahoo.com'),
  ('Williams','1978-09-05','M',5000,'williams@gmail.com'),
  ('Maria','1967-12-01','F',6000,'maria@gmail.com'),
  ('Jen','1980-02-17','F',8000,'jen@yahoo.com')
]
column=('name','DOB','gender','salary','email')
df=spark.createDataFrame(data,column)
df.show()

#  Change DataType using PySpark withColumn()
df1=df.withColumn('salary',col('salary').cast('Integer'))
df1.show()
df1.printSchema()

# Adding multiple new column using withColumn()
df.withColumn('country',lit('US'))\
  .withColumn('department',lit('IT')).show()

# split the column 'email' and add it new columns
df.withColumn('user',split('email','@').getItem(0))\
  .withColumn('domain',split('email','@').getItem(1)).show()

#Add Column Based on Another Column of DataFrame
df.withColumn('bonus',df.salary * 0.3).show()

#  Add column by concatinating existing columns
df.withColumn("name", concat_ws(",","firstname","lastname")).show()

#  Add Column using when otherwise condition
df.withColumn('grade',when(df.salary == 3000,'A')\
                              .when((df.salary >=4000) & (df.salary < 6000),'B')
                              .otherwise('C')).show()
#
df.withColumn('gender',when(df.gender == 'M','0')\
                               .when(df.gender == 'F','1')\
                               .otherwise('null')).show()

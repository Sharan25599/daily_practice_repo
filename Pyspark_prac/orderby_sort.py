from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName('example1').getOrCreate()

Data = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]
columns= ["employee_name","department","state","salary","age","bonus"]


df = spark.createDataFrame(Data,columns)

# sort the column department default it gives ascending order
df.sort('department').show()

# sort the column department and state by descending order
df.orderBy(df.department.desc(),df.state.desc()).show()

df.sort(col("department").asc(),col("state").asc()).show()
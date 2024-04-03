from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, col

spark=SparkSession.builder.appName("example").getOrCreate()

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

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(Data,schema)

# the total salary of each department
df.groupby('department').sum('salary').show()

# find the average age by each department and each state
df.groupby('department','state').avg('age').show()

df.groupby('department','salary').sum('salary','bonus').show()

df.groupby('department').agg(sum('salary').alias('total_salary'),\
                             avg('salary').alias('total_salary'),\
                             sum('bonus').alias('total_bonus'),\
                             max('bonus').alias('max_salary')).show()

data = [("HR", "Male", 5000),
        ("IT", "Female", 6000),
        ("HR", "Female", 5500),
        ("IT", "Male", 7000),
        ("Finance", "Male", 6500)]

columns = ["department", "gender", "salary"]
df1 = spark.createDataFrame(data, columns)

df1.groupby('department','gender').count().show()

df1.groupby('department').min('salary').show()


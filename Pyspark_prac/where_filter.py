from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("example").getOrCreate()

# where the age is greater than 30.
data = [("John", 25, "Male"),
        ("Alice", 35, "Female"),
        ("Bob", 30, "Male")]

columns = ["name", "age", "gender"]
df = spark.createDataFrame(data, columns)


filtered_df = df.filter(df.age > 30)
filtered_df.show()

# filter out the rows where the price is less than 100.
data = [("Product A", 120, "Electronics"),
        ("Product B", 80, "Clothing"),
        ("Product C", 150, "Electronics")]

columns = ["product_name", "price", "category"]
df1 = spark.createDataFrame(data, columns)

# df1.filter(df1.price >= 100).show()


# where the temperature is above 30 degrees Celsius and the location is either "New York" or "Los Angeles".
data = [("2024-04-01", 25, "New York"),
        ("2024-04-01", 32, "Los Angeles"),
        ("2024-04-02", 28, "Chicago")]

columns = ["date", "temperature", "location"]
df2 = spark.createDataFrame(data, columns)

df2.filter((df2.temperature > 30) & (df2.location.isin('New York','Los Angeles'))).show()

#
data=[(10,'Rahul',100,'M',2000),
      (20,'Barath',101,'M',4000),
      (30,'Balaji',102,'M',5000),
      (40,'Pooja',103,'F',3000),
      (50,'rishab',105,'M',6000)]
schema=('id','name','emp_dept_id','gender','salary')

df3=spark.createDataFrame(data,schema)

df3.filter((df3.salary >=4000) & (df3.salary <= 5000)).show()

#filter the gender where gender equal to female
# df3.filter(df3.gender == 'F').show()

# filter based upon gender is male and salary is 4000
# df3.filter((df3.gender=='M') &(df3.salary== 4000)).show()

# filter based on id is 40 or emp_dept_id is 103
# df3.filter((df3.id == 40) | (df3.emp_dept_id == 103)).show()

# filter on name starts with R and where salary is 2000
df3.filter((df3.name.startswith('R')) & (df3.salary == 2000)).show()

df3.filter(df3.name.endswith('a')).show()

df3.filter(df3.name.contains('l')).show()
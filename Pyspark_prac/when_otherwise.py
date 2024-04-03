from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark=SparkSession.builder.appName("when_otherwiseExample").getOrCreate()

data=[('ABC','Science',80,'p',90),
      ('XYZ','Maths',20,'f',75),
      ('PQR','Social',92,'p',85),
      ('MNO','English',None,'NA',70)]
columns=('Name','Subject','Marks','Status','Attendance')
df=spark.createDataFrame(data,columns)
df.show()

df_when_otherwise=df.withColumn('New_status',when(df.Marks >=50,'pass')
                                                 .when(df.Marks < 50,'fail')
                                                  .otherwise('Absentee'))
df_when_otherwise.show()

# using AND(&) and OR(|) operators
df1=df.withColumn('Grade',when((df.Marks>=80) | (df.Attendance>=80),'Distinction')
                                  .when((df.Marks<=50) & (df.Attendance<=65),'Good')
                                  .otherwise('Average'))
df1.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark=SparkSession.builder.appName('sample').getOrCreate()

customer_df=spark.read.format('json').option('multiLine',True).load(r"C:\Users\SharanKumar\Downloads\sample-json.json")
# customer_df.show()

def snake_case_to_lower(x):
    a = x.lower()
    return a.replace(' ','_')

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),customer_df.columns))
to_snake_case_df = customer_df.toDF(*lst)
to_snake_case_df.show()
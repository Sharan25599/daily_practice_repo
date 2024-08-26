# Databricks notebook source
# MAGIC %run /Workspace/medallion_arch/bronze_to_silver/common_functions

# COMMAND ----------

# MAGIC %run /Workspace/medallion_arch/bronze_to_silver/custom_schema

# COMMAND ----------

# DBTITLE 1,mounted the file
source='wasbs://medallionarch@storageworkspace123.blob.core.windows.net/'
mountpoint = '/mnt/mountpointstorageadfjson'
key='fs.azure.account.key.storageworkspace123.blob.core.windows.net'
value='4tTVvw/zU+T0rqZOhFRvoW5Arz5QAUjdyBftBM8zdmZRY46HnfkYisqIQyu6iLVmsyO9GXyt70ls+AStC93V'
mounting_from_adf(source,mountpoint,key,value)

# COMMAND ----------

# DBTITLE 1,read the  json file
file_format='json'
file_path = "dbfs:/mnt/mountpointstorageadfjson/source_to_bronze/sample-json.json"
options={'multiLine':True}
custom_schema=custom_schema
customer_df=read_file(file_format, file_path,custom_schema, **options)
customer_df.display()

# COMMAND ----------

# DBTITLE 1,changed the datatype of the columns
customer_df=type_cast(customer_df, "Customer ID", 'int')
customer_df=type_cast(customer_df, "Delivery Date", 'date')
customer_df=type_cast(customer_df, "Order Date", 'date')
customer_df=type_cast(customer_df, "Order ID", 'int')
customer_df=type_cast(customer_df, "Price", 'float')
customer_df=type_cast(customer_df, "Quantity", 'int')
# customer_df.printSchema()

# COMMAND ----------

# DBTITLE 1,converted columns to snake_case
snake_case_udf=udf(snake_case,StringType())
lst=list(map(lambda x:snake_case(x),customer_df.columns))
to_snake_case_df=customer_df.toDF(*lst)
# to_snake_case_df.display()

# COMMAND ----------

# DBTITLE 1,dropped the null values from the json file
drop_nulls_df=drop_null_values(to_snake_case_df)
# drop_nulls_df.display()

# COMMAND ----------

# DBTITLE 1,dropped the duplicates from the json file
drop_duplicates_df=drop_duplicates(drop_nulls_df)
# drop_duplicates_df.display()


# COMMAND ----------

table_format="delta"
mode="overwrite"
path="dbfs:/mnt/mountpointstorageadfjson/bronze_to_silver"
write_to_delta(drop_duplicates_df, table_format, mode, path)

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

read_processArea_df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/Process_Area.csv')
read_processArea_df.display()

# COMMAND ----------

read_module_df= spark.read.format('csv').option('header',True).load('dbfs:/FileStore/Module.csv')
read_module_df.display()

# COMMAND ----------

read_container_df = spark.read.format('csv').option('header',True).load('dbfs:/FileStore/Container.csv')
read_container_df.display()

# COMMAND ----------

convert_timestamp_processArea_df = read_processArea_df.withColumn('CreatedTime', to_timestamp(col('CreatedTime')))
split_year_processArea_df = convert_timestamp_processArea_df.withColumn('year', year(col('CreatedTime')))
split_year_processArea_df.display()

# COMMAND ----------

convert_timestamp_module_df = read_module_df.withColumn('CreatedTime', to_timestamp(col('CreatedTime')))
split_year_module_df = convert_timestamp_module_df.withColumn('year', year(col('CreatedTime')))
split_year_module_df.display()

# COMMAND ----------

convert_timestamp_container_df = read_container_df.withColumn('CreatedTime', to_timestamp(col('CreatedTime')))
split_year_container_df = convert_timestamp_container_df.withColumn('year', year(col('CreatedTime')))
split_year_container_df.display()

# COMMAND ----------

table_dict = {
    "read_processArea_df": read_processArea_df,
    "split_year_module_df": split_year_module_df,
    "split_year_container_df": split_year_container_df
}

# COMMAND ----------

file_format = 'delta'
storage_type = 'delta_file'
base_path = 'dbfs:/FileStore/partition_multiple_merge/testdemo1/'
mode = 'upsertall'
options = {"mergeSchema": "true"}
partition_by = ['year']
full_load = "True"
hard_delete = "Yes"
cond = None
set_to_upsert = None
hive_metastore = False
table_name = ''
path = f"{base_path}{table_name}"

# COMMAND ----------

def update_upsert_col(partition_column, unique_values):
    if partition_column == 'None':
        return [unique_values]
    else:
        return [unique_values, partition_column]

# register the update_upsert_col function as a UDF
update_upsert_col_udf = udf(update_upsert_col, ArrayType(StringType()))

read_TableNames = spark.read.format('csv').options(header='True', delimiter=',').load('dbfs:/FileStore/TableNames.csv')

# update the upsert_col based on the condition
read_TableNames = read_TableNames.withColumn("upsert_col", update_upsert_col_udf(read_TableNames["PartitionColumn"], read_TableNames["UniqueValues"]))

read_TableNames.printSchema()
read_TableNames.display()

# COMMAND ----------

def writeToDatalake(df, file_format: str, storage_type: str, path: str, mode: str, options={}, partition_by=[], full_load=None, hard_delete=None, cond=None, set_to_upsert=None, hive_metastore=False):
    if mode not in ['overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default', 'upsert', 'upsertall']:
        raise ValueError(f"Unknown save mode: {mode}. Accepted save modes are 'overwrite', 'append', 'upsert', 'upsertall', 'ignore', 'error', 'errorifexists', 'default'.")

    if mode == 'upsertall' and storage_type == 'delta_file':
        if not DeltaTable.isDeltaTable(spark, path):
            df_writer = df.write.mode("overwrite").format("delta").options(**options)
            if partition_by:
                print("delta table and split and save as a partition")
                df_writer = df_writer.partitionBy(partition_by)
            df_writer.save(path)
        else:
            deltaTable = DeltaTable.forPath(spark, path)
            matchKeys = " AND ".join(f"old.{col_name} = new.{col_name}" for col_name in upsert_col)
            update_cols = [col_name for col_name in df.columns if col_name != 'EDP_InsertDateTime']
            update_dict = {col_name: col(f"new.{col_name}") for col_name in update_cols}
            if 'EDP_UpdateDateTime' in update_dict:
                update_dict['EDP_UpdateDateTime'] = date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS+00:00")
            
            deltaTable.alias("old") \
                .merge(df.alias("new"), matchKeys) \
                .whenMatchedUpdate(set=update_dict) \
                .whenNotMatchedInsertAll() \
                .execute()

            if full_load == "True" and hard_delete == "Yes":
                deltaTable.alias("old").merge(
                    df.alias("new"),
                    matchKeys
                    ).whenNotMatchedBySourceUpdate(set={
                        "EDP_DeletedAtSource": lit(1),
                        "EDP_ActiveStatus": lit(0)
                    }).execute()
    else:
        df_writer = df.write.mode(mode).format(file_format).options(**options)
        if partition_by:
            df_writer = df_writer.partitionBy(partition_by)
        df_writer.save(path)

# COMMAND ----------

for row in read_TableNames.collect():
    table_name = row['TableName'].strip()
    partition_column = row['PartitionColumn'].strip()
    upsert_col = row['upsert_col']

    # check if dataframe exists for the given table
    df = table_dict.get(table_name)
    if df is None:
        print(f"No dataframe found for table {table_name}")
        continue

    # verify if upsert_col columns exist in the dataframe
    missing_cols = [col for col in upsert_col if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Columns {', '.join(missing_cols)} in upsert_col are not found in DataFrame for table {table_name}")

    # construct the path for saving in delta table
    path = f"{base_path}{table_name}"

    if partition_column == 'None':
        partition_by = []
    else:
        partition_by = ['year']

    writeToDatalake(df, file_format, storage_type, path, mode, options, partition_by, full_load, hard_delete, cond, set_to_upsert, hive_metastore)

# COMMAND ----------



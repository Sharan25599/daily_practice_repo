# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /Workspace/medallion_arch/silver_to_gold/common_functions

# COMMAND ----------

# DBTITLE 1,read the file from bronze_to_silver
table_path="dbfs:/mnt/mountpointstorageadfjson/bronze_to_silver"
customer_df = spark.read.format("delta").load(table_path)
# customer_df.display()

# COMMAND ----------

# DBTITLE 1,Separating the month column from order_date column
customer_df=month_column(customer_df, "order_date", "month")
# customer_df.display()

# COMMAND ----------

# DBTITLE 1,Revenue by Month and Country (Price*Quantity)
revenue_df=total_revenue(customer_df, "revenue", "price", "quantity", "month", "country", "total_revenue" )
# revenue_df.display()

# COMMAND ----------

# DBTITLE 1,Customer Based on Country Level i.e count distinct
customer_based_on_country_df=customer_based_on_country(customer_df,"country","customer_id","count_of_customer")
customer_based_on_country_df.display()

# COMMAND ----------

# DBTITLE 1,Average Delivery Time
average_delivery_time_df=avg_delivery_time(customer_df, "delivery_time", "delivery_date", "order_date")
average_delivery_time_df.display()

# COMMAND ----------

# DBTITLE 1,Average Order Price
average_order_price_df=avg_order_price(customer_df,"order_id","price", "average_price")
average_order_price_df.display()

# COMMAND ----------

# DBTITLE 1,write revenue_df to delta_table
write_to_delta(revenue_df,"delta","overwrite","dbfs:/mnt/mountpointstorageadfjson/silver_to_gold/revenue_df")

# COMMAND ----------

# DBTITLE 1,write customer_based_on_country_df to delta_table
write_to_delta(customer_based_on_country_df,"delta","overwrite","dbfs:/mnt/mountpointstorageadfjson/silver_to_gold/customer_based_on_country_df")

# COMMAND ----------

# DBTITLE 1,write average_delivery_time_df to delta_table
write_to_delta(average_delivery_time_df,"delta","overwrite","dbfs:/mnt/mountpointstorageadfjson/silver_to_gold/average_delivery_time_df")

# COMMAND ----------

# DBTITLE 1,write average_order_price_df to delta_table
write_to_delta(average_order_price_df,"delta","overwrite","dbfs:/mnt/mountpointstorageadfjson/silver_to_gold/average_order_price_df")

# COMMAND ----------


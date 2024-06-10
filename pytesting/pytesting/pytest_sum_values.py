# Databricks notebook source
# MAGIC %run "/Workspace/CI_CD/pytesting/sum_values"

# COMMAND ----------


pip install pytest


# COMMAND ----------

data = [(10,), (3,), (7,)]
columns = ["values"]
df = spark.createDataFrame(data, columns)

result = calculate_sum(df, "values")

# COMMAND ----------

# import pytest


def test_calculate_sum():
    data = [(10,), (3,), (7,)]
    columns = ["values"]
    df = spark.createDataFrame(data, columns)
    
    result = calculate_sum(df, "values")
    assert result == 20, "Test case 1 failed"

    data = [(4,), (5,), (6,)]
    df = spark.createDataFrame(data, columns)
    result = calculate_sum(df, "values")
    assert result == 15, "Test case 2 failed"

test_calculate_sum()
print("All tests passed!")

# COMMAND ----------


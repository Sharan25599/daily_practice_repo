# Databricks notebook source
def read(file_format,file_path,**options):
    return spark.read.format(file_format).option(options).load(file_path)

# COMMAND ----------

options={'header':True}
file_path=" "
df1=read('csv',file_path,**options)

# COMMAND ----------

options={'header':True}
file_path=" "
df2=read('csv',file_path,**options)

# COMMAND ----------

def join_dataframe(df1,df2,join_condition,col_name):
    return df1.join(df2,df1.col_name == df2.col_name,join_condition)

# COMMAND ----------

def pivot(df,group_by_col,pivot_col,agg_col):
    return df.groupBy(group_by_col),pivot(pivot_col).sum(agg_col)

# COMMAND ----------

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.display()

# COMMAND ----------

from pyspark.sql.functions import expr
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)).where("Total is not null")
unPivotDF.display()

# COMMAND ----------

# stack function, which typically takes multiple pairs of column values and their corresponding column names, and stacks them vertically to create a single column with the specified names.
# stack(n, expr1, expr2.. exprn) 

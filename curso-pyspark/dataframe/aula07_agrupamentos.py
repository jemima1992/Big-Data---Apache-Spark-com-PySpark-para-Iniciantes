# Databricks notebook source
# DBTITLE 1,Preparando dados e criando o DataFrame
import pyspark.sql.functions as F
data = [("Anderson","Sales","NY",90000,34,10000),
     ("Kenedy","Sales","CA",86000,56,20000),
     ("Billy","Sales","NY",81000,30,23000),
     ("Andy","Finance","CA",90000,24,23000),
     ("Mary","Finance","NY",83000,36,15000),
     ("Keyth","Marketing","CA",80000,25,18000),
     ("Truman","Marketing","NY",91000,50,21000)
    ]

schema = ["emp_name","dep_name","state","salary","age","bonus"]
df = spark.createDataFrame(data=data, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Sum
df.groupBy(F.col("dep_name")).sum("salary").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Count
df.groupBy(F.col("dep_name")).count().show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Min
df.groupBy(F.col("dep_name")).min("salary").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Max
df.groupBy(F.col("dep_name")).max("salary").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Mean
df.groupBy(F.col("dep_name")).mean("salary").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,AVG
df.groupBy(F.col("dep_name")).avg("salary").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Groupyby com multiplas colunas
df.groupBy(F.col("dep_name"),F.col("state")).mean("salary","bonus").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Agg
df.groupBy(F.col("dep_name"))\
.agg(F.sum("salary").alias("Sum_salary"), \
     F.avg("salary").alias("Avg_salary"), \
     F.sum("bonus").alias("Sum_bonus"), \
     F.max("bonus").alias("Max_bonus"), \
     F.min("bonus").alias("Min_bonus")
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Filtrando DataFrame Agrupado
df.groupBy(F.col("dep_name"))\
.agg(F.sum("salary").alias("Sum_salary"), \
     F.avg("salary").alias("Avg_salary"), \
     F.sum("bonus").alias("Sum_bonus"), \
     F.max("bonus").alias("Max_bonus"), \
     F.min("bonus").alias("Min_bonus")
).where(F.col("Sum_salary") >= 79000 ).show(truncate=False)
# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# COMMAND ----------

rdd = sc.parallelize(numbers)

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Visualizar partições com seus valores
rdd.glom().collect()

# COMMAND ----------

# DBTITLE 1,Reduzindo o número de partições
rdd2 = rdd.coalesce(1)

# COMMAND ----------

rdd2.getNumPartitions()

# COMMAND ----------

rdd2.glom().collect()

# COMMAND ----------

# DBTITLE 1,Aumenta as partições mas faz um shuffle nos dados
rdd3 = rdd.repartition(4)

# COMMAND ----------

rdd3.getNumPartitions()

# COMMAND ----------

rdd3.glom().collect()

# COMMAND ----------

df = spark.read.option("delimiter",";").option("header","true").csv("/FileStore/tables/covid/arquivo_geral-2.csv")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df_repartition2 = df.repartition(F.col("regiao"))

# COMMAND ----------

df_partition = df.rdd.glom().collect()

# COMMAND ----------

df_repartition2 = df_partition.repartition(200)
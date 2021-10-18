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

# DBTITLE 1,Criação de um Dataframe para exemplo de particionamento por coluna
df = spark.read.option("delimiter",";").option("header","true").csv("/FileStore/tables/covid/arquivo_geral.csv")


# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df_partition = df.repartition(F.col("regiao"))

# COMMAND ----------

df_partition.rdd.glom().collect()

# COMMAND ----------

df_repartition2 = df_partition.repartition(200)

# COMMAND ----------

df_repartition2.rdd.glom().collect()

# COMMAND ----------

df_repartition2.rdd.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Função para imprimir detalhes do particionamento
def print_partitions(df):
  numPartitions = df.rdd.getNumPartitions()
  print("Total partitions: {}".format(numPartitions))
  df.explain()
  parts =df.rdd.glom().collect()
  i = 0
  j = 0
  for p in parts:
    print("Partition {}:".format(4))
    for r in p:
        print("Row {}:{}".format(j, r))
        j = j+1
    i = i+1    

# COMMAND ----------

print_partitions(df_repartition2)

# COMMAND ----------

df_repartition2.write.partitionBy("regiao","estado").mode("overwrite").csv("/FileStore/tables/covid/exemplo02/")

# COMMAND ----------


     df= (df.withColumn("Dia", F.substring(F.col("data"),9,2).cast("integer")).
                                withColumn("Mes", F.substring(F.col("data"), 6, 2).cast("integer")).
                                withColumn("Ano", F.substring(F.col("data"), 1, 4).cast("integer"))
                             )
 


# COMMAND ----------

df.write.partitionBy("mes","dia").mode("overwrite").csv("/FileStore/tables/covid/exemplo03/")

# COMMAND ----------

display(df)
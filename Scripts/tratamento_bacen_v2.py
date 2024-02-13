# Databricks notebook source
from pyspark.sql.types import *
from datetime import date, timedelta

data_atual = date.today()
td = timedelta(1)
data_atual = data_atual - td

# COMMAND ----------

# MAGIC %md ### LENDO O CSV SALVO NO DATA LAKE

# COMMAND ----------

path = "dbfs:/mnt/dados_bacen2/bronze/tabela_{}.csv".format(data_atual)
dados = spark.read.csv(path)

# COMMAND ----------

# MAGIC %md ### REALIZANDO O TRATAMENTO DO CSV

# COMMAND ----------


schema = StructType([
    StructField("_c0", StringType(),False),
    StructField("Quantidade", DecimalType(),False),
    StructField("Valor", FloatType(),False),
    StructField("Categoria", StringType(),False),
    StructField("Denominacao", FloatType(),False),
    StructField("Especie", StringType(),False),
    StructField("Date_atu", DateType(),False)
])

dados_v2 = (spark.read.option("header", True).schema(schema).csv(path))
dados_v2 = dados_v2.drop("_c0")

# COMMAND ----------

# MAGIC %md ### SALVANDO A TABELA ANALITICA NO HIVE

# COMMAND ----------

dados_v2.write.mode("append").format("Delta").saveAsTable("dados_bacen_empilhado")

# COMMAND ----------

dados_v2.write.mode("append").format('delta').save('/mnt/dados_bacen2/silver/bacen_historico')

# COMMAND ----------

# MAGIC %md ### SALVANDO UMA VERSÃO ANALITICA NO DATALAKE

# COMMAND ----------

dados_v2.write.mode("overwrite").format('delta').save('/mnt/dados_bacen2/gold/tabela_bacen_{}'.format(data_atual))

# COMMAND ----------

# MAGIC %md ### CRIANDO UMA TABELA DO ANO ATUAL PARA CONSUMO ANALITICO

# COMMAND ----------

# MAGIC %sql
# MAGIC replace table default.dados_bacen_BI as
# MAGIC select Quantidade,
# MAGIC        Valor,
# MAGIC        Categoria,
# MAGIC        Denominacao,
# MAGIC        Especie,
# MAGIC        Date_atu
# MAGIC         from dados_bacen_empilhado
# MAGIC                 where Date_atu = (SELECT MAX(Date_atu) from dados_bacen_empilhado)

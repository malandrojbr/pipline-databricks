# Databricks notebook source
import pandas as pd
import requests
import numpy as np
from datetime import date, timedelta

data_atual = date.today()
td = timedelta(1)
data_atual = data_atual - td
print(data_atual) 


# COMMAND ----------

# MAGIC %md ### VERIFICA O ACESSO AO DIRETORIO DBFS NO DATALAKE

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.ls("/mnt/dados_bacen/historico_circulacao")

# COMMAND ----------

# MAGIC %md ### LENDO O CSV SALVO NO DATA LAKE

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC
# MAGIC path = "dbfs:/mnt/dados_bacen/historico_circulacao/tabela_{}.csv".format(data_atual)
# MAGIC dados = spark.read.csv(path)
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %md ### REALIZANDO O TRATAMENTO DO CSV

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("_c0", StringType(),False),
# MAGIC     StructField("Quantidade", DecimalType(),False),
# MAGIC     StructField("Valor", FloatType(),False),
# MAGIC     StructField("Categoria", StringType(),False),
# MAGIC     StructField("Denominacao", FloatType(),False),
# MAGIC     StructField("Especie", StringType(),False),
# MAGIC     StructField("Date_atu", DateType(),False)
# MAGIC ])
# MAGIC
# MAGIC
# MAGIC
# MAGIC dados_v2 = (spark.read.option("header", True).schema(schema).csv(path))
# MAGIC
# MAGIC dados_v2 = dados_v2.drop("_c0")
# MAGIC
# MAGIC display(dados_v2)

# COMMAND ----------

# MAGIC %md ### SALVANDO A TABELA ANALITICA NO HIVE

# COMMAND ----------

# MAGIC %python
# MAGIC dados_v2.write.mode("append").format("parquet").saveAsTable("dados_bacen_empilhado")

# COMMAND ----------

dados_v2.write.mode("append").format('parquet').save('/mnt/dados_bacen/historico_analitico/tabela_bacen_empilhada')

# COMMAND ----------

# MAGIC %md ### SALVANDO UMA VERSÃO ANALITICA NO DATALAKE

# COMMAND ----------

# MAGIC %python
# MAGIC dados_v2.write.mode("overwrite").format('parquet').save('/mnt/dados_bacen/historico_analitico/tabela_bacen_{}'.format(data_atual))

# COMMAND ----------

# MAGIC %md ### VERIFICANDO SE A O CONTEUDO FOI SALVO COM SUCESSO

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.format("parquet").load("/mnt/dados_bacen/historico_analitico/tabela_bacen_{}".format(data_atual))
# MAGIC display(df)
# MAGIC
# MAGIC df.printSchema()

# COMMAND ----------

# MAGIC %python
# MAGIC df = spark.read.format("parquet").load("/mnt/dados_bacen/historico_analitico/tabela_bacen_empilhada")
# MAGIC display(df)
# MAGIC
# MAGIC df.printSchema()

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

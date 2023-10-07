// Databricks notebook source
// MAGIC %md ### VERIFICA O ACESSO AO DIRETORIO DBFS NO DATALAKE

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados_bacen/historico_circulacao")

// COMMAND ----------

// MAGIC %md ### LENDO O CSV SALVO NO DATA LAKE

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC path = "dbfs:/mnt/dados_bacen/historico_circulacao/tabela_v2.csv"
// MAGIC dados = spark.read.csv(path)
// MAGIC

// COMMAND ----------

// MAGIC %md ### REALIZANDO O TRATAMENTO DO CSV

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC
// MAGIC schema = StructType([
// MAGIC     StructField("_c0", StringType(),False),
// MAGIC     StructField("Data", DateType(),False),
// MAGIC     StructField("Quantidade", DecimalType(),False),
// MAGIC     StructField("Valor", FloatType(),False),
// MAGIC     StructField("Denominacao", FloatType(),False),
// MAGIC     StructField("Especie", StringType(),False)
// MAGIC ])
// MAGIC
// MAGIC
// MAGIC
// MAGIC dados_v2 = (spark.read.option("header", True).schema(schema).csv(path))
// MAGIC
// MAGIC dados_v2 = dados_v2.drop("_c0")
// MAGIC
// MAGIC display(dados_v2)

// COMMAND ----------

// MAGIC %md ### SALVANDO A TABELA ANALITICA NO HIVE

// COMMAND ----------

// MAGIC %python
// MAGIC dados_v2.write.format("parquet").saveAsTable("dados_bacen_v4")

// COMMAND ----------

// MAGIC %md ### SALVANDO UMA VERSÃO ANALITICA NO DATALAKE

// COMMAND ----------

// MAGIC %python
// MAGIC dados_v2.write.format('parquet').save('/mnt/dados_bacen/silver/tabela_bacen_v2')

// COMMAND ----------

// MAGIC %md ### VERIFICANDO SE A O CONTEUDO FOI SALVO COM SUCESSO

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.read.format("parquet").load("/mnt/dados_bacen/silver/tabela_bacen_v2")
// MAGIC display(df)
// MAGIC
// MAGIC df.printSchema()

// COMMAND ----------

// MAGIC %md ### CRIANDO UMA TABELA DO ANO ATUAL PARA CONSUMO ANALITICO

// COMMAND ----------

// MAGIC %sql
// MAGIC create table default.dados_bacen_tratado as 
// MAGIC select * from dados_bacen_v4 where Data between '2023-01-01' and '2023-12-31'

// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(df.select("anuncio.*","anuncio.endereco.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

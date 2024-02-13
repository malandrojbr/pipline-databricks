// Databricks notebook source
// MAGIC %md ### MONTA O DIRETORIO DENTRO DO DATA LAKE NO DBFS

// COMMAND ----------

dbutils.fs.mkdirs("/mnt/dados_bacen")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt")

// COMMAND ----------

// MAGIC %md ### CONFIGURA A CONEXÃO COM O DATA LAKE AZURE

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> cliente_id,
  "fs.azure.account.oauth2.client.secret" -> cliente_secret,
  "fs.azure.account.oauth2.client.endpoint" -> cliente_endpoint)
// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bacen@datalakedatamaster2.dfs.core.windows.net/",
  mountPoint = "/mnt/dados_bacen",
  extraConfigs = configs)

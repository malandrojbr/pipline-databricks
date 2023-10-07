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
  "fs.azure.account.oauth2.client.id" -> "c982c8f0-37a1-45a1-a814-1aa5d27e1d5f",
  "fs.azure.account.oauth2.client.secret" -> "6z28Q~3D2U2CWMSwc-UJ2Gb9rHjExLNTTDfE8chB",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/9d471541-1424-4224-8780-f624800ed595/oauth2/token")
// Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bacen@datalakedatamaster2.dfs.core.windows.net/",
  mountPoint = "/mnt/dados_bacen",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %md ### VERIFICA SE OS DIRETORIOS DO DATA LAKE FORAM MONTADOS NA PASTA DENTRO DO DBFS

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados_bacen")

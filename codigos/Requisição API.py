# Databricks notebook source
# MAGIC %md ### API do Banco Central
# MAGIC
# MAGIC https://dadosabertos.bcb.gov.br/dataset?res_format=API

# COMMAND ----------

# MAGIC %md ## INSTALANDO AS BIBLIOTECAS

# COMMAND ----------

import pandas as pd
import requests
import numpy as np
import pandablob
from azure.storage.blob import ContainerClient

# COMMAND ----------

# MAGIC %md ## REQUISIÇÃO DA API

# COMMAND ----------

link = "https://olinda.bcb.gov.br/olinda/servico/mecir_dinheiro_em_circulacao/versao/v1/odata/informacoes_diarias?$top=10000000&$orderby=Data%20desc&$format=json"

requisicao = requests.get(link)
informacoes = requisicao.json()

# COMMAND ----------

# MAGIC %md ### TRANSFORMANDO A VARIVEL EM UM DATAFRAME
# MAGIC

# COMMAND ----------

tabela = pd.DataFrame(informacoes["value"])

# COMMAND ----------

# MAGIC %md ### SALVANDO OS DADOS NO DATA LAKE

# COMMAND ----------

account_url = "https://datalakedatamaster2.blob.core.windows.net"
token = "Ojfw6PpbOeUfrmo9zn0lBnMBdutV4FHtL3y7slsR3XsB03TWue8yrTFtGAkAWknWg/j6rlHqTbW7+AStnfju7g=="
container = "bacen/historico_circulacao"
blobname = "tabela_v2.csv"

container_client = ContainerClient(account_url, container, credential=token)
blob_client = container_client.get_blob_client(blob=blobname)


pandablob.df_to_blob(tabela,blob_client,overwrite=True)

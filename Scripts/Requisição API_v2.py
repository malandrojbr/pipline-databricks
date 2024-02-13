# Databricks notebook source
# MAGIC %md ## INSTALANDO AS BIBLIOTECAS

# COMMAND ----------

!pip install pandablob -q
import pandas as pd
import requests
import numpy as np
import pandablob
from azure.storage.blob import ContainerClient

# COMMAND ----------

# MAGIC %md ## REQUISIÇÃO DA API

# COMMAND ----------

link2 = "https://olinda.bcb.gov.br/olinda/servico/mecir_dinheiro_em_circulacao/versao/v1/odata/informacoes_diarias_com_categoria?$top=100&$orderby=Data%20desc&$format=json"

requisicao = requests.get(link2)
informacoes = requisicao.json()

# COMMAND ----------

# MAGIC %md ### TRANSFORMANDO A VARIVEL EM UM DATAFRAME
# MAGIC

# COMMAND ----------

tabela = pd.DataFrame(informacoes["value"])

tabela['Date_atu'] = pd.to_datetime(tabela['Data']).dt.date

tabela = tabela.drop(columns=['Data'])

# COMMAND ----------

# MAGIC %md #### TRAZENDO A DATA MAIS RECENTE DA TABELA

# COMMAND ----------

from datetime import date, timedelta

data_atual = date.today()
td = timedelta(1)
data_atual = data_atual - td

tabela2 = tabela.where(tabela.Date_atu == data_atual).dropna()

# COMMAND ----------

# MAGIC %md ### SALVANDO OS DADOS NO DATA LAKE

# COMMAND ----------

account_url = "https://datalakedatamaster2.blob.core.windows.net"
token = "Ojfw6PpbOeUfrmo9zn0lBnMBdutV4FHtL3y7slsR3XsB03TWue8yrTFtGAkAWknWg/j6rlHqTbW7+AStnfju7g=="
container = "bacen/bronze"
blobname = "tabela_{}.csv".format(data_atual)

container_client = ContainerClient(account_url, container, credential=token)
blob_client = container_client.get_blob_client(blob=blobname)


pandablob.df_to_blob(tabela2,blob_client,overwrite=True)

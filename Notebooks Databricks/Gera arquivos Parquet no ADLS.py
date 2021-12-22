# Databricks notebook source
# MAGIC %md
# MAGIC ## Gerando os dados da tabela em arquivos Parquet no ADLS
# MAGIC Esse segundo notebook visa criar um estudo de caso para um Pipeline Datafactory, onde no primeiro Notebook eu crio uma tabela de filmes no Databricks a partir de um arquivo CSV no ADLS, esse pega os dados dessa tabela e grava num segundo container do ADLS acionado via DataFactory.

# COMMAND ----------

# DBTITLE 1,Variáveis
db = "movies"
table = "movies_imdb"
tb = db+"."+table
path ="abfss://parquets001@adlsrescue01.dfs.core.windows.net/"
tpath = f"{path}/{db}/{table}"

# COMMAND ----------

# DBTITLE 1,Configurando o acesso ao ADLS
spark.conf.set(
  "fs.azure.account.key.adlsrescue01.dfs.core.windows.net", 
  dbutils.secrets.get(scope="rescue", key="rescuesakey"))

# COMMAND ----------

# DBTITLE 1,Cria um Container chamado parquets001
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://parquets001@adlsrescue01.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# DBTITLE 1,Grava o conteúdo da tabela no formato parquet no novo container
spark.table(tb).write.format("parquet").save(tpath)

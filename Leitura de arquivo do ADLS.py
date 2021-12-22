# Databricks notebook source
# MAGIC %md
# MAGIC # Teste de leitura de arquivo do ADLS
# MAGIC Vamos fazer a leitura de um arquivo CSV do ADLS e criação de uma tabela no Databricks a partir dela, vamos iniciar criando uma conexão com o storage account.
# MAGIC 
# MAGIC Para isso temos que realizar algumas configurações antes:
# MAGIC 
# MAGIC ## Configuração do databricks-cli
# MAGIC Para esse exemplo vamos usar o proprio cluster criado para instalar o darabricks mas o mesmo pode ser feito em qualquer desktop.
# MAGIC 
# MAGIC ## Usando o Terminal do cluster criado.
# MAGIC Configurações do Cluster existe a opção APP e entre elas a opção terminal, caso ela esteja desativada, vá em `Admin Console`e escolha `Workspace Settings`, digite terminal e habilite a função, com isso podemos acessar o bash do cluster.
# MAGIC 
# MAGIC ## Criando um Token pro databricks-cli
# MAGIC Aindo no Workspace do Databricks, em `User Settings` existe a opção `Access Tokens`, crie um token e guarde o hash gerado, você vai precisar na configuração do Databricks.
# MAGIC 
# MAGIC ### Instalando o Databricks-cli
# MAGIC Via `pip` rodamos o seguinte comando: `pip install databricks-cli` isso vai instalar o client, porém devemos configura-lo. Crie um arquivo conforme mostrado abaixo via `vim`.
# MAGIC 
# MAGIC ``` vim /databricks/driver/.databrickscfg ```
# MAGIC 
# MAGIC Coloque o seguinte conteudo no arquivo:
# MAGIC ```
# MAGIC [DEFAULT]
# MAGIC host = https://adb-8941059639417143.3.azuredatabricks.net
# MAGIC token = dapiec0eb6df251f51d257052ab27327d7e4
# MAGIC ```
# MAGIC 
# MAGIC Sendo o Token acima o token que você gerou no Workspace do Databricks.
# MAGIC 
# MAGIC ### Criando o Scope e a Key via databricks-cli
# MAGIC Para que possamos acessar o Storage account, precisamos criar um scope via databricks-cli e criar uma key pra esse scope, onde nela colocamos a key account do Storage Account ADLS onde iremos trabalhar os arquivos. Faremos isso em 2 comandos:
# MAGIC * `databricks secrets create-scope --scope rescue --initial-manage-principal "users"`
# MAGIC * `databricks secrets put --scope rescue --key rescuesakey`
# MAGIC 
# MAGIC Nos comandos acima criamos um scope de nome `rescue` e criamos uma key para o escopo rescue de nome `rescuesakey`.
# MAGIC O segundo comando pede pra escolher um editor de texto, pegamos a key account do storage account lá no portal da azure, colocamos no editor escolhido e salvamos.
# MAGIC 
# MAGIC Sendo assim a parte de scopo está pronta para usarmos no Spark a seguir.

# COMMAND ----------

# DBTITLE 1,Configurando o Storage Account pro SPARK
spark.conf.set(
  "fs.azure.account.key.adlsrescue01.dfs.core.windows.net", 
  dbutils.secrets.get(scope="rescue", key="rescuesakey"))

# COMMAND ----------

# DBTITLE 1,Cria um conteiner chamado rescue001 no Storage Account
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://rescue001@adlsrescue01.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trabalhando com os dados
# MAGIC Criei uma pasta chamada `movies` dentro do Storage Account, lá eu joguei o conteúdo de um dataset de filmes que peguei no Kaggle, o link segue abaixo:
# MAGIC https://www.kaggle.com/harshitshankhdhar/imdb-dataset-of-top-1000-movies-and-tv-shows

# COMMAND ----------

# DBTITLE 1,Listando o conteúdo da pasta movies do container rescue001
dbutils.fs.ls("abfss://rescue001@adlsrescue01.dfs.core.windows.net/movies")

# COMMAND ----------

# DBTITLE 1,Mostrando as primeiras linhas do arquivo movies_metadata
# MAGIC %fs head 'abfss://rescue001@adlsrescue01.dfs.core.windows.net/movies/imdb_top_1000.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando um dataframe Spark e persistindo os dados numa tabela
# MAGIC Vamos criar um dataframe chamado `movies` com base no conteúdo desse arquivo e posteriormente vamos criar um database chamado movies e por sua vêz uma tabela com o mesmo nome do arquivo, note que se trata de um .csv e na criação do dataframe vamos usar o `InferSchema` para que ele possa identificar o melhor tipo de data type para cada coluna.

# COMMAND ----------

movies_metadata_csv = "abfss://rescue001@adlsrescue01.dfs.core.windows.net/movies/imdb_top_1000.csv"

movies_metadata = (spark.read                        
   .option("header", "true")                 
   .option("inferSchema", "true")
   .csv(movies_metadata_csv)                   
   )

display(movies_metadata)

# COMMAND ----------

# DBTITLE 1,Verificando os Data Types do Dataframe
movies_metadata.printSchema()

# COMMAND ----------

# DBTITLE 1,Tratando o dataframe deixando só os dados que preciso e organizando por Ratting
movies_metadata = (spark.read                        
   .option("header", "true")                 
   .option("inferSchema", "true")
   .csv(movies_metadata_csv)
   .select("Series_Title","Released_Year","IMDB_Rating")
   .orderBy("IMDB_Rating", ascending=False)
   )

display(movies_metadata)

# COMMAND ----------

# DBTITLE 1,Criando uma Temp Table no Databricks com o Dataframe
movies_metadata.createOrReplaceTempView("movies_metadata_tmpview")

# COMMAND ----------

# DBTITLE 1,Rodando um Select na tempView criada
# MAGIC %sql
# MAGIC select Series_Title 
# MAGIC from movies_metadata_tmpview;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando um database e uma tabela com base no Dataframe
# MAGIC Vamos fazer uma criação de uma tabela com base nos dados tratados do dataset para consulta de uma aplicação terceira ou qualquer tipo de analise necessária.
# MAGIC Vamos criar um database de nome `movies` e uma tabela com o nome de `movies_imdb`.

# COMMAND ----------

# DBTITLE 1,Cria o database Movies caso ele não exista e exclui a tabela caso exista.
# MAGIC %sql
# MAGIC create database if not exists movies;
# MAGIC drop table if exists movies.movies_imdb;

# COMMAND ----------

# DBTITLE 1,Cria a tabela movies_imdb com base no Dataframe movies_metadata
movies_metadata.write.mode("overwrite").saveAsTable("movies.movies_imdb")

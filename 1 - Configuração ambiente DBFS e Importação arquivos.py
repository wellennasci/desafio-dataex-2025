# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração do Ambiente DBFS e Importação de Arquivos na Landing Zone
# MAGIC
# MAGIC  Este notebook tem como objetivo configurar o ambiente no DBFS e importar arquivos para a Landing Zone, que é o primeiro estágio do nosso pipeline de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bibliotecas Utilizadas
# MAGIC Para realizar as operações de download e manipulação de arquivos, utilizamos a seguinte biblioteca:

# COMMAND ----------

import urllib.request

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Criando Diretórios no DBFS
# MAGIC Para organizar o armazenamento dos dados, criamos os seguintes diretórios no DBFS:
# MAGIC
# MAGIC - Landing Zone: onde os dados brutos serão inicialmente armazenados.
# MAGIC - Bronze: estágio de dados brutos para posterior transformação.
# MAGIC - Silver: estágio de dados refinados e estruturados.
# MAGIC - Gold: estágio final com dados prontos para análise.
# MAGIC
# MAGIC

# COMMAND ----------

# Criando diretorios no DBFS
dbutils.fs.mkdirs("/mnt/lhdw/landingzone")
dbutils.fs.mkdirs("/mnt/lhdw/bronze")
dbutils.fs.mkdirs("/mnt/lhdw/silver")
dbutils.fs.mkdirs("/mnt/lhdw/gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Baixando e Movendo Arquivos para o DBFS
# MAGIC Agora, vamos baixar os arquivos da base de dados  e movê-los para o diretório LandingZone no DBFS.

# COMMAND ----------

# Baixando e movendo os arquivos para o DBFS

#categorias
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/categorias.csv"
temp_path = "/tmp/categorias.csv"
dbfs_path = "/mnt/lhdw/landingzone/categorias.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#produtos
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/produtos.csv"
temp_path = "/tmp/produtos.csv"
dbfs_path = "/mnt/lhdw/landingzone/produtos.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#clientes
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/clientes.csv"
temp_path = "/tmp/clientes.csv"
dbfs_path = "/mnt/lhdw/landingzone/clientes.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#cidades
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/cidades.csv"
temp_path = "/tmp/cidades.csv"
dbfs_path = "/mnt/lhdw/landingzone/cidades.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#paises
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/paises.csv"
temp_path = "/tmp/paises.csv"
dbfs_path = "/mnt/lhdw/landingzone/paises.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#vendedores
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/vendedores.csv"
temp_path = "/tmp/vendedores.csv"
dbfs_path = "/mnt/lhdw/landingzone/vendedores.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

#vendas
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/vendas_1.csv"
temp_path = "/tmp/vendas_1.csv"
dbfs_path = "/mnt/lhdw/landingzone/vendas_1.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

print("Todos os arquivos foram baixados e salvos na Landing Zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC Após o download e a movimentação dos arquivos, podemos verificar os arquivos armazenados na Landing Zone.

# COMMAND ----------

# Evidenciando os arquivos Criados
display(dbutils.fs.ls("/mnt/lhdw/landingzone/"))

# COMMAND ----------

# MAGIC %md
# MAGIC Agora, vamos carregar e exibir a tabela de vendas para verificar o conteúdo dos dados importados.

# COMMAND ----------

#evidenciando a tabela fato
# Caminho para o arquivo CSV
vendas_exibir = spark.read.csv("/mnt/lhdw/landingzone/vendas_1.csv", header=True, inferSchema=True)

# Exibir os dados
display(vendas_exibir)

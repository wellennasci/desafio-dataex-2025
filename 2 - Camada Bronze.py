# Databricks notebook source
# MAGIC %md
# MAGIC ## **Camada Bronze - Processamento e Armazenamento dos Dados**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Importação das Bibliotecas**
# MAGIC O módulo SparkSession gerencia a sessão do Spark, functions fornece funções para manipulação de dados e types define tipos de dados estruturados.

# COMMAND ----------

# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração da SparkSession
# MAGIC Aqui, a SparkSession é criada com configurações otimizadas:

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Load Data Bronze") \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definição dos Caminhos no Data Lake
# MAGIC Definindo os caminhos para a Landing Zone (origem dos arquivos) e a camada Bronze (armazenamento em Delta Lake).

# COMMAND ----------

lz_path = "/mnt/lhdw/landingzone"
bronze_path = "/mnt/lhdw/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos Arquivos da Landing Zone
# MAGIC Cada arquivo CSV armazenado na Landing Zone é lido e convertido em um DataFrame, com:
# MAGIC
# MAGIC header=True: Considera a primeira linha como cabeçalho.
# MAGIC
# MAGIC inferSchema=True: Detecta automaticamente o tipo de cada coluna.

# COMMAND ----------

#Lendo os arquivos na camada Landing zone
categorias_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/categorias.csv", header=True, inferSchema=True)
produtos_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/produtos.csv", header=True, inferSchema=True)
clientes_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/clientes.csv", header=True, inferSchema=True)
cidades_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/cidades.csv", header=True, inferSchema=True)
paises_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/paises.csv", header=True, inferSchema=True)
vendedores_df =  spark.read.option("delimiter", ",").csv(f"{lz_path}/vendedores.csv", header=True, inferSchema=True)
vendas_df = spark.read.option("delimiter", ",").csv(f"{lz_path}/vendas_1.csv", header=True, inferSchema=True)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvando os Dados na Camada Bronze
# MAGIC
# MAGIC Os dados são convertidos e armazenados no Delta Lake, permitindo versionamento e maior eficiência.
# MAGIC
# MAGIC format("delta"): Define o formato Delta.
# MAGIC
# MAGIC mode("overwrite"): Substitui os arquivos caso já existam.

# COMMAND ----------

# Salvando os DataFrames no formato Delta em uma estrutura de diretórios no caminho bronze
categorias_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/categorias")
produtos_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/produtos")
clientes_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/clientes")
cidades_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/cidades")
paises_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/paises")
vendedores_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/vendedores")
vendas_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liberação de Memória
# MAGIC A função unpersist() libera memória ao remover os DataFrames do cache, otimizando o uso de recursos.

# COMMAND ----------

# Liberando a memória após salvar os DataFrames
categorias_df.unpersist()
produtos_df.unpersist()
clientes_df.unpersist()
cidades_df.unpersist()
paises_df.unpersist()
vendedores_df.unpersist()
vendas_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exibição dos Arquivos Salvos na Camada Bronze

# COMMAND ----------

# Evidenciando os arquivos salvos na camada bronze
display(dbutils.fs.ls("/mnt/lhdw/bronze/"))

# COMMAND ----------

# Verificando arquivos Parquet no diretório
display(dbutils.fs.ls("/mnt/lhdw/bronze/vendas/"))


# COMMAND ----------

#evidenciando a tabela fato
vendas_exibir = spark.read.format("delta").load("/mnt/lhdw/bronze/vendas")
display(vendas_exibir)
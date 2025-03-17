# Databricks notebook source
# MAGIC %md
# MAGIC #  Carga Incremental
# MAGIC Carga incremental de dados de vendas na camada Silver. Realização da leitura, transformação e escrita dos dados em formato Delta Parquet, garantindo que apenas os dados novos sejam carregados e processados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação das Bibliotecas

# COMMAND ----------

# Importando as bibliotecas necessárias
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Inicialização do SparkSession

# COMMAND ----------

# Iniciando a SparkSession
spark = SparkSession.builder \
    .appName("Incremental Load Silver") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição de Caminhos de Armazenamento

# COMMAND ----------

# Definindo caminhos de armazenamento no Data Lake
lz_path = "/mnt/lhdw/landingzone"
bronze_path = "/mnt/lhdw/bronze"
silver_path = "/mnt/lhdw/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos Dados

# COMMAND ----------

# Caminhos para os arquivos CSV fornecidos
url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/vendas_2.csv"
temp_path = "/tmp/vendas_2.csv"
dbfs_path = "/mnt/lhdw/landingzone/vendas_2.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")

url = "https://github.com/andrerosa77/trn-dwe-2025/raw/main/vendas_3.csv"
temp_path = "/tmp/vendas_3.csv"
dbfs_path = "/mnt/lhdw/landingzone/vendas_3.csv"
urllib.request.urlretrieve(url, temp_path)
dbutils.fs.cp(f"file:{temp_path}", f"dbfs:{dbfs_path}")


# COMMAND ----------

# Carregando os dados das bases vendas_2.csv e vendas_3.csv
vendas_2_df = spark.read.csv(f"{lz_path}/vendas_2.csv", header=True, inferSchema=True)
vendas_3_df = spark.read.csv(f"{lz_path}/vendas_3.csv", header=True, inferSchema=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Salvando os Dados na Camada Bronze
# MAGIC Os dados carregados das vendas são salvos na camada Bronze em formato Delta, utilizando o modo overwrite para sobrescrever qualquer dado existente.

# COMMAND ----------

# Salvando os DataFrames no formato Delta em uma estrutura de diretórios no caminho bronze
vendas_2_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/vendas_2")
vendas_3_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/vendas_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ##  União das Bases de Dados
# MAGIC As duas bases de dados de vendas (vendas_2 e vendas_3) são unidas em um único DataFrame, para facilitar o processamento posterior.

# COMMAND ----------

# Unindo as duas bases de vendas (vendas_2 e vendas_3)
vendas_df = vendas_2_df.union(vendas_3_df)
display(vendas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação da Última Data Processada na Camada Silver
# MAGIC O código verifica a última data de venda já processada na camada Silver, buscando o valor máximo da coluna DataVenda. Caso não existam dados na camada Silver, last_data_venda será None.
# MAGIC
# MAGIC

# COMMAND ----------

# Verificando qual a última DataVenda já processada na camada Silver
#last_data_venda recebe o valor máximo da venda
try:
    silver_df = spark.read.format("delta").load(f"{silver_path}/vendas")
    last_data_venda = silver_df.agg(max("DataVenda")).collect()[0][0]
except Exception as e:
    last_data_venda = None
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtragem dos Dados Incrementais

# COMMAND ----------

# Verificando se existem dados novos (incremental)
if last_data_venda:
    # Carregando os dados novos a partir da última DataVenda
    novas_vendas_df = vendas_df.filter(col("DataVenda") > last_data_venda)
else:
    # Se não houver dados processados na Silver, carregar todos os dados
    novas_vendas_df = vendas_df   
  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformações e Limpeza dos Dados
# MAGIC - A coluna NumeroTransacao é removida do DataFrame;
# MAGIC - O preço unitário dos produtos é adicionado à tabela de vendas por meio de um join com a tabela produtos_df. A coluna PrecoTotal é calculada multiplicando a Quantidade pela PrecoUnitario;
# MAGIC - São adicionadas as colunas de Ano e Mês para particionamento dos dados.

# COMMAND ----------

# Aplicando as transformações na camada Silver
novas_vendas_df = novas_vendas_df.drop("NumeroTransacao")

#Calculando o preco total
produtos_df = spark.read.format("delta").load(f"{bronze_path}/produtos")

# Fazer o join da tabela de vendas com os preços históricos
vendas_com_preco_df = novas_vendas_df.alias("v") \
    .join(produtos_df.alias("p"), 
          (col("v.ProdutoID") == col("p.ProdutoID")) & 
          (col("p.DataCadastro") <= col("v.DataVenda")), 
          "left") \
    .select(
        col("v.*"), 
        round(col("p.Preco"), 2).alias("PrecoUnitario")
    )

# Calculando o PrecoTotal considerando o Desconto(%)
novas_vendas_df = vendas_com_preco_df.withColumn(
    "PrecoTotal", round(col("Quantidade") * col("PrecoUnitario") * (1 - col("Desconto")), 2)
)

# Organizando as colunas (reordenando as colunas para deixar "PrecoUnitario" depois de "Quantidade")
novas_vendas_df = novas_vendas_df.select(
    "VendasID", "VendedorID", "ClienteID", "ProdutoID", "Quantidade", "PrecoUnitario", "Desconto", "PrecoTotal", "DataVenda"
)

# Adicionar colunas de Ano e Mês para particionamento
vendas_df  = novas_vendas_df \
    .withColumn("Ano", year(col("DataVenda"))) \
    .withColumn("Mes", month(col("DataVenda")))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando dos Dados Incrementais na Camada Silver

# COMMAND ----------

# Salvando os dados incrementais na camada Silver em formato Delta Parquet
vendas_df.write.format("delta").mode("append").partitionBy("Ano", "Mes").save(f"{silver_path}/vendas")

# Confirmando a carga
print("Carga incremental na camada Silver concluída com sucesso!!!")

display(vendas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Exibindo Arquivos e Tabelas Salvas

# COMMAND ----------

# Evidenciando os arquivos salvos na camada silver
display(dbutils.fs.ls("/mnt/lhdw/silver/"))

# COMMAND ----------

# Evidenciando os arquivos particionados
display(dbutils.fs.ls("/mnt/lhdw/silver/vendas/"))

# COMMAND ----------

# Evidenciando os arquivos particionados
display(dbutils.fs.ls("/mnt/lhdw/silver/vendas/Ano=2018"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Após carga incremental é necessário rodar novamente a camada Gold.**
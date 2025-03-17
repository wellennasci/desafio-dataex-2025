# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver - Transformação
# MAGIC Transformação dos dados da camada Bronze para a camada Silver no Data Lake, aplicando tratamentos, ajustes e garantindo a qualidade dos dados antes de serem consumidos por outras camadas ou análises.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação de Bibliotecas

# COMMAND ----------

# Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import year, month
from pyspark.sql.functions import col, format_number, regexp_replace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração da SparkSession

# COMMAND ----------

# Iniciar a SparkSession com configurações otimizadas
spark = SparkSession.builder \
    .appName("Transformação Camada Silver") \
    .config("spark.sql.shuffle.partitions", "200")  \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição de Caminhos no Data Lake

# COMMAND ----------

# Definindo caminhos de armazenamento no Data Lake
bronze_path = "/mnt/lhdw/bronze"
silver_path = "/mnt/lhdw/silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Dados da Camada Bronze

# COMMAND ----------

produtos_df = spark.read.format("delta").load(f"{bronze_path}/produtos")
categorias_df = spark.read.format("delta").load(f"{bronze_path}/categorias")
cidades_df = spark.read.format("delta").load(f"{bronze_path}/cidades")
clientes_df = spark.read.format("delta").load(f"{bronze_path}/clientes")
paises_df = spark.read.format("delta").load(f"{bronze_path}/paises")
vendedores_df = spark.read.format("delta").load(f"{bronze_path}/vendedores")
vendas_df = spark.read.format("delta").load(f"{bronze_path}/vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformações Aplicadas
# MAGIC
# MAGIC **1. Tabela Clientes**
# MAGIC
# MAGIC - Unificação das colunas de nome (PrimeiroNome, NomeDoMeio, UltimoNome) em uma única coluna Nome.
# MAGIC - Remoção das colunas antigas de nome.
# MAGIC - Reorganização as colunas para manter ClienteID e Nome no início.
# MAGIC
# MAGIC **2. Tabela Vendedores**
# MAGIC
# MAGIC - Unificação das colunas de nome (PrimeiroNome, NomeDoMeio, UltimoNome) em uma única coluna Nome.
# MAGIC - Remoção das colunas antigas de nome.
# MAGIC - Reorganização das colunas para manter VendedorID e Nome no início.
# MAGIC
# MAGIC **3. Tabela Vendas**
# MAGIC
# MAGIC - Remoção da coluna NumeroTransacao.
# MAGIC - Cálculo do PrecoTotal multiplicando a quantidade pelo preço unitário do produto na data da venda e aplicando o desconto.
# MAGIC - Cria as colunas Ano e Mes a partir da DataVenda para particionamento.

# COMMAND ----------

#Explorando a tabela clientes
display(clientes_df)

# COMMAND ----------


# Unificando as colunas de nome da tabela clientes
clientes_df = clientes_df.withColumn("Nome", concat_ws(" ", "PrimeiroNome", "NomeDoMeio", "UltimoNome"))

# Removendo as colunas antigas de nome
clientes_df = clientes_df.drop("PrimeiroNome", "NomeDoMeio", "UltimoNome")

# Regorganizando as colunas 
cols = clientes_df.columns
cliente_id_index = cols.index('ClienteID')
new_cols = cols[:cliente_id_index + 1] + ['Nome'] + [col for col in cols if col not in ['Nome', 'ClienteID']]
clientes_df = clientes_df.select(new_cols)

display(clientes_df)


# COMMAND ----------

#Explorando a tabela vendedores
display(vendedores_df)

# COMMAND ----------


# Unificando as colunas de nome da tabela vendedores
vendedores_df = vendedores_df.withColumn("Nome", concat_ws(" ", "PrimeiroNome", "NomeDoMeio", "UltimoNome"))

# Removendo as colunas antigas de nome
vendedores_df = vendedores_df.drop("PrimeiroNome", "NomeDoMeio", "UltimoNome")

# Regorganizando as colunas 
cols = vendedores_df.columns
vendedores_id_index = cols.index('VendedorID')
new_cols = cols[:vendedores_id_index + 1] + ['Nome'] + [col for col in cols if col not in ['Nome', 'VendedorID']]
vendedores_df = vendedores_df.select(new_cols)

display(vendedores_df)

# COMMAND ----------

#Explorando a tabela vendas
display(vendas_df)


# COMMAND ----------

# Eliminando a coluna 'NumeroTransacao' do DataFrame de vendas
vendas_df = vendas_df.drop('NumeroTransacao')
display(vendas_df)


# COMMAND ----------

# Fazendo o join da tabela de vendas com a tabela de produtos
vendas_com_preco_df = vendas_df.alias("v") \
    .join(produtos_df.alias("p"), 
          (col("v.ProdutoID") == col("p.ProdutoID")) & 
          (col("p.DataCadastro") <= col("v.DataVenda")), 
          "left") \
    .select(
        col("v.*"), 
        round(col("p.Preco"), 2).alias("PrecoUnitario")
    )

# Calculando o PrecoTotal considerando o Desconto(%)
vendas_df = vendas_com_preco_df.withColumn(
    "PrecoTotal", round(col("Quantidade") * col("PrecoUnitario") * (1 - col("Desconto")), 2)
)

# Organizando as colunas 
vendas_df = vendas_df.select(
    "VendasID", "VendedorID", "ClienteID", "ProdutoID", "Quantidade", "PrecoUnitario", 
    "Desconto", "PrecoTotal", "DataVenda"
)

# Exibindo os dados
display(vendas_df)



# COMMAND ----------

# MAGIC %md
# MAGIC > Durante a transformação, o PrecoTotal foi calculado multiplicando a quantidade pelo preço unitário do produto na data da venda. No entanto, percebi que alguns valores de venda ficaram nulos. Isso ocorre porque a data da venda está menor que a data de cadastro do produto, o que impossibilita associar um preço válido a essa transação. Por uma opção didática decidi não transformar os valores nulos em zero (0.00).

# COMMAND ----------

produtos_df.filter(col("ProdutoID") == 101).select("ProdutoID", "DataCadastro").show()


# COMMAND ----------

# Criando as colunas de Ano e Mês da tabela vendas
vendas_df = vendas_df.withColumn("Ano", year("DataVenda")) \
                     .withColumn("Mes", month("DataVenda"))
display(vendas_df)                     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravaçao dos Dados na Camada Silver
# MAGIC Os dados transformados são salvos na camada Silver no formato Delta:

# COMMAND ----------

# Tabela Vendas (com particionamento por Ano e Mês)
vendas_df.write.format("delta") \
    .partitionBy("Ano", "Mes") \
    .mode("overwrite") \
    .save(f"{silver_path}/vendas")

# Tabela Clientes 
clientes_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/clientes")

# Tabela Vendedores 
vendedores_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/vendedores")

# Tabela Produtos
produtos_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/produtos")

# Tabela Categorias 
categorias_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/categorias")

# Tabela Cidades 
cidades_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/cidades")

# Tabela Paises
paises_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/paises")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Liberação de Memória
# MAGIC Após salvar os DataFrames, é utilizada a função unpersist() para liberar a memória.

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
# MAGIC ## Validação dos Arquivos Salvos

# COMMAND ----------

# Evidenciando os arquivos salvos na camada silver
display(dbutils.fs.ls("/mnt/lhdw/silver/"))

# COMMAND ----------

# Evidenciando os arquivos particionados
display(dbutils.fs.ls("/mnt/lhdw/silver/vendas/Ano=2018"))


# COMMAND ----------

#evidenciando a tabela fato
vendas_exibir = spark.read.format("delta").load("/mnt/lhdw/silver/vendas/Ano=2018/Mes=1")
display(vendas_exibir)
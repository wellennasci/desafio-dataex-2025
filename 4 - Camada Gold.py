# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold - Carga de Dados e Processamento
# MAGIC O objetivo é transformar e armazenar os dados da camada Silver para a camada Gold de maneira otimizada para análises e BI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importando as bibliotecas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Ambiente

# COMMAND ----------

# Iniciar a SparkSession com configurações otimizadas
spark = SparkSession.builder \
    .appName("Carga Gold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definindo caminhos de armazenamento

# COMMAND ----------

silver_path = "/mnt/lhdw/silver"
gold_path = "/mnt/lhdw/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carga de Dados da Camada Silver

# COMMAND ----------

# Carregando os dados da camada Silver
vendas_df = spark.read.format("delta").load(f"{silver_path}/vendas")
clientes_df = spark.read.format("delta").load(f"{silver_path}/clientes")
vendedores_df = spark.read.format("delta").load(f"{silver_path}/vendedores")
produtos_df = spark.read.format("delta").load(f"{silver_path}/produtos")
categorias_df = spark.read.format("delta").load(f"{silver_path}/categorias")
cidades_df = spark.read.format("delta").load(f"{silver_path}/cidades")
paises_df = spark.read.format("delta").load(f"{silver_path}/paises")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adição de Surrogate Keys (SK) nas Dimensões
# MAGIC Cada tabela de dimensão recebe uma chave substituta (SK) para melhor gerenciamento dos dados.
# MAGIC

# COMMAND ----------


# Adicionando a SK nas tabelas de dimensão
clientes_df = clientes_df.withColumn("SK_Cliente", monotonically_increasing_id() + 1)
vendedores_df = vendedores_df.withColumn("SK_Vendedor", monotonically_increasing_id() + 1)
produtos_df = produtos_df.withColumn("SK_Produto", monotonically_increasing_id() + 1)
categorias_df = categorias_df.withColumn("SK_Categoria", monotonically_increasing_id() + 1)
cidades_df = cidades_df.withColumn("SK_Cidade", monotonically_increasing_id() + 1)
paises_df = paises_df.withColumn("SK_Pais", monotonically_increasing_id() + 1)


# COMMAND ----------

#Evidencia da aplicação
display(paises_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementação do SCD Tipo 2
# MAGIC Para manter histórico de alterações nas dimensões, cada tabela recebe as colunas:
# MAGIC
# MAGIC - data_inicio: Data de início do registro.
# MAGIC - data_fim: Data final padrão "9999-12-31".
# MAGIC - registro_ativo: Indicador de registro ativo.

# COMMAND ----------

#Aplicando o SCD2 nas tabelas dimensões

clientes_df = clientes_df.withColumn("data_inicio", F.current_date()) \
                         .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                         .withColumn("registro_ativo", F.lit(1))

vendedores_df = vendedores_df.withColumn("data_inicio", F.current_date()) \
                             .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                             .withColumn("registro_ativo", F.lit(1))

produtos_df = produtos_df.withColumn("data_inicio", F.current_date()) \
                         .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                         .withColumn("registro_ativo", F.lit(1))

categorias_df = categorias_df.withColumn("data_inicio", F.current_date()) \
                             .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                             .withColumn("registro_ativo", F.lit(1))

cidades_df = cidades_df.withColumn("data_inicio", F.current_date()) \
                       .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                       .withColumn("registro_ativo", F.lit(1))

paises_df = paises_df.withColumn("data_inicio", F.current_date()) \
                     .withColumn("data_fim", F.lit("9999-12-31").cast("date")) \
                     .withColumn("registro_ativo", F.lit(1))



# COMMAND ----------

#Evidencia da aplicação
display(paises_df)

# COMMAND ----------

#Evidencia da aplicação
display(clientes_df)

# COMMAND ----------

#Evidencia da aplicação
display(cidades_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Adição de Surrogate Keys (SK) na Fato**
# MAGIC A tabela fato vendas_df é enriquecida com as surrogate keys das dimensões, substituindo as pk´s:
# MAGIC
# MAGIC - ClienteID -> SK_Cliente
# MAGIC - VendedorID -> SK_Vendedor
# MAGIC - ProdutoID -> SK_Produto
# MAGIC
# MAGIC Após a substituição, as pk´s são removidos para otimização.

# COMMAND ----------

# Realizando o JOIN da tabela fato Vendas com as dimensões para obter as SKs
vendas_df = vendas_df \
    .join(clientes_df.select("ClienteID", "SK_Cliente"), "ClienteID", "left") \
    .join(vendedores_df.select("VendedorID", "SK_Vendedor"), "VendedorID", "left") \
    .join(produtos_df.select("ProdutoID", "SK_Produto"), "ProdutoID", "left") \
    

# Removendo as PKs e manter apenas as SKs
vendas_df = vendas_df.drop("ClienteID", "VendedorID", "ProdutoID", )

vendas_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando os Dados na Camada Gold
# MAGIC Os DataFrames transformados são salvos no formato Delta.

# COMMAND ----------

clientes_df.write.format("delta").mode("overwrite").save(f"{gold_path}/clientes")
vendedores_df.write.format("delta").mode("overwrite").save(f"{gold_path}/vendedores")
produtos_df.write.format("delta").mode("overwrite").save(f"{gold_path}/produtos")
categorias_df.write.format("delta").mode("overwrite").save(f"{gold_path}/categorias")
cidades_df.write.format("delta").mode("overwrite").save(f"{gold_path}/cidades")
paises_df.write.format("delta").mode("overwrite").save(f"{gold_path}/paises")

# Salvando a tabela de vendas na camada Gold com particionamento por ano e mês
vendas_df.write.format("delta") \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .save(f"{gold_path}/vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liberação de Memória
# MAGIC
# MAGIC Após o processamento, os DataFrames são liberados com .unpersist() para otimização do uso de memória.

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
# MAGIC ## Evidências da Carga

# COMMAND ----------

# Evidenciando os arquivos salvos na camada gold
display(dbutils.fs.ls("/mnt/lhdw/gold/"))

# COMMAND ----------

# Evidenciando os arquivos particionados
display(dbutils.fs.ls("/mnt/lhdw/gold/vendas/Ano=2018"))


# COMMAND ----------

#evidenciando a tabela fato
vendas_exibir = spark.read.format("delta").load("/mnt/lhdw/gold/vendas/Ano=2018/Mes=2")
display(vendas_exibir)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/lhdw/gold/produtos"))

# COMMAND ----------

#evidenciando a tabela produtos
prod_exibir = spark.read.format("delta").load("/mnt/lhdw/gold/produtos")
display(prod_exibir)
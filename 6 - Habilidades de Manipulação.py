# Databricks notebook source
# MAGIC %md
# MAGIC # Habilidades de Manipulação

# COMMAND ----------

# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, year, month, sum
from pyspark.sql.functions import col, month, year, round
from pyspark.sql.functions import format_number

# COMMAND ----------

# Iniciando a SparkSession
spark = SparkSession.builder \
    .appName("Habilidades") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

#Definindo caminho da pasta
gold_path = "/mnt/lhdw/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 1**
# MAGIC Qual o número total de linhas carregado na tabela fato?
# MAGIC

# COMMAND ----------

# Carregando dados da camada Gold
vendas_df = spark.read.format("delta").load(f"{gold_path}/vendas")

# Contando o número total de linhas na tabela fato
total_linhas_fato = vendas_df.count()

print(f"O número total de linhas carregadas na tabela fato é: {total_linhas_fato}")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC **EXERCÍCIO 2**
# MAGIC Qual o total de Vendas em Fev/2018 para cada Categoria?
# MAGIC

# COMMAND ----------

# Carregando a tabela de produto e categoria 
produtos_df = spark.read.format("delta").load("/mnt/lhdw/gold/produtos")
categorias_df = spark.read.format("delta").load("/mnt/lhdw/gold/categorias")

#Juntando a tabela de produtos com a de categorias para obter SK_Categoria
produtos_com_categoria_df = produtos_df.join(categorias_df, "CategoriaID", "inner")

# Join da tabela de vendas com a tabela produtos_com_categoria_df 
vendas_com_categoria_df = vendas_df.join(produtos_com_categoria_df, "SK_Produto", "inner")

# Filtrando os dados para fevereiro de 2018
vendas_fev_2018_df = vendas_com_categoria_df.filter(
    (year(col("DataVenda")) == 2018) & (month(col("DataVenda")) == 2)
)

# Agrupando por categoria e somando o total de vendas
total_vendas_fev_2018_por_categoria = vendas_fev_2018_df.groupBy("NomeCategoria").agg(
    round(sum("PrecoTotal"), 2).alias("TotalVendas")
).withColumn("TotalVendas", format_number("TotalVendas", 2))


display(total_vendas_fev_2018_por_categoria)


# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 3**
# MAGIC Qual o total de vendas em quantidade em Mar/2018 por Vendedor?
# MAGIC

# COMMAND ----------

# Carregando a tabela vendedores
vendedores_df = spark.read.format("delta").load("/mnt/lhdw/gold/vendedores")

# Filtrando as vendas realizadas em março de 2018
vendas_mar_2018 = vendas_df.filter(
    (month(col("DataVenda")) == 3) & (year(col("DataVenda")) == 2018)
)

# Join entre a tabela de vendas e a tabela de vendedores
vendas_com_nomes = vendas_mar_2018.join(
    vendedores_df, vendas_mar_2018.SK_Vendedor == vendedores_df.SK_Vendedor, "inner"
)

# Agrupando por nome do vendedor e somar a quantidade de vendas
vendas_por_vendedor = vendas_com_nomes.groupBy("Nome").sum("Quantidade")

display(vendas_por_vendedor)


# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 4**
# MAGIC Qual o total de vendas em (R$) em Fev/2018 por país?

# COMMAND ----------

# Carregando as tabelas pais, cidades e clientes
paises_df = spark.read.format("delta").load("/mnt/lhdw/gold/paises")
cidades_df = spark.read.format("delta").load("/mnt/lhdw/gold/cidades")
clientes_df = spark.read.format("delta").load("/mnt/lhdw/gold/clientes")

# Filtrando as vendas realizadas em fev de 2018
vendas_fev_2018 = vendas_df.filter(
    (month(col("DataVenda")) == 2) & (year(col("DataVenda")) == 2018)
)

# Join entre a tabela de vendas e a tabela de clientes
vendas_com_clientes = vendas_fev_2018.join(
    clientes_df, vendas_fev_2018.SK_Cliente == clientes_df.SK_Cliente, "inner"
)

# Join entre a tabela de clientes e a tabela de cidades (para acessar PaisID)
vendas_com_cidades = vendas_com_clientes.join(
    cidades_df, vendas_com_clientes.CidadeID == cidades_df.CidadeID, "inner"
)

# Join entre a tabela de cidades e a tabela de países (para acessar PaisNome)
vendas_com_pais = vendas_com_cidades.join(
    paises_df, vendas_com_cidades.PaisID == paises_df.PaisID, "inner"
)

# Agrupando por nome do país e somar o total de vendas (PrecoTotal)
vendas_por_pais = vendas_com_pais.groupBy("PaisNome").sum("PrecoTotal") \
    .withColumnRenamed("sum(PrecoTotal)", "TotalVendas") \
    .withColumn("TotalVendas", format_number("TotalVendas", 2)) 

display(vendas_por_pais)

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 5** Qual o Total de Vendas(R$) mês a mês em 2018?

# COMMAND ----------

# Filtrando as vendas realizadas em 2018
vendas_2018 = vendas_df.filter(year(col("DataVenda")) == 2018)

# Agrupando por mês e somar o total de vendas (PrecoTotal)
vendas_mes_a_mes = vendas_2018.groupBy(
    month(col("DataVenda")).alias("Mes"), 
    year(col("DataVenda")).alias("Ano")
).agg(
    round(sum("PrecoTotal"), 2).alias("TotalVendas") 
).orderBy("Ano", "Mes") \
.withColumn("TotalVendas", format_number("TotalVendas", 2))

display(vendas_mes_a_mes)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **EXERCÍCIO 6**
# MAGIC Qual o total de desconto mês a mês em 2018?
# MAGIC

# COMMAND ----------


# Filtrando as vendas realizadas em 2018
vendas_2018 = vendas_df.filter(year(col("DataVenda")) == 2018)

# Calculando o total de desconto (monetário) por mês
desconto_mes_a_mes = vendas_2018.withColumn(
    "DescontoMonetario", round(col("PrecoTotal") * col("Desconto"), 2)
) \
.groupBy(month(col("DataVenda")).alias("Mes"), year(col("DataVenda")).alias("Ano")) \
.agg(round(sum("DescontoMonetario"), 2).alias("TotalDesconto")) \
.orderBy("Ano", "Mes") \
.withColumn("TotalDesconto", format_number("TotalDesconto", 2))

display(desconto_mes_a_mes)


# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 7** Qual a variação %(MoM) de Total de vendas (R$) de Mar/2018 para Fev/2018.

# COMMAND ----------

# Filtrando as vendas realizadas em 2018
vendas_2018 = vendas_df.filter(year(col("DataVenda")) == 2018)

# Filtrando as vendas de fevereiro e março de 2018
vendas_fev_mar_2018 = vendas_2018.filter((month(col("DataVenda")) == 2) | (month(col("DataVenda")) == 3))

# Agrupando por mês e somar o total de vendas (R$)
vendas_por_mes = vendas_fev_mar_2018.groupBy(month(col("DataVenda")).alias("Mes"), year(col("DataVenda")).alias("Ano")) \
    .agg(sum("PrecoTotal").alias("TotalVendas"))

# Obtendo o total de vendas de Fev/2018 e Mar/2018
vendas_fev_2018 = vendas_por_mes.filter((col("Mes") == 2) & (col("Ano") == 2018)).collect()[0]["TotalVendas"]
vendas_mar_2018 = vendas_por_mes.filter((col("Mes") == 3) & (col("Ano") == 2018)).collect()[0]["TotalVendas"]

# Calculando a variação percentual (MoM)
variacao_mom = ((vendas_mar_2018 - vendas_fev_2018) / vendas_fev_2018) * 100

print(f"A variação percentual MoM de Total de vendas (R$) de Mar/2018 para Fev/2018 é: {variacao_mom:.2f}%")


# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 8** Qual o Top 10 produtos com maior Valor de vendas (R$) em Fev/2018.

# COMMAND ----------

# Filtrando as vendas realizadas em fevereiro de 2018
vendas_fev_2018 = vendas_df.filter((month(col("DataVenda")) == 2) & (year(col("DataVenda")) == 2018))

# Agrupando por ProdutoID e somar o total de vendas (R$) para cada produto
vendas_por_produto = vendas_fev_2018.groupBy("SK_Produto") \
    .agg(round(sum("PrecoTotal"), 2).alias("TotalVendas"))

# Join com a tabela de produtos para obter o nome dos produtos
top_10_produtos_com_nome = vendas_por_produto.join(produtos_df, on="SK_Produto", how="inner") \
    .select("ProdutoNome", "TotalVendas") \
    .orderBy(col("TotalVendas").desc()) \
    .limit(10) \
    .withColumn("TotalVendas", format_number("TotalVendas", 2)) \

display(top_10_produtos_com_nome)


# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 9**
# MAGIC Retorne o % (Share*) de Total de vendas (R$), por Categoria em Fev/2018.
# MAGIC Percentual de participação de vendas da categoria com relação do total

# COMMAND ----------

# Filtrando as vendas realizadas em fevereiro de 2018
vendas_fev_2018 = vendas_df.filter((month(col("DataVenda")) == 2) & (year(col("DataVenda")) == 2018))

# Agrupando por CategoriaID e somar o total de vendas (R$) para cada categoria
vendas_por_categoria = vendas_fev_2018.join(produtos_df, on="SK_Produto", how="inner") \
    .groupBy("CategoriaID") \
    .agg(round(sum("PrecoTotal"), 2).alias("TotalVendasCategoria"))


# Calculando o total geral de vendas em fevereiro de 2018
total_vendas_fev_2018 = vendas_fev_2018.agg(round(sum("PrecoTotal"), 2).alias("TotalVendasFev2018")).collect()[0]["TotalVendasFev2018"]

# Calculando o percentual de participação por categoria
vendas_por_categoria_com_percentual = vendas_por_categoria.withColumn(
    "PercentualVendas(%)",
    round((col("TotalVendasCategoria") / total_vendas_fev_2018) * 100, 2)  
)

# Join com a tabela de categorias para obter o nome da categoria
vendas_por_categoria_com_nome = vendas_por_categoria_com_percentual.join(categorias_df, on="CategoriaID", how="inner") \
    .select("NomeCategoria", "TotalVendasCategoria", "PercentualVendas(%)") \
    .orderBy(col("PercentualVendas(%)").desc()) \
    .withColumn("TotalVendasCategoria", format_number("TotalVendasCategoria", 2)) \

display(vendas_por_categoria_com_nome)

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCÍCIO 10**
# MAGIC Retorne o Ticket médio de Total de por categoria no ano de Jan/2018.
# MAGIC

# COMMAND ----------

# Filtrando as vendas realizadas em janeiro de 2018
vendas_jan_2018 = vendas_df.filter((month(col("DataVenda")) == 1) & (year(col("DataVenda")) == 2018))

# Join com a tabela de produtos para obter a categoria do produto
vendas_com_categoria = vendas_jan_2018.join(produtos_df, on="SK_Produto", how="inner")

# Calculando o total de vendas e o número de vendas por categoria
ticket_medio_categoria = vendas_com_categoria.groupBy("CategoriaID") \
    .agg(
        sum("PrecoTotal").alias("TotalVendas"),
        count("VendasID").alias("NumeroVendas")
    ) \
    .withColumn("TicketMedio", round(col("TotalVendas") / col("NumeroVendas"), 2))

# Join com a tabela de categorias para obter o nome da categoria
ticket_medio_com_nome = ticket_medio_categoria.join(categorias_df, on="CategoriaID", how="inner") \
    .select("NomeCategoria", "TicketMedio") \
    .orderBy("TicketMedio", ascending=False)

display(ticket_medio_com_nome)

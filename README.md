#🚀 Desafio DataEx - Databricks

Este repositório contém a solução do desafio DataEx realizado no Databricks, onde foram implementadas as principais práticas de modelagem dimensional, arquitetura Medallion e processamento de dados com PySpark e Delta Lake.

📂 Estrutura do Repositório
O desafio foi estruturado em seis notebooks, seguindo a metodologia proposta:

1️⃣ Configuração do ambiente DBFS e importação dos arquivos
2️⃣ Camada Bronze → Ingestão dos dados no formato Delta
3️⃣ Camada Silver → Transformações e limpeza dos dados
4️⃣ Camada Gold → Estruturação final dos dados para consumo
5️⃣ Carga Incremental → Implementação de processamento incremental
6️⃣ Habilidades de Manipulação → Testes e manipulações avançadas


🛠️ Tecnologias Utilizadas
Databricks Community Edition
PySpark
Delta Lake
Arquitetura Medallion (Bronze, Silver e Gold)
Armazenamento DBFS (Databricks File System)
Processamento incremental com SCD2

📌 Descrição do Desafio
O objetivo deste desafio foi desenvolver um pipeline de dados estruturado em Databricks, aplicando conceitos de arquitetura Medallion e modelagem dimensional. As principais atividades incluíram:

✔️ Ingestão de dados brutos (Landing Zone → Bronze)
✔️ Limpeza e padronização dos dados (Silver)
✔️ Criação de uma camada otimizada para consumo analítico (Gold)
✔️ Implementação de Slowly Changing Dimension Type 2 (SCD2)
✔️ Criação de um fluxo de carga incremental para a tabela fato


Este projeto foi desenvolvido como parte de um desafio educacional. Sinta-se à vontade para explorar e contribuir!

# 🎬 Sistema de Recomendação MovieLens (Data Lakehouse)

Um sistema de recomendação end-to-end construído sobre uma arquitetura moderna de Data Lakehouse. O projeto ingere dados brutos do MovieLens, processa-os utilizando Apache Spark, armazena-os no formato Apache Iceberg (gerido pelo MinIO) e orquestra todo o pipeline de Machine Learning com o Apache Airflow. A interface final é servida através de uma aplicação web interativa em Streamlit.

## 🏗️ Arquitetura do Projeto

- O pipeline de dados segue a medalhística clássica do Data Lakehouse (Silver & Gold), totalmente orquestrada através de DAGs desacopladas no Airflow:

### Camada Silver (ETL & Limpeza):

- Scripts PySpark processam os dados brutos de filmes, avaliações, tags e links.
- Os dados são limpos, tipados e guardados em tabelas Apache Iceberg armazenadas num bucket S3 local (MinIO).
- Orquestração: As tarefas rodam num Pool dedicado do Airflow (spark_pool) para otimização de recursos da máquina.
- Camada Gold (Machine Learning - ALS Avançado):
- Treinamento do algoritmo de Recomendação Colaborativa (Alternating Least Squares - ALS).
- Freshness (Decaimento Temporal): Avaliações mais antigas sofrem um decaimento exponencial, forçando o modelo a priorizar os gostos atuais do utilizador.
- Fairness (Diversidade): Aplicação de uma penalização logarítmica em filmes "Blockbusters", permitindo que filmes do long-tail (menos conhecidos, mas altamente avaliados) apareçam nas recomendações.
- O resultado final é guardado no Iceberg num formato plano (flat) otimizado para leitura.

### Frontend (Streamlit):

- Uma interface leve que consome os dados da camada Gold diretamente do Iceberg utilizando PySpark em cache, garantindo alta performance de leitura.

  🛠️ Tecnologias Utilizadas

- Processamento: Apache Spark (PySpark)
- Armazenamento: Apache Iceberg & MinIO (S3)
- Orquestração: Apache Airflow (Docker / SparkSubmitOperator)
- Machine Learning: Spark ML (ALS)
- Frontend: Streamlit
- Gestor de Pacotes: uv

## 🚀 Como Executar o Projeto

### Pré-requisitos:

#### Certifique-se de que tem instalado na sua máquina:

- Docker & Docker Compose
- Python 3.10+
- uv (Gestor de pacotes e dependências Python)

#### Passo 0: Baixar os dados

- Execute o script _ingest_movielens_raw.py_ que representa a camada Bronze, a ingestão de dados cru no MinIO.
  - Ou, pode executar a DAG _movielens_bronze_ingestion_ (substituindo a DAG do Passo 2)

#### Passo 1: Subir a Infraestrutura

- Inicie os contentores do Airflow, Spark Master, MinIO e Postgres rodando o comando na raiz do projeto:
- docker compose up -d
- Aguarde alguns minutos até que o Airflow Webserver e o Scheduler estejam saudáveis.

#### Passo 2: Executar o Pipeline de Dados (Airflow)

- Todo o processamento deve ser feito antes de abrir o frontend.
- Aceda à interface do Airflow em http://localhost:8080 (credenciais padrão: airflow / airflow).
- Ative a DAG _movielens_silver_pipeline_ que automaticamente já dá trigger na DAG _movielens_gold_pipeline_, que processam as camadas silver e gold, respectivamente.
- Rode: docker compose run --rm airflow-cli dags trigger movielens_silver_pipeline # A DAG da camada silver já aciona automaticamente a da camada gold.
  - Ou execute pela interface.

#### Passo 3: Rodar a Interface Streamlit

- Com os dados perfeitamente processados no Lakehouse, inicie o frontend.
- Para evitar problemas de importação (PYTHONPATH) e garantir que o Streamlit enxerga as configurações do Spark na raiz do projeto, utilize o uv executando o Streamlit como um módulo:
  - uv run python -m streamlit run frontend/app.py

- Acesse no seu navegador: http://localhost:8501.
- Introduza o ID de um utilizador na barra lateral e veja as recomendações geradas pelo modelo e o histórico do Lakehouse!

### 📂 Estrutura de Diretórios (Resumo)

```text
movielens_project/
├── dags/                       # Orquestração do Airflow
│   └── movielens_pipeline.py
├── docker/                     # Imagens docker customizadas
│   ├── airflow/Dockerfile
│   └── spark/Dockerfile
├── frontend/                   # Interface de Utilizador
│   └── app.py
├── transformations/            # Scripts PySpark (ETL e ML)
│   ├── movies.py
│   ├── tags.py
│   ├── ratings.py
│   ├── links.py
│   └── als_recommendation.py
├── spark_config.py             # Configuração global da Spark Session
├── ingest_movielens_raw.py     # Script de Download do banco de dados
├── docker-compose.yaml         # Infraestrutura
└── README.md                   # Este ficheiro
```

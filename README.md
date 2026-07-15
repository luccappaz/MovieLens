# 🎬 Sistema de Recomendação MovieLens (Data Lakehouse & RAG)

Um sistema de recomendação end-to-end construído sobre uma arquitetura moderna de Data Lakehouse e IA Generativa. O projeto ingere dados brutos do MovieLens, processa-os utilizando Apache Spark, armazena-os no formato Apache Iceberg (gerido pelo MinIO) e orquestra todo o pipeline com o Apache Airflow. Além do modelo de Machine Learning clássico, o sistema conta agora com um agente RAG (Retrieval-Augmented Generation) para buscas semânticas em linguagem natural, servido numa aplicação web interativa em Streamlit.

## 🏗️ Arquitetura do Projeto

O pipeline de dados é dividido em duas frentes principais: o Pipeline de Lakehouse (Silver & Gold) e o Pipeline de IA (RAG & Banco Vetorial).

### 1. Pipeline Analítico (Lakehouse - ALS)

Totalmente orquestrado através de DAGs desacopladas no Airflow:

**Camada Silver (ETL & Limpeza):**

- Scripts PySpark processam os dados brutos de filmes, avaliações, tags e links.
- Os dados são limpos, tipados e guardados em tabelas Apache Iceberg armazenadas num bucket S3 local (MinIO).
- Orquestração: as tarefas rodam num Pool dedicado do Airflow (`spark_pool`) para otimização de recursos da máquina.

**Camada Gold (Machine Learning - ALS Avançado):**

- Treinamento do algoritmo de Recomendação Colaborativa (Alternating Least Squares - ALS).
- **Freshness (Decaimento Temporal):** avaliações mais antigas sofrem um decaimento exponencial, forçando o modelo a priorizar os gostos atuais do utilizador.
- **Fairness (Diversidade):** aplicação de uma penalização logarítmica em filmes "Blockbusters", permitindo que filmes do long-tail (menos conhecidos, mas altamente avaliados) apareçam nas recomendações.
- O resultado final é guardado no Iceberg num formato plano (flat) otimizado para leitura de baixa latência.

### 2. Pipeline de Busca Semântica (IA / RAG)

Para permitir que o utilizador "converse" com o catálogo, adicionámos processamento de Linguagem Natural:

- **Enriquecimento (TMDb API):** um job PySpark consome a tabela de links para extrair sinopses, pósteres e orçamentos do The Movie Database, atualizando o Iceberg de forma distribuída.
- **Vetorização (Ollama):** as sinopses, títulos e géneros são combinados e processados pelo modelo local `nomic-embed-text` rodando no Ollama, gerando embeddings (vetores de 768 dimensões).
- **Vector DB (PostgreSQL + pgvector):** os vetores são armazenados diretamente no PostgreSQL (já existente para o Airflow) utilizando a extensão pgvector, que calcula a Similaridade de Cosseno em milissegundos para responder ao Streamlit.
- **Frontend (Streamlit):** uma interface leve dividida em duas abas que consome:
  - **Aba de Recomendações:** dados da camada Gold e Silver do Iceberg utilizando PySpark em cache.
  - **Aba de Chatbot (Busca IA):** faz a interface direta com o Ollama para vetorizar a pergunta do utilizador e consulta o banco PostgreSQL para devolver resultados baseados no significado do texto.

## 🛠️ Tecnologias Utilizadas

- **Processamento:** Apache Spark (PySpark)
- **Armazenamento:** Apache Iceberg & MinIO (S3)
- **Orquestração:** Apache Airflow (Docker / SparkSubmitOperator / DockerOperator)
- **Machine Learning (ALS):** Spark MLlib
- **Inteligência Artificial (RAG):** Ollama (LLM Local) e PostgreSQL (pgvector)
- **Frontend:** Streamlit
- **Gestor de Pacotes:** uv

## 🚀 Como Executar o Projeto

### Pré-requisitos

Certifique-se de que tem instalado na sua máquina:

- Docker & Docker Compose
- Python 3.10+
- uv (Gestor de pacotes e dependências Python)
- Ollama (instalado e rodando o modelo `nomic-embed-text`)

### Passo 0: Baixar os dados

Execute o script `ingestion/movielens_raw.py` que representa a camada Bronze, fazendo o download do ZIP e a ingestão de dados crus no MinIO.

Ou, pode executar a DAG `movielens_bronze_ingestion` (substituindo a DAG do Passo 2).

### Passo 1: Subir a Infraestrutura

Inicie os contentores do Airflow, Spark Master, MinIO e Postgres rodando o comando na raiz do projeto:

```bash
docker compose up -d
```

Aguarde alguns minutos até que o Airflow Webserver e o Scheduler estejam saudáveis.

### Passo 2: Executar o Pipeline de Dados (Airflow)

Todo o processamento deve ser feito antes de abrir o frontend.

Aceda à interface do Airflow em [http://localhost:8080](http://localhost:8080) (credenciais padrão: `airflow` / `airflow`).

Ative a DAG `movielens_silver_pipeline` que automaticamente já dá trigger na DAG `movielens_gold_pipeline`, que processam as camadas Silver e Gold, respetivamente.

Pelo terminal, pode rodar:

```bash
docker compose run --rm airflow-cli dags trigger movielens_silver_pipeline
```

### Passo 3: Enriquecimento e Vetorização (RAG)

Com a camada Silver pronta, prepare os dados para a Busca Semântica:

```bash
# 1. Enriquecer os filmes com as sinopses do TMDb
uv run python -m ingestion.tmdb_enrichment

# 2. Gerar os Embeddings no Ollama e guardar no Postgres
uv run python -m transformations.create_embeddings
```

### Passo 4: Rodar a Interface Streamlit

Com os dados perfeitamente processados no Lakehouse e no Vector DB, inicie o frontend. Para evitar problemas de importação (PYTHONPATH) e garantir que o Streamlit enxerga as configurações do Spark na raiz do projeto, utilize o uv executando o Streamlit como um módulo:

```bash
uv run python -m streamlit run frontend/app.py
```

Aceda no seu navegador: [http://localhost:8501](http://localhost:8501).

Introduza o ID de um utilizador para ver as recomendações do ALS ou navegue para a aba do Chatbot para descrever o filme que lhe apetece assistir!

## 📂 Estrutura de Diretórios (Resumo)

```
movielens_project/
├── dags/                       # Orquestração do Airflow
│   └── movielens_pipeline.py
├── docker/                     # Imagens docker customizadas
│   ├── airflow/Dockerfile
│   └── postgres/initdb/init-multiple-dbs.sh # Criando db para o Iceberg e o Airflow
│   └── spark/Dockerfile
├── frontend/                   # Interface de Utilizador (Streamlit)
│   └── app.py
├── ingestions/                  # Scripts de Extração e Enriquecimento
│   ├── movielens_raw.py
│   └── tmdb_enrichment.py
├── transformations/            # Scripts PySpark (ETL, ML e Vetorização)
│   ├── movies.py
│   ├── tags.py
│   ├── ratings.py
│   ├── links.py
│   ├── als_recommendation.py
│   └── movie_embeddings.py
├── spark_config.py             # Configuração global da Spark Session
├── docker-compose.yaml          # Infraestrutura (MinIO, Postgres, Airflow)
└── README.md                   # Este ficheiro
```

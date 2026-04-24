# 🚀 Data Pipeline E-commerce | Python + Airflow + PostgreSQL

## Descrição
Projeto de Engenharia de Dados que simula um cenário real de e-commerce, com construção de um pipeline ETL completo para ingestão, transformação, análise e carga de dados.

O pipeline foi desenvolvido com foco em boas práticas, organização em camadas e geração de métricas de negócio relevantes.

## Contexto
Uma empresa de e-commerce em crescimento enfrentava dificuldades na análise de dados devido à falta de padronização, integração e automação.

Os dados estavam distribuídos em APIs e arquivos JSON, sem estrutura confiável para análises.

## PROBLEMA
- Dados inconsistentes e duplicados
- Processo manual e não automatizado
- Falta de integração entre fontes
- Dificuldade para gerar insights estratégicos

A empresa não conseguia responder perguntas como:
- Qual o preço médio por categoria?
- Qual a variação de preço (mín/máx)?
- Qual o impacto dos descontos nos produtos?

## 📚 Documentação

Para mais detalhes sobre o projeto:

- 🏗️ Arquitetura: docs/architecture.md
- 👨🏽‍⚖️ Decisões técnicas: docs/decision.md
- 🚨 Problema de negócio: docs/problem.md

## 🗂️ Estrutura do Projeto
```
Data_Pipeline_Ecommerce/
│
├── config/                     # Configurações do pipeline
│
├── dags/                      # DAGs do Apache Airflow
│   └── etl_analytics.py
│
├── data/
│   ├── raw/                   # Dados brutos (ingestão)
│   ├── trusted/               # Dados tratados/validados
│   └── analytics/            # Dados prontos para análise
│
├── docs/                      # Documentação do projeto
│   ├── architecture.md        # Arquitetura do pipeline
│   ├── decision.md            # Decisões técnicas
│   ├── problem.md             # Problema de negócio
│   └── images/                # Diagramas e imagens
│
├── logs/                      # Logs do Airflow e pipeline
│   ├── scheduler/
│   ├── dag_processor_manager/
│   ├── dag_id=etl_analytics/
│   └── pipeline.log
│
├── src/                       # Código principal do projeto
│   ├── analytics/             # Métricas e cálculos de negócio
│   │   └── price_metrics.py
│   │
│   ├── connection/            # Conexão com banco de dados
│   │   └── postgres.py
│   │
│   ├── ingestion/             # Camada de ingestão de dados
│   │   └── ingestion.py
│   │
│   ├── processing/            # Transformações (ETL)
│   │   └── transform.py
│   │
│   ├── storage/               # Carga de dados (load)
│   │   └── load.py
│   │
│   └── utils/                 # Utilitários gerais
│       ├── logger.py
│       ├── helpers.py
│       └── get_latest_file.py
│
├── .env                       # Variáveis de ambiente
├── .env.example
├── .gitignore
│
├── airflow.cfg               # Configuração do Airflow
├── docker-compose.yml        # Orquestração do ambiente
├── webserver_config.py       # Configuração web Airflow
│
├── requirements.txt
└── README.md
```
# 🛒 Medallion Pipeline - E-commerce

Pipeline de Engenharia de Dados end-to-end baseado em arquitetura Medallion, utilizando Apache Spark, Delta Lake, Apache Airflow e HDFS para processamento incremental distribuído e construção de um modelo dimensional analítico.

---

# 📌 Visão Geral

Este projeto simula uma plataforma moderna de dados para e-commerce, implementando um pipeline incremental distribuído com separação em camadas:

```text
Landing → Raw → Trusted → Refined
```

O pipeline realiza:

- Ingestão incremental via watermark
- Processamento distribuído com Spark Cluster
- Merge incremental com Delta Lake
- Estratégia híbrida Unprocessed + Lookback
- Data Quality explícita
- Modelagem dimensional (Star Schema)
- SCD Tipo 2 em dimensões analíticas

---

# 🧩 Arquitetura

![Arquitetura Medallion](docs/diagrams/arquitetura_medallion_pipeline.png)

---

# 🧱 Stack Tecnológica

| Tecnologia | Finalidade |
|---|---|
| Apache Airflow 2.10.5 | Orquestração |
| Apache Spark 3.5.8 (Standalone Cluster) | Processamento distribuído |
| Delta Lake 3.2.0 | ACID + Merge incremental |
| Hadoop HDFS 3.2.1 | Data Lake distribuído |
| PostgreSQL 15 | Metadata do Airflow |
| Docker Compose | Infraestrutura local |
| Python 3.10.12 | Desenvolvimento |

---

# 🔄 Fluxo do Pipeline

```text
MySQL / CSV
      ↓
Landing
(Ingestão incremental)
      ↓
Raw
(Batch incremental + Delta Merge)
      ↓
Trusted
(Data Quality + Regras de Negócio)
      ↓
Refined
(Modelo Dimensional)
```

---

# 🏛️ Arquitetura em Camadas

## 🟡 Landing

Camada responsável pela ingestão incremental dos dados.

### Características

- Leitura JDBC (MySQL)
- Ingestão incremental via watermark
- Persistência em Parquet
- Controle incremental via metadata em HDFS

---

## 🔵 Raw

Camada responsável pela padronização e consistência incremental.

### Características

- Batch incremental
- Schema enforcement
- Deduplicação
- Merge incremental com Delta Lake
- Colunas técnicas
- Estratégia Unprocessed + Lookback

---

## 🟢 Trusted

Camada responsável pela qualidade e confiabilidade dos dados.

### Validações

- CPF
- Email
- Telefone
- Regras de negócio
- Flags de qualidade

---

## 🟣 Refined

Camada analítica baseada em modelo dimensional.

### Dimensões

- dim_cliente (SCD Tipo 2)
- dim_produto (SCD Tipo 2)
- dim_pagamento
- dim_data

### Fato

- fato_vendas

### Granularidade

```text
1 linha = 1 item de pedido
```

---

# 🧠 Estratégia Incremental

O pipeline implementa uma estratégia incremental híbrida para garantir:

- Reprocessamento controlado
- Idempotência
- Tratamento de late arriving data
- Eficiência operacional

## Estratégias utilizadas

### Watermark

Utilizado na Landing para captura incremental:

```sql
WHERE data_transacao > watermark
```

### Unprocessed

Identificação de registros ainda não processados via comparação incremental.

### Lookback

Reprocessamento determinístico das últimas partições para garantir consistência.

### Delta Merge

Operações ACID com merge incremental utilizando Delta Lake.

---

# 📊 Modelo Dimensional

![Modelo Dimensional](docs/diagrams/modelo_dimensional.png)

---

# ⚙️ Orquestração

O pipeline é orquestrado via Apache Airflow utilizando DAGs desacopladas por camada.

## Responsabilidades

- Agendamento
- Dependências
- Observabilidade
- Retries
- Logs centralizados

---

# 🗂️ Estrutura do Projeto

```text
medallion-pipeline-ecommerce/
├── airflow/
├── spark/
├── infra/
├── docs/
└── README.md
```

📄 Estrutura completa disponível em:

```text
docs/project_structure.md
```

---

# 🚀 Como Executar

## Subir infraestrutura

```bash
docker compose -f infra/docker/docker-compose.yml up -d
```

---

## Acessar interfaces

| Serviço | URL |
|---|---|
| Airflow | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| HDFS Namenode | http://localhost:9870 |

---

## Executar pipeline

As DAGs podem ser executadas diretamente via interface do Airflow.

Fluxo recomendado:

```text
01_landing
   ↓
02_raw
   ↓
03_trusted
   ↓
04_refined
```

---

# 📈 Características do Projeto

- Pipeline incremental real
- Processamento distribuído
- Delta Lake
- Merge incremental
- SCD Tipo 2
- Star Schema
- Data Quality
- Arquitetura Medallion
- Spark Standalone Cluster
- Separação clara por camadas
- Reprocessamento controlado
- Governança de dados

---

# 📌 Decisões Arquiteturais

| Decisão | Motivo |
|---|---|
| Delta Lake | Merge incremental + ACID |
| Watermark | Ingestão incremental eficiente |
| Spark Cluster | Processamento distribuído |
| SCD Tipo 2 | Histórico de entidades |
| Star Schema | Performance analítica |
| Docker Compose | Reprodutibilidade |

---

# 🔍 Observabilidade

- Logs centralizados via Airflow
- Monitoramento de DAGs
- Controle incremental por metadata
- Arquivos `_SUCCESS`
- Spark UI para análise de jobs

---

# 📚 Documentação Técnica

A documentação técnica do projeto foi separada por responsabilidade para facilitar navegação, manutenção e aprofundamento arquitetural.

| Documento | Descrição |
|---|---|
| `docs/architecture.md` | Documentação completa da arquitetura Medallion, estratégias incrementais, infraestrutura distribuída e modelagem dimensional |
| `docs/project_structure.md` | Organização estrutural do projeto, separação por camadas, DAGs, jobs Spark e componentes da infraestrutura |
| `docs/decisions.md` | Registro das principais decisões arquiteturais (ADRs), incluindo motivações, benefícios e tradeoffs técnicos |

---

# 🚀 Evoluções Futuras

- Great Expectations
- Metadata-driven pipelines
- Kubernetes Executor
- Camada de Serving
- Observabilidade avançada
- Testes automatizados
- CI/CD

---

# 🎯 Objetivo

Demonstrar a construção de uma arquitetura moderna de engenharia de dados próxima de cenários reais de produção, com foco em:

- Escalabilidade
- Governança
- Confiabilidade
- Processamento incremental
- Modelagem analítica
- Qualidade de dados
- Arquitetura distribuída

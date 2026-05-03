# 🏗️ Arquitetura - Medallion Pipeline E-commerce

## 📌 Visão Geral

Pipeline de dados distribuído baseado em arquitetura Medallion com modelagem dimensional na camada final.

---

## 🧱 Infraestrutura

Executado via Docker Compose:

* Airflow (Scheduler + Webserver)
* PostgreSQL (metadata)
* Spark Cluster (Master + Worker)
* HDFS (Namenode + Datanode)

---

## ⚙️ Execução

```text
Airflow DAG
   ↓
SparkSubmit
   ↓
Spark Cluster (Standalone)
   ↓
HDFS (Delta Lake)
```

---

## 🏛️ Arquitetura em Camadas

```text
Landing → Raw → Trusted → Refined
```

---

## 🟡 Landing (Ingestion Layer)

### Características:

* Ingestão incremental via watermark
* Leitura JDBC (MySQL)
* Persistência em Parquet particionado
* Metadata de controle em HDFS

### Estratégia:

```sql
WHERE data_transacao > watermark
```

---

## 🔵 Raw (CDC Layer)

### Funções:

* Padronização de schema
* Deduplicação
* CDC com Delta Lake

### Estratégias implementadas:

* Backlog processing
* Lookback window
* Merge incremental (upsert)

### Armazenamento:

* Delta Lake particionado

---

## 🟢 Trusted (Data Quality Layer)

### Funções:

* Limpeza de dados
* Validação de regras de negócio
* Padronização

### Validações:

* CPF (dígito verificador)
* Email (regex)
* Telefone

### Output:

* Dataset confiável para consumo analítico

---

## 🟣 Refined (Analytics Layer)

### Modelo: Star Schema

#### Dimensões:

* dim_cliente (SCD Tipo 2)
* dim_produto (SCD Tipo 2)
* dim_pagamento
* dim_data

#### Fato:

* fato_vendas

### Características:

* Surrogate keys
* Integridade referencial
* Métricas derivadas
* CDC aplicado também na camada analítica

---

## 🔁 Estratégia Incremental Global

Aplicada em todas as camadas:

* Identificação de partições novas
* Reprocessamento controlado (lookback)
* Merge incremental com Delta

---

## 📊 Data Quality

* Validação explícita em Trusted
* Integridade referencial na Refined
* Controle de duplicidade na fato
* Flags de qualidade de dados

---

## 📌 Decisões Arquiteturais

| Decisão     | Motivo                         |
| ----------- | ------------------------------ |
| Delta Lake  | suporte a merge e ACID         |
| Watermark   | ingestão incremental eficiente |
| SCD2        | rastreabilidade histórica      |
| Star Schema | performance analítica          |
| Docker      | reprodutibilidade              |

---

## 🚀 Evoluções Possíveis

* Data Quality framework (Great Expectations)
* Orquestração distribuída (Celery/Kubernetes)
* Camada de serving (API/BI)
* Observabilidade (metrics/logs)

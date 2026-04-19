# 📊 Data Engineering Pipeline (End-to-End)

## 🧠 Visão Geral

Este projeto implementa um pipeline de dados end-to-end simulando um ambiente real de engenharia de dados, integrando múltiplas fontes e aplicando transformações em arquitetura de camadas (Medallion Architecture).

As fontes incluem:

* Banco transacional (MySQL)
* Arquivos CSV para enriquecimento de dados

O ambiente é totalmente containerizado com Docker e composto por:

* Apache Airflow (orquestração)
* Apache Spark (processamento distribuído)
* HDFS (data lake)
* Superset (visualização)

---

## 📊 Arquitetura do Pipeline

---
```markdown
```mermaid
flowchart LR

    subgraph "Fontes de Dados"
        A["MySQL (Transacional)"]
        B["CSV (Fonte Externa)"]
    end

    subgraph "Landing (HDFS)"
        C["Dados Brutos"]
    end

    subgraph "Raw (HDFS)"
        D["Parquet + Metadata"]
    end

    subgraph "Trusted (HDFS)"
        E["Limpeza e Padronização"]
    end

    subgraph "Refined (HDFS)"
        F["Modelo Dimensional"]
    end

    subgraph "Processamento"
        G["Spark Jobs"]
    end

    subgraph "Orquestração"
        H["Airflow DAGs"]
    end

    subgraph "Consumo"
        I["Superset / DuckDB"]
    end

    A --> C
    B --> C

    C --> G --> D
    D --> G --> E
    E --> G --> F

    F --> I

    H --> G
````
---

## 🏗️ Arquitetura em Camadas

### 🟤 Landing (Bronze - Ingestão)
- Dados brutos conforme origem
- Armazenados em formato **Parquet**
### ⚪ Raw
- Conversão para **Delta Lake**
- Inclusão de metadados:
  - Data de ingestão
  - Origem
- Suporte a versionamento e operações ACID

### 🟡 Trusted
- Limpeza e padronização
- Tratamento de nulos
- Deduplicação
- Validação de qualidade

### 🟢 Refined
- Modelagem dimensional (Star Schema)
- Enriquecimento de dados

## 🧩 Modelo Dimensional
![Modelo Dimensional](docs/modelo_dimensional.png)

#### Tabelas:

**Fato**
- fato_vendas

**Dimensões**
- dim_cliente
- dim_produto
- dim_data
- dim_pagamento

---

## 🔄 Fluxo de Dados

1. Dados são ingeridos do MySQL e CSV
2. Armazenados na camada Landing
3. Processados para Raw (Parquet + metadata)
4. Tratados na Trusted (qualidade)
5. Modelados na Refined (fato + dimensões)
6. Consumidos via Superset

---

## ⚙️ Stack Tecnológica

- Apache Airflow
- Apache Spark (PySpark)
- HDFS
- Docker Compose
- MySQL
- Superset
- DuckDB

---

## 🔑 Decisões Técnicas

### Surrogate Keys com Hash

```python
F.abs(F.hash("id_pedido", "id_item_pedido", "id_produto")).cast("bigint")
```

**Motivação:**
- Determinístico
- Escalável
- Compatível com processamento distribuído

---

### Uso de Parquet

- Formato colunar
- Melhor compressão
- Alta performance no Spark

---

### Arquitetura em Camadas

- Reprocessamento isolado
- Melhor governança
- Separação de responsabilidades

---

## 🚀 Roadmap (Evoluções Futuras)

- Carga incremental (partition por data)
- Data Quality automatizada
- Integração direta com engine analítica
- Monitoramento e alertas no Airflow

---

## ▶️ Como Executar

```bash
# Subir ambiente
Docker Compose up -d --build

# Acessar Airflow
http://localhost:8080

# Acessar Superset
http://localhost:8088
```

---

## 📌 Objetivo

Demonstrar na prática a construção de um pipeline moderno de engenharia de dados com boas práticas de mercado, incluindo:

- Arquitetura em camadas
- Processamento distribuído
- Orquestração de workflows
- Modelagem dimensional
- Visualização de dados

````

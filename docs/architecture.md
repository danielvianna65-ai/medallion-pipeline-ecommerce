# 🏗️ Arquitetura - Medallion Pipeline E-commerce

Documentação técnica da arquitetura de dados utilizada no projeto **Medallion Pipeline - E-commerce**, implementado com Apache Spark, Delta Lake, Apache Airflow e HDFS.

---

# 📌 Visão Geral

O projeto implementa uma arquitetura moderna de engenharia de dados baseada no padrão **Medallion Architecture**, separando o pipeline em múltiplas camadas com responsabilidades bem definidas.

O objetivo da arquitetura é garantir:

- Escalabilidade
- Processamento incremental eficiente
- Governança de dados
- Qualidade de dados
- Reprocessamento controlado
- Consistência analítica

---

# 🧩 Arquitetura Geral

![Arquitetura Medallion](diagrams/arquitetura_medallion_pipeline.png)

---

# 🏛️ Arquitetura em Camadas

```text
Landing → Raw → Trusted → Refined
```

Cada camada possui responsabilidades específicas dentro do pipeline.

---

# 🟡 Landing Layer

Camada responsável pela ingestão incremental dos dados brutos.

## Objetivos

- Capturar dados de origem
- Persistir dados brutos
- Controlar incrementalidade
- Minimizar reprocessamentos

---

## Fontes de Dados

### MySQL

- Base transacional de e-commerce
- Leitura via JDBC

### CSV

- Dados auxiliares de enriquecimento
- Ingestão batch

---

## Estratégia Incremental

A ingestão incremental é baseada em watermark:

```sql
WHERE data_transacao > watermark
```

O watermark é persistido em metadata no HDFS, permitindo continuidade entre execuções.

---

## Armazenamento

### Formato

- Parquet

### Particionamento

```text
dt=YYYY-MM-DD
```

### Objetivos do particionamento

- Partition pruning
- Redução de I/O
- Eficiência incremental
- Melhor desempenho de leitura

---

# 🔵 Raw Layer

Camada responsável pela padronização e consistência incremental dos dados.

---

## Responsabilidades

- Padronização de schema
- Schema enforcement
- Deduplicação
- Controle incremental
- Persistência Delta Lake

---

## Estratégia Incremental

A camada Raw implementa uma estratégia híbrida incremental:

### Unprocessed

Identificação de registros ainda não processados via comparação incremental.

### Lookback

Reprocessamento determinístico das últimas partições para tratamento de:

- Late arriving data
- Atualizações retroativas
- Consistência incremental

---

## Merge Incremental

Operações de upsert são realizadas utilizando Delta Lake:

- INSERT
- UPDATE

Baseadas em:

- Chave de negócio
- Controle incremental

---

## Colunas Técnicas

Exemplos:

```text
processing_raw
ingestion_timestamp
source_file
```

---

## Armazenamento

### Formato

- Delta Lake

### Benefícios

- ACID transactions
- Merge incremental
- Schema evolution
- Time travel
- Versionamento

---

# 🟢 Trusted Layer

Camada responsável pela qualidade e confiabilidade dos dados.

---

## Responsabilidades

- Limpeza
- Padronização
- Validação
- Regras de negócio
- Enriquecimento

---

## Data Quality

### Validações implementadas

| Regra | Objetivo |
|---|---|
| CPF | Validação de dígito verificador |
| Email | Validação regex |
| Telefone | Padronização |
| Duplicidade | Consistência analítica |

---

## Estratégia Incremental

A camada Trusted reutiliza:

- Unprocessed
- Lookback
- Delta Merge

Mantendo consistência entre as camadas do pipeline.

---

## Flags de Qualidade

Exemplos:

```text
is_valid_cpf
is_valid_email
is_valid_phone
```

---

## Colunas Técnicas

```text
processing_trusted
quality_status
```

---

# 🟣 Refined Layer

Camada analítica baseada em modelagem dimensional.

---

# 🧩 Modelo Dimensional

![Modelo Dimensional](diagrams/modelo_dimensional.png)

---

## Estratégia Analítica

Implementação de modelo dimensional no padrão:

```text
Star Schema
```

---

## Dimensões

| Dimensão | Estratégia |
|---|---|
| dim_cliente | SCD Tipo 2 |
| dim_produto | SCD Tipo 2 |
| dim_pagamento | Snapshot |
| dim_data | Calendário |

---

## Fato

### fato_vendas

Grão definido como:

```text
1 linha = 1 item de pedido
```

---

## Características Analíticas

- Surrogate keys
- Business keys
- Integridade referencial
- Métricas derivadas
- Histórico dimensional

---

## Estratégia SCD Tipo 2

As dimensões históricas utilizam:

- hash_diff
- dt_inicio
- dt_fim
- is_current

Permitindo rastreamento histórico das entidades.

---

# ⚙️ Orquestração

A orquestração é realizada utilizando Apache Airflow.

---

## Responsabilidades do Airflow

- Agendamento
- Dependências
- Retry
- Monitoramento
- Logs centralizados
- Observabilidade

---

## Fluxo de Execução

```text
Airflow DAG
    ↓
SparkSubmitOperator
    ↓
Spark Standalone Cluster
    ↓
PySpark Jobs
    ↓
Delta Lake on HDFS
```

---

# 🧱 Infraestrutura

Toda a infraestrutura é executada localmente via Docker Compose.

---

## Componentes

| Componente | Função |
|---|---|
| Airflow | Orquestração |
| PostgreSQL | Metadata |
| Spark Master | Coordenação do cluster |
| Spark Worker | Execução distribuída |
| HDFS Namenode | Metadata HDFS |
| HDFS Datanode | Armazenamento |
| Docker | Containerização |

---

# 📂 Estrutura do Data Lake

```text
/data/
├── 01_landing/
├── 02_raw/
├── 03_trusted/
└── 04_refined/
```

---

## Formatos por camada

| Camada | Formato |
|---|---|
| Landing | Parquet |
| Raw | Delta Lake |
| Trusted | Delta Lake |
| Refined | Delta Lake |

---

# 🔍 Observabilidade

O pipeline possui mecanismos básicos de observabilidade operacional.

---

## Recursos implementados

- Logs centralizados via Airflow
- Controle incremental por metadata
- Arquivos `_SUCCESS`
- Spark UI
- Retry automático
- Rastreamento de DAGs

---

# 📈 Estratégias de Escalabilidade

A arquitetura foi desenhada visando escalabilidade horizontal.

---

## Estratégias utilizadas

- Processamento distribuído
- Partition pruning
- Batch incremental
- Reprocessamento controlado
- Delta Merge
- Separação por camadas

---

## Batch Incremental vs Streaming

Batch incremental foi escolhido por:

- Simplicidade operacional
- Menor custo computacional
- Cenário adequado para analytics batch

Streaming seria indicado para cenários near real-time.

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

# 🎯 Objetivo Arquitetural

Demonstrar a construção de uma arquitetura moderna de dados próxima de ambientes reais de produção, utilizando conceitos avançados de:

- Engenharia de Dados
- Processamento Distribuído
- Arquitetura Medallion
- Data Lakehouse
- Modelagem Dimensional
- Data Quality
- Governança de Dados

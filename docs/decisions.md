# 📌 Architectural Decisions — Ecommerce Lakehouse Analytics Platform

Este documento registra as principais decisões arquiteturais adotadas no projeto.

O objetivo é demonstrar os critérios técnicos utilizados na construção da arquitetura.

---

# ADR-001 — Uso da Arquitetura Medallion

## Decisão

Utilizar arquitetura em múltiplas camadas:

```text
Landing → Raw → Trusted → Refined
```

## Motivação

Separar responsabilidades do pipeline para melhorar:

* Governança
* Escalabilidade
* Reprocessamento
* Qualidade de dados
* Organização operacional

---

# ADR-002 — Uso de Apache Spark

## Decisão

Utilizar Apache Spark como engine de processamento distribuído.

## Benefícios

* Escalabilidade horizontal
* Processamento distribuído
* Integração com Delta Lake
* Integração com HDFS

---

# ADR-003 — Spark Standalone Cluster

## Decisão

Utilizar Spark Standalone Cluster via Docker Compose.

## Benefícios

* Ambiente distribuído real
* Simplicidade operacional
* Reprodutibilidade

---

# ADR-004 — Uso de Delta Lake

## Decisão

Utilizar Delta Lake nas camadas Raw, Trusted e Refined.

## Benefícios

* ACID transactions
* Merge incremental
* Time travel

---

# ADR-005 — Estratégia Incremental via Watermark

## Decisão

Utilizar estratégia incremental baseada em watermark para controle de ingestão incremental.

## Estratégia

```sql
WHERE data_transacao > watermark
```

## Benefícios

* Redução de I/O
* Melhor performance
* Execuções incrementais eficientes

---

# ADR-006 — Estratégia Híbrida Incremental

## Decisão

Utilizar estratégia híbrida incremental combinando Unprocessed, Lookback e Delta Merge.

## Estratégia

* Unprocessed
* Lookback
* Delta Merge

## Benefícios

* Idempotência
* Consistência incremental
* Tratamento de late arriving data

---

# ADR-007 — Uso de Apache Airflow

## Decisão

Utilizar Apache Airflow para orquestração dos pipelines de dados.

## Motivação

Necessidade de:

* Agendamento
* Observabilidade
* Retry automático
* Controle operacional

---

# ADR-008 — Separação de DAGs por Camada

## Benefícios

* Responsabilidade clara
* Melhor monitoramento
* Facilidade operacional

---

# ADR-009 — Modelagem Dimensional

## Decisão

Utilizar Star Schema na camada Refined.

## Benefícios

* Performance analítica
* Facilidade para BI
* Organização dimensional

---

# ADR-010 — Uso de SCD Tipo 2

## Estratégia

Utilização de:

* hash_diff
* dt_inicio
* dt_fim
* is_current

## Benefícios

* Histórico completo
* Auditoria
* Rastreabilidade

---

# ADR-011 — Particionamento por Data

## Estratégia

```text
dt=YYYY-MM-DD
```

## Benefícios

* Partition pruning
* Melhor performance
* Eficiência incremental

---

# ADR-012 — Docker Compose para Infraestrutura

## Benefícios

* Reprodutibilidade
* Facilidade de setup
* Ambiente isolado

---

# ADR-013 — Uso do Hive Metastore

## Decisão

Utilizar Hive Metastore como catálogo centralizado.

## Motivação

Necessidade de:

* Governança analítica
* SQL serving
* Integração com BI
* Metadata centralizada

## Benefícios

* Centralização de schemas
* Compatibilidade SQL
* Integração analítica

---

# ADR-014 — Uso do Spark ThriftServer

## Decisão

Utilizar Spark ThriftServer como camada SQL.

## Motivação

Necessidade de:

* JDBC/ODBC serving
* Integração com ferramentas BI
* Consultas SQL distribuídas

## Benefícios

* SQL serving distribuído
* Integração com Superset
* Analytics sobre Spark SQL

---

# ADR-015 — Uso do Apache Superset

## Decisão

Utilizar Apache Superset como camada de Business Intelligence.

## Motivação

Necessidade de:

* Dashboards executivos
* Consumo analítico
* Visualização de métricas

## Benefícios

* Dashboards interativos
* Semantic analytics
* Visualização executiva

---

# ADR-016 — Camada Semântica Analítica

## Decisão

Utilizar views analíticas como semantic layer.

## Principal View

```sql
refined.vw_fato_vendas_enriquecida
```

## Benefícios

* Reutilização analítica
* Padronização de métricas
* Simplificação para BI

---
# ADR-017 — Organização da Camada Analytics Engineering

## Decisão

Organizar consultas analíticas, KPIs executivos e semantic layer por domínio analítico dentro da estrutura:

```text
superset/sql/
```
## Estrutura

* semantic_layer
* executive_kpis
* sales_analytics
* customer_analytics
* payment_analytics
* product_analytics

## Motivação

Necessidade de:

* Padronizar consultas analíticas
* Reutilizar SQL entre dashboards
* Centralizar regras analíticas
* Reduzir complexidade na camada BI
* Melhorar organização e manutenção das queries

## Benefícios

* Reutilização analítica
* Padronização de métricas
* Organização modular
* Simplificação dos dashboards
* Melhor governança analítica
* Separação entre serving analítico e visualização BI

---
# 📌 Considerações Finais

As decisões arquiteturais adotadas priorizam:

* Escalabilidade
* Governança
* Analytics distribuído
* SQL serving
* Lakehouse architecture
* Business Intelligence
* Reprocessamento controlado
* Consistência analítica
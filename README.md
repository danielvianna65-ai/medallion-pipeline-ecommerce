# 🛒 Medallion Pipeline E-commerce

## 📌 Visão Geral

Este projeto implementa um pipeline de engenharia de dados end-to-end baseado na arquitetura **Medallion**, com ingestão incremental, processamento distribuído e modelagem dimensional.

O pipeline processa dados de e-commerce provenientes de um banco MySQL e fontes auxiliares (CSV), transformando-os em um modelo analítico pronto para consumo.

---

## 🧱 Stack Tecnológica

* Apache Airflow (orquestração)
* Apache Spark 3.5.8 (processamento distribuído)
* Delta Lake (armazenamento e merge incremental)
* HDFS (data lake)
* Docker Compose (infraestrutura)
* Python (PySpark)

---

## 🔄 Fluxo do Pipeline

```text
MySQL / CSV
   ↓
Landing (ingestão incremental)
   ↓
Raw (CDC + Delta Merge)
   ↓
Trusted (Data Quality + regras de negócio)
   ↓
Refined (modelo dimensional - Star Schema)
```

---

## 🟡 Landing

* Ingestão incremental via **watermark**
* Leitura via JDBC (MySQL)
* Escrita em Parquet particionado
* Controle de estado via metadata em HDFS

---

## 🔵 Raw

* Conversão para Delta Lake
* CDC com **merge incremental**
* Estratégia:

  * backlog (dados não processados)
  * lookback (reprocessamento recente)
* Deduplicação por chave de negócio

---

## 🟢 Trusted

* Limpeza e padronização de dados
* Validações:

  * CPF (regra oficial)
  * Email
  * Telefone
* Flags de qualidade de dados
* Merge incremental consistente

---

## 🟣 Refined

Implementação de modelo dimensional (Star Schema):

### Dimensões

* Cliente (SCD Tipo 2)
* Produto (SCD Tipo 2)
* Pagamento
* Data

### Fato

* Fato de vendas (nível item do pedido)

---

## 📊 Características do Projeto

* Pipeline incremental real (não full reload)
* CDC com Delta Lake
* SCD Tipo 2 implementado
* Data Quality explícita
* Arquitetura distribuída (Spark Cluster)
* Separação clara por camadas

---

## 🚀 Objetivo

Demonstrar uma arquitetura moderna de engenharia de dados, próxima de cenários reais de produção, com foco em:

* Escalabilidade
* Governança
* Qualidade de dados
* Modelagem analítica

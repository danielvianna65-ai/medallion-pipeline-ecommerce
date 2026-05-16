# Analytics Engineering Layer

Esta camada organiza consultas analíticas reutilizáveis utilizadas pelos dashboards Apache Superset da plataforma analytics.

As queries foram separadas por domínio analítico para padronização de métricas, reutilização de SQL e simplificação da camada BI.

---

## Estrutura

```text
superset/sql/

├── customer_analytics
├── executive_kpis
├── payment_analytics
├── product_analytics
├── sales_analytics
└── semantic_layer
```

---

## Semantic Layer

A camada semântica é responsável por abstrair a complexidade da modelagem dimensional utilizada na camada Refined.

### Principal View Analítica

```sql
refined.vw_fato_vendas_enriquecida
```

Essa view consolida:

- Dimensão Cliente
- Dimensão Produto
- Dimensão Data
- Dimensão Pagamento
- Métricas de vendas

A semantic layer é utilizada como base reutilizável para consultas analíticas e dashboards executivos.

---

## Executive KPIs

Camada responsável pelas métricas executivas consolidadas utilizadas no dashboard principal.

### KPIs

- Total de pedidos confirmados
- Receita total
- Ticket médio
- Pedidos não confirmados

---

## Sales Analytics

Consultas analíticas relacionadas à evolução temporal das vendas e comportamento comercial.

### Métricas

- Receita mensal
- Evolução diária da receita
- Receita por categoria
- Receita por dia da semana

---

## Customer Analytics

Consultas analíticas relacionadas ao comportamento e participação financeira dos clientes.

### Métricas

- Top clientes por receita
- Participação financeira dos clientes
- Perfil de consumo

---

## Product Analytics

Consultas analíticas relacionadas à performance comercial dos produtos.

### Métricas

- Top produtos por receita
- Participação financeira dos produtos
- Performance comercial

---

## Payment Analytics

Consultas analíticas relacionadas ao comportamento transacional e financeiro dos pagamentos.

### Métricas

- Volume por status de pagamento
- Tendência de receita por pagamento
- Participação por forma de pagamento

---

## Benefícios Arquiteturais

- Reutilização analítica
- Padronização de métricas
- Simplificação da camada BI
- Organização modular das consultas
- Governança analítica
- Separação entre serving analítico e visualização
- Redução de complexidade nos dashboards

---

## Integração Analítica

Fluxo de consumo analítico:

```text
Spark SQL → Hive Metastore → Spark ThriftServer → Apache Superset
```

A camada SQL atua como abstração analítica reutilizável para consumo via BI e consultas distribuídas.
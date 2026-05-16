# Ecommerce Executive Analytics Dashboard

Dashboard executivo responsável pela análise integrada de vendas, clientes, produtos e pagamentos da plataforma analítica.

---

## Objetivos Analíticos

- Monitorar KPIs executivos do ecommerce
- Acompanhar evolução temporal da receita
- Identificar produtos com maior participação comercial
- Analisar comportamento de clientes
- Monitorar status e tendências de pagamentos
- Avaliar performance operacional do ecommerce

---

## KPIs Executivos

- Total Pedidos
- Receita Total
- Ticket Médio por Pedido
- Pedidos Não Confirmados

---

## Análises Disponíveis

### Sales Analytics

- Receita total por mês
- Evolução diária da receita
- Receita por categoria
- Receita por dia da semana

### Customer Analytics

- Top clientes por receita

### Product Analytics

- Top produtos por receita

### Payment Analytics

- Volume por status de pagamento
- Tendência de pagamentos por status

---

## Camada Analítica

Dataset utilizado:

```sql
refined.vw_fato_vendas_enriquecida
```
---

## Queries SQL

### Executive KPIs

- `superset/sql/executive_kpis/total_pedidos_confirmados.sql`


- `superset/sql/executive_kpis/receita_total_confirmada.sql`


- `superset/sql/executive_kpis/ticket_medio_confirmado.sql`


- `superset/sql/executive_kpis/pedidos_nao_confirmados.sql`

### Sales Analytics

- `superset/sql/sales_analytics/receita_mensal.sql`


- `superset/sql/sales_analytics/evolucao_diaria_receita.sql`


- `superset/sql/sales_analytics/receita_por_categoria.sql`


- `superset/sql/sales_analytics/receita_por_dia_semana.sql`

### Customer Analytics

- `superset/sql/customer_analytics/top_clientes_receita.sql`

### Product Analytics

- `superset/sql/product_analytics/top_produtos_receita.sql`

### Payment Analytics

- `superset/sql/payment_analytics/volume_pedidos_status_pagamento.sql`


- `superset/sql/payment_analytics/tendencia_receita_pagamentos.sql`

---

## Principais Insights

- Evolução temporal das vendas
- Tendências de receita
- Produtos mais relevantes comercialmente
- Clientes com maior participação financeira
- Eficiência operacional dos pagamentos
- Distribuição dos status transacionais

---

## Screenshot

![Executive Dashboard](../../../docs/screenshots/superset_dashboard.png)
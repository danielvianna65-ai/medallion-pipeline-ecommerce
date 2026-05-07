# 📌 Architectural Decisions - Medallion Pipeline E-commerce

Este documento registra as principais decisões arquiteturais adotadas no projeto, incluindo motivações, benefícios e tradeoffs.

O objetivo é demonstrar os critérios técnicos utilizados na construção da arquitetura de dados.

---

# ADR-001 — Uso da Arquitetura Medallion

## Decisão

Utilizar arquitetura em múltiplas camadas:

```text
Landing → Raw → Trusted → Refined
```

---

## Motivação

Separar responsabilidades do pipeline para melhorar:

- Governança
- Escalabilidade
- Reprocessamento
- Qualidade de dados
- Organização operacional

---

## Benefícios

- Isolamento de responsabilidades
- Reprocessamento controlado
- Melhor rastreabilidade
- Separação entre dado bruto e analítico
- Maior clareza operacional

---

## Tradeoffs

- Maior complexidade estrutural
- Mais pipelines para manutenção
- Maior custo de armazenamento

---

# ADR-002 — Uso de Apache Spark

## Decisão

Utilizar Apache Spark como engine de processamento distribuído.

---

## Motivação

O pipeline possui:

- Múltiplas transformações
- Processamento incremental
- Operações distribuídas
- Merge incremental
- Potencial crescimento volumétrico

---

## Benefícios

- Processamento distribuído
- Escalabilidade horizontal
- API madura para ETL
- Integração com Delta Lake
- Integração nativa com HDFS

---

## Tradeoffs

- Maior consumo de memória
- Complexidade operacional superior ao Pandas
- Overhead para pequenos volumes

---

# ADR-003 — Spark Standalone Cluster

## Decisão

Utilizar Spark Standalone Cluster em ambiente Docker Compose.

---

## Motivação

Escolha realizada visando:

- Simplicidade operacional
- Facilidade de desenvolvimento local
- Menor complexidade de infraestrutura
- Ambiente reproduzível

---

## Benefícios

- Fácil configuração
- Baixo overhead operacional
- Ambiente distribuído real
- Melhor aprendizado da arquitetura Spark

---

## Tradeoffs

- Menor elasticidade
- Menor resiliência
- Sem auto-scaling
- Kubernetes seria mais adequado em produção

---

# ADR-004 — Uso de Delta Lake

## Decisão

Utilizar Delta Lake nas camadas Raw, Trusted e Refined.

---

## Motivação

Necessidade de:

- Merge incremental
- Operações ACID
- Controle de schema
- Versionamento
- Reprocessamento confiável

---

## Benefícios

- ACID transactions
- Merge incremental
- Schema enforcement
- Time travel
- Histórico de versões

---

## Tradeoffs

- Dependência adicional de bibliotecas
- Maior complexidade operacional
- Overhead em pequenos datasets

---

# ADR-005 — Estratégia Incremental via Watermark

## Decisão

Implementar ingestão incremental utilizando watermark.

---

## Motivação

Evitar:

- Full reload
- Reprocessamento completo
- Leitura desnecessária de dados históricos

---

## Estratégia

```sql
WHERE data_transacao > watermark
```

---

## Benefícios

- Redução de I/O
- Melhor performance
- Menor custo computacional
- Execuções mais rápidas

---

## Tradeoffs

- Dependência de coluna temporal confiável
- Necessidade de controle de metadata
- Maior complexidade incremental

---

# ADR-006 — Estratégia Híbrida Unprocessed + Lookback

## Decisão

Implementar estratégia incremental híbrida baseada em:

- Unprocessed
- Lookback

---

## Motivação

Garantir:

- Idempotência
- Consistência incremental
- Tratamento de late arriving data
- Reprocessamento controlado

---

## Estratégia

### Unprocessed

Identificação de registros ainda não processados.

### Lookback

Reprocessamento determinístico das últimas partições.

---

## Benefícios

- Maior confiabilidade incremental
- Recuperação de dados atrasados
- Redução de inconsistências
- Reprocessamento eficiente

---

## Tradeoffs

- Maior complexidade lógica
- Leitura adicional de partições recentes
- Mais regras operacionais

---

# ADR-007 — Uso de Apache Airflow

## Decisão

Utilizar Apache Airflow como orquestrador do pipeline.

---

## Motivação

Necessidade de:

- Agendamento
- Dependências
- Observabilidade
- Retry automático
- Controle operacional

---

## Benefícios

- DAGs declarativas
- Monitoramento visual
- Retry automático
- Logs centralizados
- Escalabilidade operacional

---

## Tradeoffs

- Curva de aprendizado
- Complexidade operacional
- Dependência de metadata database

---

# ADR-008 — Separação de DAGs por Camada

## Decisão

Separar DAGs conforme as camadas da arquitetura Medallion.

---

## Motivação

Melhorar:

- Organização
- Observabilidade
- Manutenção
- Reprocessamento

---

## Benefícios

- Responsabilidade clara
- Melhor monitoramento
- Menor acoplamento
- Facilidade operacional

---

## Tradeoffs

- Maior quantidade de DAGs
- Mais dependências entre pipelines
- Maior esforço de coordenação

---

# ADR-009 — Modelagem Dimensional (Star Schema)

## Decisão

Utilizar Star Schema na camada Refined.

---

## Motivação

Necessidade de:

- Performance analítica
- Simplicidade para consumo
- Organização dimensional
- Separação fato/dimensão

---

## Benefícios

- Consultas analíticas eficientes
- Facilidade para BI
- Melhor organização analítica
- Compatibilidade com DW tradicional

---

## Tradeoffs

- Necessidade de modelagem adicional
- Maior esforço de transformação
- Redundância dimensional controlada

---

# ADR-010 — Uso de SCD Tipo 2

## Decisão

Implementar Slowly Changing Dimension Tipo 2 nas dimensões históricas.

---

## Motivação

Necessidade de rastrear:

- Mudanças de atributos
- Histórico de entidades
- Evolução dimensional

---

## Estratégia

Utilização de:

- hash_diff
- dt_inicio
- dt_fim
- is_current

---

## Benefícios

- Histórico completo
- Auditoria
- Análises temporais
- Rastreabilidade

---

## Tradeoffs

- Crescimento volumétrico
- Complexidade incremental
- Lógica adicional de merge

---

# ADR-011 — Particionamento por Data

## Decisão

Utilizar particionamento baseado em data.

---

## Estratégia

```text
dt=YYYY-MM-DD
```

---

## Motivação

Melhorar:

- Partition pruning
- Leitura incremental
- Eficiência de consultas

---

## Benefícios

- Redução de I/O
- Melhor performance
- Melhor gerenciamento de dados
- Escalabilidade

---

## Tradeoffs

- Maior número de diretórios
- Necessidade de gerenciamento de partições
- Possibilidade de small files

---

# ADR-012 — Docker Compose para Infraestrutura

## Decisão

Executar toda a infraestrutura via Docker Compose.

---

## Motivação

Garantir:

- Reprodutibilidade
- Facilidade de setup
- Isolamento de ambiente
- Padronização

---

## Benefícios

- Ambiente portátil
- Facilidade de execução
- Simplicidade operacional
- Consistência local

---

## Tradeoffs

- Não ideal para produção
- Limitações de escala
- Menor resiliência

---

# 📌 Considerações Finais

As decisões arquiteturais adotadas priorizam:

- Clareza estrutural
- Escalabilidade
- Incrementalidade
- Reprocessamento controlado
- Governança
- Confiabilidade analítica

A arquitetura foi construída visando aproximar o projeto de cenários reais de engenharia de dados encontrados em ambientes modernos de Data Lakehouse.

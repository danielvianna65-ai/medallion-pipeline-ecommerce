## 📁 Estrutura do Projeto

```text
medallion-pipeline-ecommerce/
├── airflow/
│   ├── dags/
│   │   ├── 01_landing/
│   │   │   ├── clientes_enrichment_landing_dag.py
│   │   │   └── ecommerce_incremental_landing_dag.py
│   │   │
│   │   ├── 02_raw/
│   │   │   ├── raw_clientes_enrichment_dag.py
│   │   │   └── raw_delta_merge_ecommerce_dag.py
│   │   │
│   │   ├── 03_trusted/
│   │   │   ├── ecommerce_trusted_dag.py
│   │   │   └── trusted_clientes_enrichment_dag.py
│   │   │
│   │   └── 04_refined/
│   │       └── ecommerce_medallion_refined_dag.py
│   │
│   ├── Dockerfile
│   └── logs/
│
├── docs/
│   ├── architecture.md
│   ├── project_structure.md
│   ├── clientes_enrichment.csv
│   └── diagrams/
│       ├── Arquitetura_Medallion_Pipeline.png
│       └── modelo_dimensional.png
│
├── infra/
│   ├── docker/
│   │   └── docker-compose.yml
│   │
│   └── hadoop/
│       ├── core-site.xml
│       └── hdfs-site.xml
│
├── spark/
│   ├── Dockerfile
│   │
│   ├── external-jars/
│   │   ├── delta-spark_2.12-3.2.0.jar
│   │   ├── delta-storage-3.2.0.jar
│   │   └── mysql-connector-j-8.3.0.jar
│   │
│   └── jobs/
│       ├── 01_landing/
│       │   ├── landing_clientes_enrichment.py
│       │   └── landing_incremental.py
│       │
│       ├── 02_raw/
│       │   ├── raw_categorias.py
│       │   ├── raw_clientes_enrichment.py
│       │   ├── raw_clientes.py
│       │   ├── raw_enderecos.py
│       │   ├── raw_estoque.py
│       │   ├── raw_itens_pedido.py
│       │   ├── raw_pagamentos.py
│       │   ├── raw_pedidos.py
│       │   └── raw_produtos.py
│       │
│       ├── 03_trusted/
│       │   ├── trusted_categorias.py
│       │   ├── trusted_clientes_enrichment.py
│       │   ├── trusted_clientes.py
│       │   ├── trusted_enderecos.py
│       │   ├── trusted_estoque.py
│       │   ├── trusted_itens_pedido.py
│       │   ├── trusted_pagamentos.py
│       │   ├── trusted_pedidos.py
│       │   └── trusted_produtos.py
│       │
│       └── 04_refined/
│           ├── refined_dim_cliente.py
│           ├── refined_dim_data.py
│           ├── refined_dim_pagamento.py
│           ├── refined_dim_produto.py
│           └── refined_fato_vendas.py
│
├── main.py
└── README.md
...
```
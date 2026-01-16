medallion-pipeline-ecommerce/
├── airflow/
│   ├── dags/
│   │   ├── landing/
│   │   │   └── ecommerce_landing_dag.py
│   │   ├── raw/
│   │   ├── silver/
│   │   └── gold/
│   └── Dockerfile
│
├── spark/
│   └── jobs/
│       ├── landing/
│       ├── raw/
│       ├── silver/
│       └── gold/
│
├── infra/
│   └── docker/
│        ├── docker-compose.yml
│        ├── spark/
│        │   └── external-jars/
│        │       └── mysql-connector-j-8.3.0.jar
│     hadoop/
│     ├── core-site.xml
│     hdfs-site.xml
│
├── docs/
│   ├── architecture.md
│   └── medallion.md
│
├── .gitignore
└── README.md

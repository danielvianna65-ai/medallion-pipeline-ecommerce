from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    TimestampType
)

pagamentos_schema = StructType([
    StructField("id_pagamento", IntegerType(), False),
    StructField("id_pedido", IntegerType(), False),
    StructField("metodo_pagamento", StringType(), True),
    StructField("status", StringType(), True),
    StructField("valor", DecimalType(10, 2), True),
    StructField("data_pagamento", TimestampType(), True),
])

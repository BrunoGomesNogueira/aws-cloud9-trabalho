from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from config.settings import carregar_config
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, 
    TimestampType, LongType
)

config = carregar_config()
app_name = config['spark']['app_name']
path_pagamentos = config['paths']['pagamentos']

 
# Spark
print("Abrindo a sessao spark")
spark = SparkSession.builder.appName(app_name).getOrCreate()

# Caminhos

#input_path_pagamentos = "data/input/pagamentos"
#input_path_pedidos = "data/input/pedidos"
 
 
# ========================
# PAGAMENTOS
# ========================
 
# Schema para pagamentos
schema_pagamentos = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DoubleType(), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", TimestampType(), True),
    StructField(
        "avaliacao_fraude",
        StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True)
        ]),
        True
    )   
])
 
print("Abrindo o dataframe de pagamentos usando schema manual")
df_pagamentos = spark.read.option("compression", "gzip").json(path_pagamentos, schema=schema_pagamentos)

 
# Flatten
df_pagamentos = (
    df_pagamentos
    .withColumn("fraude", F.col("avaliacao_fraude.fraude"))
    .withColumn("score", F.col("avaliacao_fraude.score"))
    .drop("avaliacao_fraude")
)
 
df_pagamentos.show()
df_pagamentos.printSchema()
 
 
# ========================
# PEDIDOS
# ========================
 
 
# Schema para pedidos
schema_pedidos = StructType([
    StructField("ID_PEDIDO", StringType(), True),
    StructField("PRODUTO", StringType(), True),
    StructField("VALOR_UNITARIO", DoubleType(), True),
    StructField("QUANTIDADE", LongType(), True),
    StructField("DATA_CRIACAO", TimestampType(), True),
    StructField("UF", StringType(), True),
    StructField("ID_CLIENTE", LongType(), True)
])
 
print("Abrindo o dataframe de pedidos")
df_pedidos = (
    spark.read
         .csv(f"{PEDIDOS_PATH}/*.csv.gz", header=True, schema = schema_pedidos, sep=";")
)
 ## Adicionando no relatório o valor total do pedido 
df_pedidos = df_pedidos.withColumn("VALOR_TOTAL", col("VALOR_UNITARIO") * col("QUANTIDADE"))
df_pedidos.show()
df_pedidos.printSchema()

 
## Filtrando pagamentos
df_pagamento_filtrado = df_pagamentos.filter(
    (F.col("status") == False) & 
    (F.col("fraude") == False )
)
df_pagamentos.show()
 
 
## Filtrando pedidos
df_pedidos_filtrado = df_pedidos.filter(
    (F.year(F.col("DATA_CRIACAO")) == 2025 )
)
df_pedidos_filtrado.show()
 
 
## Realizando o join da tabelas
df_join = df_pagamento_filtrado.join(df_pedidos_filtrado, on="ID_PEDIDO", how="inner")
 
## Selecionando as tabelas necessárias
df_relatorio = df_join.select(
    "id_pedido",
    "UF",
    "forma_pagamento",
    "VALOR_UNITARIO",
    "DATA_CRIACAO"
)
 
## Ordenando
df_relatorio = df_relatorio.orderBy(
    F.col("UF").asc(),
    F.col("forma_pagamento").desc(),
    F.col("DATA_CRIACAO").desc()
)
 
 
df_relatorio.show()
 
## salva em parquet
print("Salvando em parquet")
df_relatorio.write.mode("overwrite").parquet(OUTPUT_PATH)



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, 
    TimestampType, LongType
)

config = carregar_config()
app_name = config['spark']['app_name']
spark = SparkSessionManager.get_spark_session(app_name=app_name)
path_pagamentos = config['paths']['pagamentos']
path_pedidos = config['paths']['pedidos']
output_path = config['paths']['output']
dh = DataHandler(spark)
 
# Spark
print("Abrindo a sessao spark")
spark = SparkSession.builder.appName(app_name).getOrCreate()

# Caminhos

#input_path_pagamentos = "data/input/pagamentos"
#input_path_pedidos = "data/input/pedidos"
 
 
# ========================
# PAGAMENTOS
# ========================
print("Abrindo o dataframe de pagamentos usando schema manual")
df_pagamentos = dh.load_pagamentos(path = path_pagamentos)

 
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
print("Abrindo o dataframe de pedidos")

compression_pedidos = config['file_options']['pedidos_csv']['compression']
header_pedidos = config['file_options']['pedidos_csv']['header']
separator_pedidos = config['file_options']['pedidos_csv']['sep']
df_pedidos = dh.load_pedidos(path = path_pedidos, compression=compression_pedidos, header=header_pedidos, sep=separator_pedidos)
df_pedidos.show()

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
dh.write_parquet(df=df_relatorio, path=output_path)


# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                              ArrayType, DateType, FloatType, DoubleType, BooleanType, TimestampType)

class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe dos Pagamentos."""
        return StructType([
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

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType([
                StructField("ID_PEDIDO", StringType(), True),
                StructField("PRODUTO", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("QUANTIDADE", LongType(), True),
                StructField("DATA_CRIACAO", TimestampType(), True),
                StructField("UF", StringType(), True),
                StructField("ID_CLIENTE", LongType(), True)
])

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", "gzip").json(path, schema=schema)

    def load_pedidos(self, path: str, compression: str, header:bool, sep:str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        return self.spark.read.option("compression", compression).csv(path, header=header, schema=schema, sep=sep)

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        print(f"Dados salvos com sucesso em: {path}")

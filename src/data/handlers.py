# src/data/handlers.py
from core.logger import get_logger
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    DateType,
    FloatType,
    DoubleType,
    BooleanType,
    TimestampType,
)

logger = get_logger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe dos Pagamentos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", DoubleType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), True),
                            StructField("score", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType(
            [
                StructField("ID_PEDIDO", StringType(), True),
                StructField("PRODUTO", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("QUANTIDADE", LongType(), True),
                StructField("DATA_CRIACAO", TimestampType(), True),
                StructField("UF", StringType(), True),
                StructField("ID_CLIENTE", LongType(), True),
            ]
        )

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        try:
            return self.spark.read.option("compression", "gzip").json(path, schema=schema)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")
            logger.error(f"Erro Spark ao carregar pagamentos: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar pagamentos: {e}")
            raise

    def load_pedidos(self, path: str, compression: str, header: bool, sep: str) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        try:
            return self.spark.read.option("compression", compression).csv(path, header=header, schema=schema, sep=sep)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")
            logger.error(f"Erro Spark ao carregar pedidos: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar pedidos: {e}")
            raise

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.
        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        try:
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Dados salvos com sucesso em: {path}")
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Caminho não encontrado: {path}")
            logger.error(f"Erro Spark ao salvar parquet: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar parquet: {e}")
            raise

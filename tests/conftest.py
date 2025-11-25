"""
Configuração de fixtures compartilhadas para os testes.

Este arquivo contém fixtures reutilizáveis que podem ser usadas
em todos os testes do projeto.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    BooleanType,
)
from src.data.transformations import Transformation

# ==================== Spark Session ====================


@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.

    Scope: session - compartilhada entre todos os testes
    """
    spark = (
        SparkSession.builder.appName("PySpark Unit Tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")  # Otimização para testes
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ==================== Transformer ====================


@pytest.fixture
def transformer():
    """
    Retorna uma instância de Transformation para os testes.

    Scope: function - nova instância para cada teste
    """
    return Transformation()


# ==================== Schemas ====================


@pytest.fixture
def schema_pedidos():
    """Schema para DataFrame de pedidos."""
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


@pytest.fixture
def schema_pagamentos():
    """Schema para DataFrame de pagamentos."""
    return StructType(
        [
            StructField("ID_PEDIDO", StringType(), True),  # Maiúscula para join
            StructField("forma_pagamento", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("fraude", BooleanType(), True),
            StructField("score", DoubleType(), True),
        ]
    )


@pytest.fixture
def schema_pagamentos_com_struct():
    """Schema para DataFrame de pagamentos com struct de avaliação."""
    return StructType(
        [
            StructField("ID_PEDIDO", StringType(), True),  # Maiúscula para join
            StructField("forma_pagamento", StringType(), True),
            StructField("status", BooleanType(), True),
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


# ==================== Dados de Teste - Pedidos ====================


@pytest.fixture
def dados_pedidos_validos():
    """Dados válidos de pedidos para testes."""
    return [
        ("P001", "Notebook", 3500.00, 1, datetime(2025, 11, 15, 9, 0, 0), "SP", 101),
        ("P002", "Mouse", 120.50, 2, datetime(2025, 11, 14, 10, 0, 0), "RJ", 102),
        ("P003", "Teclado", 250.00, 1, datetime(2025, 11, 15, 8, 0, 0), "MG", 103),
    ]


@pytest.fixture
def dados_pedidos_2024():
    """Dados de pedidos do ano 2024."""
    return [
        ("P004", "Monitor", 1500.00, 1, datetime(2024, 12, 15, 9, 0, 0), "SP", 104),
        ("P005", "Webcam", 300.00, 1, datetime(2024, 11, 14, 10, 0, 0), "RJ", 105),
    ]


@pytest.fixture
def dados_pedidos_edge_cases():
    """Dados de pedidos com edge cases."""
    return [
        ("P006", "Produto Zero", 0.0, 1, datetime(2025, 1, 1, 0, 0, 0), "BA", 106),
        ("P007", "Quantidade Zero", 100.0, 0, datetime(2025, 1, 2, 0, 0, 0), "CE", 107),
        ("P008", "Valores Grandes", 99999.99, 1000, datetime(2025, 1, 3, 0, 0, 0), "DF", 108),
    ]


@pytest.fixture
def dados_pedidos_com_null():
    """Dados de pedidos com valores nulos."""
    return [
        ("P009", "Produto Null", None, 1, datetime(2025, 1, 1, 0, 0, 0), "SP", 109),
        ("P010", "Quantidade Null", 100.0, None, datetime(2025, 1, 2, 0, 0, 0), "RJ", 110),
    ]


# ==================== Dados de Teste - Pagamentos ====================


@pytest.fixture
def dados_pagamentos_validos():
    """Dados válidos de pagamentos para testes."""
    return [
        ("P001", "credito", False, False, 0.1),
        ("P002", "debito", False, False, 0.2),
        ("P003", "pix", False, False, 0.15),
    ]


@pytest.fixture
def dados_pagamentos_com_fraude():
    """Dados de pagamentos com indicação de fraude."""
    return [
        ("P001", "credito", False, True, 0.95),  # Com fraude
        ("P002", "debito", False, False, 0.1),  # Sem fraude
        ("P003", "pix", True, False, 0.2),  # Status inválido
    ]


@pytest.fixture
def dados_pagamentos_com_struct():
    """Dados de pagamentos com struct de avaliação de fraude."""
    return [
        ("P001", "credito", False, {"fraude": False, "score": 0.1}),
        ("P002", "debito", False, {"fraude": True, "score": 0.9}),
        ("P003", "pix", False, {"fraude": False, "score": 0.2}),
    ]


# ==================== DataFrames de Teste ====================


@pytest.fixture
def df_pedidos_validos(spark_session, schema_pedidos, dados_pedidos_validos):
    """DataFrame de pedidos válidos."""
    return spark_session.createDataFrame(dados_pedidos_validos, schema_pedidos)


@pytest.fixture
def df_pedidos_2024(spark_session, schema_pedidos, dados_pedidos_2024):
    """DataFrame de pedidos de 2024."""
    return spark_session.createDataFrame(dados_pedidos_2024, schema_pedidos)


@pytest.fixture
def df_pedidos_edge_cases(spark_session, schema_pedidos, dados_pedidos_edge_cases):
    """DataFrame de pedidos com edge cases."""
    return spark_session.createDataFrame(dados_pedidos_edge_cases, schema_pedidos)


@pytest.fixture
def df_pagamentos_validos(spark_session, schema_pagamentos, dados_pagamentos_validos):
    """DataFrame de pagamentos válidos."""
    return spark_session.createDataFrame(dados_pagamentos_validos, schema_pagamentos)


@pytest.fixture
def df_pagamentos_com_fraude(spark_session, schema_pagamentos, dados_pagamentos_com_fraude):
    """DataFrame de pagamentos com fraude."""
    return spark_session.createDataFrame(dados_pagamentos_com_fraude, schema_pagamentos)


@pytest.fixture
def df_pagamentos_com_struct(spark_session, schema_pagamentos_com_struct, dados_pagamentos_com_struct):
    """DataFrame de pagamentos com struct de fraude."""
    return spark_session.createDataFrame(dados_pagamentos_com_struct, schema_pagamentos_com_struct)


# ==================== Helpers ====================


@pytest.fixture
def assert_dataframes_equal():
    """
    Fixture helper para comparar DataFrames.

    Retorna uma função que compara dois DataFrames e verifica se são iguais.
    """

    def _assert_equal(df_resultado: DataFrame, df_esperado: DataFrame):
        """Compara dois DataFrames e levanta AssertionError se diferentes."""
        # Verificar contagem
        assert (
            df_resultado.count() == df_esperado.count()
        ), f"Contagem diferente: {df_resultado.count()} vs {df_esperado.count()}"

        # Verificar colunas
        assert (
            df_resultado.columns == df_esperado.columns
        ), f"Colunas diferentes: {df_resultado.columns} vs {df_esperado.columns}"

        # Verificar conteúdo
        resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: str(x))
        esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: str(x))

        assert resultado_coletado == esperado_coletado, "Conteúdo dos DataFrames é diferente"

    return _assert_equal

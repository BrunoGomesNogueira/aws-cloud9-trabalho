# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, LongType, TimestampType
from src.processing.transformations import Transformation
from datetime import datetime


@pytest.fixture(scope="session")
def spark_session():
    """
        Cria uma SparkSession para ser usada em todos os testes.
        A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_add_valor_total_pedidos(spark_session):
    """
    Testa a função add_valor_total_pedidos para garantir que a coluna 'valor_total'
    é calculada corretamente.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("ID_PEDIDO", StringType(), True),
        StructField("PRODUTO", StringType(), True),
        StructField("VALOR_UNITARIO", DoubleType(), True),
        StructField("QUANTIDADE", LongType(), True),
        StructField("DATA_CRIACAO", TimestampType(), True),
        StructField("UF", StringType(), True),
        StructField("ID_CLIENTE", LongType(), True),
    ])
    dados_entrada = [
        ("P001", "Notebook", 3500.00, 1, datetime(2025, 11, 15, 9, 0, 0), "SP", 101),
        ("P002", "Mouse", 120.50, 2, datetime(2025, 11, 14, 10, 0, 0), "RJ", 102),
        ("P003", "Teclado", 250.00, 1, datetime(2025, 11, 15, 8, 0, 0), "MG", 103)


    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    schema_esperado = StructType([
        StructField("ID_PEDIDO", StringType(), True),
        StructField("PRODUTO", StringType(), True),
        StructField("VALOR_UNITARIO", DoubleType(), True),
        StructField("QUANTIDADE", LongType(), True),
        StructField("DATA_CRIACAO", TimestampType(), True),
        StructField("UF", StringType(), True),
        StructField("ID_CLIENTE", LongType(), True),
        StructField("VALOR_TOTAL", DoubleType(), True),
    ])
    dados_esperados = [
        ("P001", "Notebook", 3500.00, 1, datetime(2025, 11, 15, 9, 0, 0), "SP", 101, 3500.00),
        ("P002", "Mouse", 120.50, 2, datetime(2025, 11, 14, 10, 0, 0), "RJ", 102, 241.00),
        ("P003", "Teclado", 250.00, 1, datetime(2025, 11, 15, 8, 0, 0), "MG", 103, 250.00)
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act (Executar a função a ser testada)
    df_resultado = transformer.add_valor_total_pedidos(df_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['PRODUTO'])
    esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x['PRODUTO'])

    assert df_resultado.count() == df_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."

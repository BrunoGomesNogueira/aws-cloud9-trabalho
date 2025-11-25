"""
Testes para o módulo de transformações PySpark.

Este módulo contém testes organizados em classes para cada
função de transformação, incluindo casos de sucesso e edge cases.
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    BooleanType,
)


class TestAvaliacaoFraude:
    """Testes para a função avaliacao_fraude."""

    def test_expande_campos_fraude_sucesso(self, transformer, df_pagamentos_com_struct, spark_session):
        """
        Testa se a função expande corretamente os campos de fraude do struct.
        """
        # Act
        resultado = transformer.avaliacao_fraude(df_pagamentos_com_struct)

        # Assert
        assert "fraude" in resultado.columns
        assert "score" in resultado.columns
        assert "avaliacao_fraude" not in resultado.columns
        assert resultado.count() == 3

    def test_valores_fraude_corretos(self, transformer, df_pagamentos_com_struct):
        """Testa se os valores de fraude foram expandidos corretamente."""
        # Act
        resultado = transformer.avaliacao_fraude(df_pagamentos_com_struct)

        # Assert - Verificar primeiro registro
        primeiro_registro = resultado.filter(F.col("ID_PEDIDO") == "P001").first()
        assert primeiro_registro["fraude"] == False
        assert primeiro_registro["score"] == 0.1

        # Assert - Verificar registro com fraude
        registro_fraude = resultado.filter(F.col("ID_PEDIDO") == "P002").first()
        assert registro_fraude["fraude"] == True
        assert registro_fraude["score"] == 0.9


class TestAddValorTotal:
    """Testes para a função add_valor_total_pedidos."""

    def test_calculo_valor_total_correto(self, transformer, df_pedidos_validos):
        """
        Testa se o valor total é calculado corretamente (preço × quantidade).
        """
        # Act
        resultado = transformer.add_valor_total_pedidos(df_pedidos_validos)

        # Assert
        assert "VALOR_TOTAL" in resultado.columns

        # Verificar cálculos específicos
        dados = resultado.collect()
        assert dados[0]["VALOR_TOTAL"] == 3500.00  # 3500 * 1
        assert dados[1]["VALOR_TOTAL"] == 241.00  # 120.50 * 2
        assert dados[2]["VALOR_TOTAL"] == 250.00  # 250 * 1

    def test_valor_total_com_zero(self, transformer, spark_session, schema_pedidos):
        """Testa cálculo quando preço ou quantidade é zero."""
        # Arrange
        dados = [
            ("P001", "Produto", 0.0, 5, datetime(2025, 1, 1, 0, 0, 0), "SP", 101),
            ("P002", "Produto", 100.0, 0, datetime(2025, 1, 1, 0, 0, 0), "RJ", 102),
        ]
        df = spark_session.createDataFrame(dados, schema_pedidos)

        # Act
        resultado = transformer.add_valor_total_pedidos(df)

        # Assert
        dados_resultado = resultado.collect()
        assert dados_resultado[0]["VALOR_TOTAL"] == 0.0  # 0 * 5
        assert dados_resultado[1]["VALOR_TOTAL"] == 0.0  # 100 * 0

    def test_valor_total_com_valores_grandes(self, transformer, df_pedidos_edge_cases):
        """Testa cálculo com valores muito grandes."""
        # Act
        resultado = transformer.add_valor_total_pedidos(df_pedidos_edge_cases)

        # Assert
        registro_grande = resultado.filter(F.col("ID_PEDIDO") == "P008").first()
        assert registro_grande["VALOR_TOTAL"] == 99999990.0  # 99999.99 * 1000

    def test_valor_total_com_null(self, transformer, spark_session, schema_pedidos, dados_pedidos_com_null):
        """Testa comportamento quando há valores null."""
        # Arrange
        df = spark_session.createDataFrame(dados_pedidos_com_null, schema_pedidos)

        # Act
        resultado = transformer.add_valor_total_pedidos(df)

        # Assert
        dados_resultado = resultado.collect()
        assert dados_resultado[0]["VALOR_TOTAL"] is None  # None * 1
        assert dados_resultado[1]["VALOR_TOTAL"] is None  # 100 * None

    def test_nao_modifica_colunas_originais(self, transformer, df_pedidos_validos):
        """Testa se as colunas originais são preservadas."""
        # Arrange
        colunas_originais = df_pedidos_validos.columns

        # Act
        resultado = transformer.add_valor_total_pedidos(df_pedidos_validos)

        # Assert
        for coluna in colunas_originais:
            assert coluna in resultado.columns


class TestFiltraPagamentos:
    """Testes para a função filtra_pagamentos_aprovados_sem_fraude."""

    def test_filtra_apenas_pagamentos_validos(self, transformer, df_pagamentos_com_fraude):
        """
        Testa se filtra apenas pagamentos com status=False e fraude=False.
        """
        # Act
        resultado = transformer.filtra_pagamentos_aprovados_sem_fraude(df_pagamentos_com_fraude)

        # Assert
        assert resultado.count() == 1  # Apenas P002 é válido

        # Verificar que o registro correto foi mantido
        registro = resultado.first()
        assert registro["ID_PEDIDO"] == "P002"
        assert registro["status"] == False
        assert registro["fraude"] == False

    def test_remove_pagamentos_com_fraude(self, transformer, df_pagamentos_com_fraude):
        """Testa se pagamentos com fraude=True são removidos."""
        # Act
        resultado = transformer.filtra_pagamentos_aprovados_sem_fraude(df_pagamentos_com_fraude)

        # Assert
        ids = [row["ID_PEDIDO"] for row in resultado.collect()]
        assert "P001" not in ids  # P001 tem fraude=True

    def test_remove_pagamentos_com_status_invalido(self, transformer, df_pagamentos_com_fraude):
        """Testa se pagamentos com status=True são removidos."""
        # Act
        resultado = transformer.filtra_pagamentos_aprovados_sem_fraude(df_pagamentos_com_fraude)

        # Assert
        ids = [row["ID_PEDIDO"] for row in resultado.collect()]
        assert "P003" not in ids  # P003 tem status=True

    def test_retorna_vazio_quando_nenhum_valido(self, transformer, spark_session, schema_pagamentos):
        """Testa comportamento quando não há pagamentos válidos."""
        # Arrange - Todos inválidos
        dados = [
            ("P001", "credito", True, False, 0.1),  # status=True
            ("P002", "debito", False, True, 0.9),  # fraude=True
            ("P003", "pix", True, True, 0.95),  # ambos=True
        ]
        df = spark_session.createDataFrame(dados, schema_pagamentos)

        # Act
        resultado = transformer.filtra_pagamentos_aprovados_sem_fraude(df)

        # Assert
        assert resultado.count() == 0


class TestFiltraPedidos:
    """Testes para a função filtra_pedidos_2025."""

    def test_filtra_apenas_pedidos_2025(self, transformer, df_pedidos_validos, df_pedidos_2024, spark_session):
        """Testa se filtra apenas pedidos do ano 2025."""
        # Arrange - Unir pedidos de 2024 e 2025
        df_combinado = df_pedidos_validos.union(df_pedidos_2024)

        # Act
        resultado = transformer.filtra_pedidos_2025(df_combinado)

        # Assert
        assert resultado.count() == 3  # Apenas os 3 de 2025

        # Verificar que todos são de 2025
        for row in resultado.collect():
            ano = row["DATA_CRIACAO"].year
            assert ano == 2025

    def test_remove_pedidos_2024(self, transformer, df_pedidos_validos, df_pedidos_2024, spark_session):
        """Testa se pedidos de 2024 são removidos."""
        # Arrange
        df_combinado = df_pedidos_validos.union(df_pedidos_2024)

        # Act
        resultado = transformer.filtra_pedidos_2025(df_combinado)

        # Assert
        ids = [row["ID_PEDIDO"] for row in resultado.collect()]
        assert "P004" not in ids  # P004 é de 2024
        assert "P005" not in ids  # P005 é de 2024

    def test_mantem_todos_pedidos_2025(self, transformer, df_pedidos_validos):
        """Testa se todos os pedidos de 2025 são mantidos."""
        # Act
        resultado = transformer.filtra_pedidos_2025(df_pedidos_validos)

        # Assert
        assert resultado.count() == df_pedidos_validos.count()

    def test_retorna_vazio_quando_nenhum_2025(self, transformer, df_pedidos_2024):
        """Testa comportamento quando não há pedidos de 2025."""
        # Act
        resultado = transformer.filtra_pedidos_2025(df_pedidos_2024)

        # Assert
        assert resultado.count() == 0


class TestJoinPagamentosPedidos:
    """Testes para a função join_pagamentos_pedidos."""

    def test_join_retorna_colunas_corretas(self, transformer, df_pagamentos_validos, df_pedidos_validos):
        """Testa se o join retorna apenas as colunas esperadas."""
        # Act
        resultado = transformer.join_pagamentos_pedidos(df_pagamentos_validos, df_pedidos_validos)

        # Assert
        colunas_esperadas = ["ID_PEDIDO", "UF", "forma_pagamento", "VALOR_UNITARIO", "DATA_CRIACAO"]
        assert resultado.columns == colunas_esperadas

    def test_join_inner_combina_corretamente(self, transformer, df_pagamentos_validos, df_pedidos_validos):
        """Testa se o join inner combina registros corretamente."""
        # Act
        resultado = transformer.join_pagamentos_pedidos(df_pagamentos_validos, df_pedidos_validos)

        # Assert
        assert resultado.count() == 3  # Todos os 3 registros têm match

        # Verificar dados de um registro específico
        registro = resultado.filter(F.col("ID_PEDIDO") == "P001").first()
        assert registro["forma_pagamento"] == "credito"
        assert registro["UF"] == "SP"

    def test_join_sem_match_retorna_vazio(self, transformer, spark_session, schema_pagamentos, schema_pedidos):
        """Testa comportamento quando não há match entre DataFrames."""
        # Arrange - Pagamentos e pedidos sem IDs em comum
        dados_pag = [("P999", "credito", False, False, 0.1)]
        dados_ped = [("P001", "Prod", 100.0, 1, datetime(2025, 1, 1, 0, 0, 0), "SP", 1)]

        df_pag = spark_session.createDataFrame(dados_pag, schema_pagamentos)
        df_ped = spark_session.createDataFrame(dados_ped, schema_pedidos)

        # Act
        resultado = transformer.join_pagamentos_pedidos(df_pag, df_ped)

        # Assert
        assert resultado.count() == 0


class TestOrdenar:
    """Testes para a função ordenar_por_uf_forma_pagamento_data_criacao."""

    def test_ordenacao_correta(self, transformer, spark_session):
        """Testa se a ordenação é aplicada corretamente."""
        # Arrange - Criar DataFrame desordenado
        schema = StructType(
            [
                StructField("ID_PEDIDO", StringType(), True),
                StructField("UF", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("DATA_CRIACAO", TimestampType(), True),
            ]
        )

        # Ordem esperada: UF asc, forma_pagamento desc, DATA_CRIACAO desc
        dados = [
            ("P004", "SP", "pix", 400.0, datetime(2025, 1, 4, 0, 0, 0)),  # SP, pix, 04/jan
            ("P003", "SP", "credito", 300.0, datetime(2025, 1, 1, 0, 0, 0)),  # SP, credito, 01/jan
            ("P002", "MG", "debito", 200.0, datetime(2025, 1, 2, 0, 0, 0)),  # MG, debito, 02/jan
            ("P001", "MG", "credito", 100.0, datetime(2025, 1, 3, 0, 0, 0)),  # MG, credito, 03/jan
        ]
        df = spark_session.createDataFrame(dados, schema)

        # Act
        resultado = transformer.ordenar_por_uf_forma_pagamento_data_criacao(df)

        # Assert
        dados_ordenados = resultado.collect()

        # Ordem esperada:
        # 1. MG, debito, 02/jan (P002)
        # 2. MG, credito, 03/jan (P001)
        # 3. SP, pix, 04/jan (P004)
        # 4. SP, credito, 01/jan (P003)
        assert dados_ordenados[0]["ID_PEDIDO"] == "P002"  # MG, debito
        assert dados_ordenados[1]["ID_PEDIDO"] == "P001"  # MG, credito
        assert dados_ordenados[2]["ID_PEDIDO"] == "P004"  # SP, pix
        assert dados_ordenados[3]["ID_PEDIDO"] == "P003"  # SP, credito

    def test_ordenacao_uf_crescente(self, transformer, spark_session):
        """Testa se UF é ordenado de forma crescente."""
        # Arrange
        schema = StructType(
            [
                StructField("ID_PEDIDO", StringType(), True),
                StructField("UF", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("DATA_CRIACAO", TimestampType(), True),
            ]
        )

        dados = [
            ("P1", "SP", "pix", 100.0, datetime(2025, 1, 1, 0, 0, 0)),
            ("P2", "BA", "pix", 100.0, datetime(2025, 1, 1, 0, 0, 0)),
            ("P3", "RJ", "pix", 100.0, datetime(2025, 1, 1, 0, 0, 0)),
        ]
        df = spark_session.createDataFrame(dados, schema)

        # Act
        resultado = transformer.ordenar_por_uf_forma_pagamento_data_criacao(df)

        # Assert
        ufs = [row["UF"] for row in resultado.collect()]
        assert ufs == ["BA", "RJ", "SP"]  # Ordem alfabética crescente

    def test_ordenacao_com_um_registro(self, transformer, spark_session):
        """Testa comportamento com apenas um registro."""
        # Arrange
        schema = StructType(
            [
                StructField("ID_PEDIDO", StringType(), True),
                StructField("UF", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("DATA_CRIACAO", TimestampType(), True),
            ]
        )

        dados = [("P1", "SP", "pix", 100.0, datetime(2025, 1, 1, 0, 0, 0))]
        df = spark_session.createDataFrame(dados, schema)

        # Act
        resultado = transformer.ordenar_por_uf_forma_pagamento_data_criacao(df)

        # Assert
        assert resultado.count() == 1
        assert resultado.first()["ID_PEDIDO"] == "P1"

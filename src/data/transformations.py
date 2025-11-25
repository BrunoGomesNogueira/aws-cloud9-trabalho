"""
Módulo de transformações PySpark.

Este módulo contém todas as transformações aplicadas aos DataFrames
do pipeline de dados, seguindo boas práticas de PySpark.
"""

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from core.logger import get_logger

logger = get_logger(__name__)


class Transformation:
    """
    Classe responsável por todas as transformações de dados do pipeline.

    Utiliza a Column API do PySpark para operações type-safe e otimizadas.
    """

    # ==================== Colunas de Fraude ====================

    def avaliacao_fraude(self, df_pagamentos: DataFrame) -> DataFrame:
        """
        Extrai informações de avaliação de fraude para colunas separadas.

        Args:
            df_pagamentos: DataFrame com coluna 'avaliacao_fraude' (struct)

        Returns:
            DataFrame com colunas 'fraude' e 'score' expandidas

        Example:
            >>> df_resultado = transformer.avaliacao_fraude(df_pagamentos)
        """
        logger.debug("Expandindo campos de avaliação de fraude")

        return (
            df_pagamentos.withColumn("fraude", F.col("avaliacao_fraude.fraude"))
            .withColumn("score", F.col("avaliacao_fraude.score"))
            .drop("avaliacao_fraude")
        )

    # ==================== Colunas Calculadas ====================

    def add_valor_total_pedidos(self, df_pedidos: DataFrame) -> DataFrame:
        """
        Adiciona coluna 'VALOR_TOTAL' calculada como preço × quantidade.

        Args:
            df_pedidos: DataFrame com colunas 'VALOR_UNITARIO' e 'QUANTIDADE'

        Returns:
            DataFrame com coluna adicional 'VALOR_TOTAL'

        Example:
            >>> df_com_total = transformer.add_valor_total_pedidos(df_pedidos)
        """
        logger.debug("Calculando valor total dos pedidos")

        return df_pedidos.withColumn("VALOR_TOTAL", F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"))

    # ==================== Filtros ====================

    def filtra_pagamentos_aprovados_sem_fraude(self, df_pagamentos: DataFrame) -> DataFrame:
        """
        Filtra pagamentos aprovados e sem fraude.

        Mantém apenas registros onde:
        - status = False (pagamento aprovado)
        - fraude = False (sem indicação de fraude)

        Args:
            df_pagamentos: DataFrame com colunas 'status' e 'fraude'

        Returns:
            DataFrame filtrado com pagamentos válidos

        Example:
            >>> df_validos = transformer.filtra_pagamentos_aprovados_sem_fraude(df_pagamentos)
        """
        logger.debug("Filtrando pagamentos aprovados e sem fraude")

        condicao = self._criar_condicao_pagamento_valido_sem_fraude()
        return df_pagamentos.filter(condicao)

    def filtra_pedidos_2025(self, df_pedidos: DataFrame) -> DataFrame:
        """
        Filtra pedidos do ano de 2025.

        Args:
            df_pedidos: DataFrame com coluna 'DATA_CRIACAO' (timestamp)

        Returns:
            DataFrame filtrado com pedidos de 2025

        Example:
            >>> df_2025 = transformer.filtra_pedidos_2025(df_pedidos)
        """
        logger.debug("Filtrando pedidos do ano 2025")

        condicao = self._criar_condicao_ano(2025)
        return df_pedidos.filter(condicao)

    # ==================== Condições Reutilizáveis ====================

    @staticmethod
    def _criar_condicao_pagamento_valido_sem_fraude() -> Column:
        """
        Cria expressão Column para pagamentos válidos.

        Returns:
            Condição: status = False AND fraude = False
        """
        return (F.col("status") == False) & (F.col("fraude") == False)

    @staticmethod
    def _criar_condicao_ano(ano: int) -> Column:
        """
        Cria expressão Column para filtrar por ano.

        Args:
            ano: Ano a filtrar

        Returns:
            Condição: YEAR(DATA_CRIACAO) = ano
        """
        return F.year(F.col("DATA_CRIACAO")) == ano

    # ==================== Joins ====================

    def join_pagamentos_pedidos(self, df_pagamentos: DataFrame, df_pedidos: DataFrame) -> DataFrame:
        """
        Realiza join entre pagamentos e pedidos.

        Join interno (INNER) usando 'ID_PEDIDO' como chave,
        selecionando apenas colunas relevantes para o relatório final.

        Args:
            df_pagamentos: DataFrame de pagamentos
            df_pedidos: DataFrame de pedidos

        Returns:
            DataFrame com dados combinados e colunas selecionadas

        Example:
            >>> df_relatorio = transformer.join_pagamentos_pedidos(
            ...     df_pagamentos, df_pedidos
            ... )
        """
        logger.debug("Realizando join entre pagamentos e pedidos")
        
        colunas_selecionadas = [
            "ID_PEDIDO",
            "UF",
            "forma_pagamento",
            "VALOR_UNITARIO",
            "DATA_CRIACAO"
        ]
        
        return (
            df_pagamentos
            .join(df_pedidos, on="ID_PEDIDO", how="inner")
            .select(*colunas_selecionadas)
        )

    # ==================== Ordenação ====================

    def ordenar_por_uf_forma_pagamento_data_criacao(self, df_relatorio: DataFrame) -> DataFrame:
        """
        Ordena o relatório final por múltiplas colunas.

        Ordem de classificação:
        1. UF (crescente)
        2. forma_pagamento (decrescente)
        3. DATA_CRIACAO (decrescente)

        Args:
            df_relatorio: DataFrame a ser ordenado

        Returns:
            DataFrame ordenado

        Example:
            >>> df_ordenado = transformer.ordenar_por_uf_forma_pagamento_data_criacao(df_relatorio)
        """
        logger.debug("Ordenando relatório final")

        return df_relatorio.orderBy(
            F.col("UF").asc(),
            F.col("forma_pagamento").desc(),
            F.col("DATA_CRIACAO").desc(),
        )

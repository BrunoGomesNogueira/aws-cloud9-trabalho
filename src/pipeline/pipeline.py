# src/pipeline/pipeline.py
import logging
from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import config.settings as settings

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation()

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")

        logger.info("Abrindo o dataframe de pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]
        try:
            df_pagamentos = self.data_handler.load_pagamentos(path=path_pagamentos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de Pagamentos: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        df_pagamentos.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]
        try:
            df_pedidos = self.data_handler.load_pedidos(
                path=path_pedidos,
                compression=compression_pedidos,
                header=header_pedidos,
                sep=separator_pedidos,
            )
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pedidos: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        logger.info("Avaliando Fraude")
        try:
            df_pagamentos_fraude = self.transformer.avaliacao_fraude(df_pagamentos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de analise de fraude: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        df_pagamentos_fraude.show(5, truncate=False)

        logger.info("Adicionando no relatório o valor total do pedido")
        try:
            df_valor_total = self.transformer.add_valor_total_pedidos(df_pedidos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados com total dos pedidos: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        df_valor_total.show(5, truncate=False)

        logger.info("Filtrando Pagamentos")
        try:
            df_filtra_pagamento = self.transformer.filtra_pagamentos(df_pagamentos_fraude)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pagamentos filtrados: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        df_filtra_pagamento.show(5, truncate=False)

        logger.info("Filtrando Pedidos")
        try:
            df_filtra_pedidos = self.transformer.filtra_pedidos(df_pedidos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pedidos filtrados: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        df_filtra_pedidos.show(5, truncate=False)

        logger.info("Fazendo a junção dos dataframes")
        try:
            resultado_final_df = self.transformer.join_pagamentos_pedidos(df_filtra_pagamento, df_filtra_pedidos)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados finais: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        resultado_final_df.show(20, truncate=False)

        logger.info("Ordena Resultado")
        try:
            ordena_df = self.transformer.ordenar(resultado_final_df)
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de resultado: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        ordena_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        try:
            path_output = config["paths"]["output"]
            self.data_handler.write_parquet(df=resultado_final_df, path=path_output)
        except Exception as e:
            logger.error(f"Problemas ao escrever o arquivo em parquet: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

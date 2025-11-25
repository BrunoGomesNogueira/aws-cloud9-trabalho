# src/main.py
from core.config import load_settings
from core.spark_session import SparkSessionManager
from core.logger import setup_logging, get_logger
from pipeline import Pipeline

# Obter logger para este módulo
logger = get_logger(__name__)


def main():
    """
    Função principal que atua como a "Raiz de Composição".
    Configura e executa o pipeline.
    """

    config = load_settings()
    app_name = config.spark.app_name

    # 1. Inicialização da sessão Spark
    spark = None
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)

        # 2. Injeção de Dependência e Execução
        # A sessão Spark é "injetada" na criação do pipeline
        pipeline = Pipeline(spark)
        pipeline.run(config=config)
    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado na execução do programa: {e}")
    finally:
        if spark:
            # 3. Finalização
            spark.stop()
            logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    setup_logging()
    main()

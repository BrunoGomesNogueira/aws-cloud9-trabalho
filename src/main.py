# src/main.py
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline
import logging

logger = logging.getLogger(__name__)


# Crie a configuração do logging
def configurar_logging():
    """Configura o logging para todo o projeto."""
    logging.basicConfig(
        # Nível mínimo de severidade para ser registrado.
        # DEBUG < INFO < WARNING < ERROR < CRITICAL
        level=logging.INFO,
        # Formato da mensagem de log.
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # Lista de handlers. Aqui, estamos logando para um arquivo e para o console.
        handlers=[
            logging.FileHandler("dataeng-pyspark-poo.log"),  # Log para arquivo
            logging.StreamHandler(),  # Log para o console (terminal)
        ],
    )
    logging.info("Logging configurado.")


def main():
    """
    Função principal que atua como a "Raiz de Composição".
    Configura e executa o pipeline.
    """

    config = carregar_config()
    app_name = config["spark"]["app_name"]

    # 1. Inicialização da sessão Spark
    spark = None
    try:
        # 1. Inicialização da sessão Spark
        spark = SparkSessionManager.get_spark_session(app_name=app_name)

        # 2. Injeção de Dependência e Execução
        # A sessão Spark é "injetada" na criação do pipeline
        pipeline = Pipeline(spark)
        pipeline.run(config=config)
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado na execução do programa: {e}")
    finally:
        if spark:
            # 3. Finalização
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    configurar_logging()
    main()

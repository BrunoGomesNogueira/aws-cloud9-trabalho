import yaml
from pydantic import BaseModel


class SparkConfig(BaseModel):
    """Configurações do Spark."""

    app_name: str


class PathsConfig(BaseModel):
    """Configurações de caminhos."""

    pagamentos: str
    pedidos: str
    output: str


class PedidosCsvConfig(BaseModel):
    """Configurações específicas para leitura de CSVs de pedidos."""

    compression: str
    header: bool
    sep: str


class FileOptionsConfig(BaseModel):
    """Opções de arquivo."""

    pedidos_csv: PedidosCsvConfig


class Settings(BaseModel):
    """Modelo completo de configurações do projeto."""

    spark: SparkConfig
    paths: PathsConfig
    file_options: FileOptionsConfig

    @classmethod
    def from_yaml(cls, yaml_path: str = "config/settings.yaml") -> "Settings":
        """
        Carrega configurações de um arquivo YAML com validação Pydantic.

        Args:
            yaml_path: Caminho para o arquivo YAML de configuração.

        Returns:
            Instância de Settings com dados validados.

        Raises:
            ValidationError: Se os dados não passarem na validação.
        """
        with open(yaml_path, "r") as file:
            config_data = yaml.safe_load(file)
        return cls(**config_data)


def load_settings(path: str = "config/settings.yaml") -> Settings:
    """
    Carrega um arquivo de configuração YAML e retorna objeto Settings tipado.

    Args:
        path: Caminho para o arquivo YAML de configuração.

    Returns:
        Objeto Settings com dados validados e tipados.

    Example:
        >>> config = load_settings()
        >>> print(config.spark.app_name)
        >>> print(config.paths.pedidos)
    """
    return Settings.from_yaml(path)

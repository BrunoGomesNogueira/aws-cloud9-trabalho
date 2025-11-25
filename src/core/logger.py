"""
Módulo de configuração de logging para o projeto usando POO.

Este módulo fornece uma classe Logger que encapsula a configuração
e uso de logging de forma orientada a objetos.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List
from enum import IntEnum


class LogLevel(IntEnum):
    """
    Enumeração dos níveis de log disponíveis.

    Herda de IntEnum para compatibilidade com logging.DEBUG, etc.
    """

    DEBUG = logging.DEBUG  # 10
    INFO = logging.INFO  # 20
    WARNING = logging.WARNING  # 30
    ERROR = logging.ERROR  # 40
    CRITICAL = logging.CRITICAL  # 50


class LoggerConfig:
    """
    Classe de configuração para o Logger.

    Attributes:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Caminho para arquivo de log
        format_string: Formato da mensagem de log
        date_format: Formato da data/hora
        console_output: Se deve mostrar logs no console
    """

    def __init__(
        self,
        level: LogLevel = LogLevel.INFO,
        log_file: Optional[str] = "dataeng-pyspark-poo.log",
        format_string: Optional[str] = None,
        date_format: str = "%Y-%m-%d %H:%M:%S",
        console_output: bool = True,
    ):
        self.level = level
        self.log_file = log_file
        self.format_string = format_string or self._default_format()
        self.date_format = date_format
        self.console_output = console_output

    @staticmethod
    def _default_format() -> str:
        """Retorna o formato padrão das mensagens de log."""
        return "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class LoggerManager:
    """
    Gerenciador de logging usando POO.

    Esta classe encapsula toda a lógica de configuração e gerenciamento
    do sistema de logging do projeto.
    """

    _instance: Optional["LoggerManager"] = None
    _configured: bool = False

    def __new__(cls):
        """Implementa o padrão Singleton para garantir uma única instância."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Inicializa o gerenciador de logging."""
        self._loggers: dict[str, logging.Logger] = {}
        self._config: Optional[LoggerConfig] = None
        self._handlers: List[logging.Handler] = []

    def setup(self, config: Optional[LoggerConfig] = None) -> None:
        """
        Configura o sistema de logging.

        Args:
            config: Objeto de configuração. Se None, usa configuração padrão.

        Example:
            >>> manager = LoggerManager()
            >>> config = LoggerConfig(level=LogLevel.DEBUG)
            >>> manager.setup(config)
        """
        if config is None:
            config = LoggerConfig()

        self._config = config
        self._handlers.clear()

        # Criar e configurar handlers
        self._setup_handlers()

        # Configurar logger raiz
        root_logger = logging.getLogger()
        root_logger.setLevel(config.level.value)

        # Remover handlers antigos
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Adicionar novos handlers
        for handler in self._handlers:
            root_logger.addHandler(handler)

        self._configured = True

        # Log inicial
        logger = self.get_logger(__name__)
        logger.info("Sistema de logging configurado com sucesso")
        if config.log_file:
            logger.info(f"Logs sendo salvos em: {config.log_file}")

    def _setup_handlers(self) -> None:
        """Configura os handlers de log (console e arquivo)."""
        formatter = logging.Formatter(self._config.format_string, datefmt=self._config.date_format)

        # Handler para console
        if self._config.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self._config.level.value)
            console_handler.setFormatter(formatter)
            self._handlers.append(console_handler)

        # Handler para arquivo
        if self._config.log_file:
            self._create_log_file_handler(formatter)

    def _create_log_file_handler(self, formatter: logging.Formatter) -> None:
        """
        Cria e configura o handler para arquivo de log.

        Args:
            formatter: Formatador a ser usado pelo handler.
        """
        log_path = Path(self._config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(self._config.log_file, encoding="utf-8")
        file_handler.setLevel(self._config.level.value)
        file_handler.setFormatter(formatter)
        self._handlers.append(file_handler)

    def get_logger(self, name: str) -> logging.Logger:
        """
        Obtém um logger para o módulo especificado.

        Args:
            name: Nome do módulo (geralmente __name__)

        Returns:
            Logger configurado para o módulo.

        Example:
            >>> manager = LoggerManager()
            >>> logger = manager.get_logger(__name__)
            >>> logger.info("Mensagem de log")
        """
        if not self._configured:
            self.setup()

        if name not in self._loggers:
            self._loggers[name] = logging.getLogger(name)

        return self._loggers[name]

    def set_level(self, level: LogLevel) -> None:
        """
        Altera o nível de log em tempo de execução.

        Args:
            level: Novo nível de log (LogLevel.DEBUG)

        Example:
            >>> manager = LoggerManager()
            >>> manager.set_level(LogLevel.DEBUG)
        """
        logging.getLogger().setLevel(level.value)
        for handler in self._handlers:
            handler.setLevel(level.value)

        logger = self.get_logger(__name__)
        logger.info(f"Nível de log alterado para: {level.name}")

    def add_file_handler(self, file_path: str) -> None:
        """
        Adiciona um novo handler de arquivo ao sistema de logging.

        Args:
            file_path: Caminho para o novo arquivo de log

        Example:
            >>> manager = LoggerManager()
            >>> manager.add_file_handler("logs/erros.log")
        """
        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(self._config.format_string, datefmt=self._config.date_format)

        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setLevel(self._config.level.value)
        file_handler.setFormatter(formatter)

        logging.getLogger().addHandler(file_handler)
        self._handlers.append(file_handler)

    @property
    def is_configured(self) -> bool:
        """Verifica se o sistema de logging já foi configurado."""
        return self._configured


# Instância global do gerenciador (Singleton)
_logger_manager = LoggerManager()


def setup_logging(config: Optional[LoggerConfig] = None) -> None:
    """
    Função de conveniência para configurar o logging.

    Args:
        config: Objeto de configuração. Se None, usa configuração padrão.

    Example:
        >>> from logger import setup_logging, LoggerConfig, LogLevel
        >>> config = LoggerConfig(level=LogLevel.DEBUG)
        >>> setup_logging(config)
    """
    _logger_manager.setup(config)


def get_logger(name: str) -> logging.Logger:
    """
    Função de conveniência para obter um logger.

    Args:
        name: Nome do módulo (geralmente __name__)

    Returns:
        Logger configurado para o módulo.

    Example:
        >>> from logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Mensagem de log")
    """
    return _logger_manager.get_logger(name)


def set_log_level(level: LogLevel) -> None:
    """
    Função de conveniência para alterar o nível de log.

    Args:
        level: Novo nível de log

    Example:
        >>> from logger import set_log_level, LogLevel
        >>> set_log_level(LogLevel.DEBUG)
    """
    _logger_manager.set_level(level)

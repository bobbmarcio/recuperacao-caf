"""
Configurações do projeto para análise de dumps PostgreSQL
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


@dataclass
class TableConfig:
    """Configuração para monitoramento de uma tabela"""
    primary_key: str
    columns: List[str]
    
    def __post_init__(self):
        if not self.primary_key:
            raise ValueError("primary_key é obrigatório")
        if not self.columns:
            raise ValueError("columns não pode estar vazio")


@dataclass
class MonitoringConfig:
    """Configuração completa de monitoramento"""
    tables: Dict[str, TableConfig] = field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, config_path: str) -> 'MonitoringConfig':
        """Carrega configuração de um arquivo YAML"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            tables = {}
            for table_name, table_data in data.get('tables', {}).items():
                tables[table_name] = TableConfig(
                    primary_key=table_data['primary_key'],
                    columns=table_data['columns']
                )
            
            return cls(tables=tables)
        except Exception as e:
            logger.error(f"Erro ao carregar configuração: {e}")
            raise


@dataclass
class DatabaseConfig:
    """Configuração de conexão com bancos de dados"""
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = ""
    postgres_database: str = "postgres"
    
    mongodb_connection_string: str = "mongodb://localhost:27017/"
    mongodb_database: str = "audit_db"
    mongodb_collection: str = "data_changes"
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Carrega configuração das variáveis de ambiente"""
        return cls(
            postgres_host=os.getenv('POSTGRES_HOST', 'localhost'),
            postgres_port=int(os.getenv('POSTGRES_PORT', '5432')),
            postgres_user=os.getenv('POSTGRES_USER', 'postgres'),
            postgres_password=os.getenv('POSTGRES_PASSWORD', ''),
            postgres_database=os.getenv('POSTGRES_DATABASE', 'postgres'),
            
            mongodb_connection_string=os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017/'),
            mongodb_database=os.getenv('MONGODB_DATABASE', 'audit_db'),
            mongodb_collection=os.getenv('MONGODB_COLLECTION', 'data_changes')
        )


@dataclass
class AppConfig:
    """Configuração geral da aplicação"""
    dump_directory: str = "./dumps"
    log_level: str = "INFO"
    debug: bool = False
    
    def __post_init__(self):
        # Criar diretório de dumps se não existir
        Path(self.dump_directory).mkdir(parents=True, exist_ok=True)


def setup_logging(log_level: str = "INFO", debug: bool = False) -> None:
    """Configura o sistema de logging"""
    logger.remove()  # Remove configuração padrão
    
    # Log para arquivo
    logger.add(
        "logs/app_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        compression="zip"
    )
    
    # Log para console
    if debug:
        logger.add(
            lambda msg: print(msg, end=""),
            level="DEBUG",
            format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | {message}"
        )
    else:
        logger.add(
            lambda msg: print(msg, end=""),
            level=log_level,
            format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
        )

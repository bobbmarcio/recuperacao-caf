"""
Parser para dumps PostgreSQL
"""

import re
import os
import pandas as pd
import sqlparse
from typing import Dict, List, Optional, Any, Iterator
from dataclasses import dataclass
from pathlib import Path
from loguru import logger


@dataclass
class TableData:
    """Dados de uma tabela extraída do dump"""
    name: str
    columns: List[str]
    data: pd.DataFrame
    primary_key: str


@dataclass
class DumpInfo:
    """Informações sobre um dump PostgreSQL"""
    file_path: str
    timestamp: Optional[str] = None
    database_name: Optional[str] = None
    pg_version: Optional[str] = None


class PostgreSQLDumpParser:
    """Parser para arquivos de dump PostgreSQL"""
    
    def __init__(self):
        self.dump_info: Optional[DumpInfo] = None
        
    def parse_dump_file(self, dump_path: str, tables_to_monitor: List[str]) -> Dict[str, TableData]:
        """
        Analisa um arquivo de dump PostgreSQL e extrai dados das tabelas especificadas
        
        Args:
            dump_path: Caminho para o arquivo de dump
            tables_to_monitor: Lista de nomes de tabelas para extrair
            
        Returns:
            Dicionário com dados das tabelas {nome_tabela: TableData}
        """
        logger.info(f"Iniciando parse do dump: {dump_path}")
        
        if not os.path.exists(dump_path):
            raise FileNotFoundError(f"Arquivo de dump não encontrado: {dump_path}")
        
        self.dump_info = DumpInfo(file_path=dump_path)
        tables_data = {}
        
        try:
            with open(dump_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Extrair informações do cabeçalho
            self._extract_dump_info(content)
            
            # Processar cada tabela monitorada
            for table_name in tables_to_monitor:
                logger.debug(f"Processando tabela: {table_name}")
                table_data = self._extract_table_data(content, table_name)
                
                if table_data:
                    tables_data[table_name] = table_data
                    logger.info(f"Tabela {table_name}: {len(table_data.data)} registros extraídos")
                else:
                    logger.warning(f"Tabela {table_name} não encontrada no dump")
                    
        except Exception as e:
            logger.error(f"Erro ao processar dump {dump_path}: {e}")
            raise
            
        logger.info(f"Parse concluído. {len(tables_data)} tabelas processadas")
        return tables_data
    
    def _extract_dump_info(self, content: str) -> None:
        """Extrai informações do cabeçalho do dump"""
        try:
            # Extrair timestamp
            timestamp_match = re.search(r'-- Dumped on (.+)', content)
            if timestamp_match:
                self.dump_info.timestamp = timestamp_match.group(1).strip()
            
            # Extrair nome do banco
            db_match = re.search(r'-- Database: (.+)', content)
            if db_match:
                self.dump_info.database_name = db_match.group(1).strip()
            
            # Extrair versão do PostgreSQL
            version_match = re.search(r'-- PostgreSQL database dump complete.*?PostgreSQL (.+)', content, re.DOTALL)
            if version_match:
                self.dump_info.pg_version = version_match.group(1).strip()
                
        except Exception as e:
            logger.warning(f"Erro ao extrair informações do dump: {e}")
    
    def _extract_table_data(self, content: str, table_name: str) -> Optional[TableData]:
        """
        Extrai dados de uma tabela específica do dump
        
        Args:
            content: Conteúdo completo do dump
            table_name: Nome da tabela a extrair
            
        Returns:
            TableData ou None se a tabela não for encontrada
        """
        try:
            # Padrão para encontrar a definição da tabela
            table_pattern = rf'CREATE TABLE (?:public\.)?{re.escape(table_name)}\s*\((.*?)\);'
            table_match = re.search(table_pattern, content, re.DOTALL | re.IGNORECASE)
            
            if not table_match:
                logger.debug(f"Definição da tabela {table_name} não encontrada")
                return None
            
            # Extrair colunas da definição
            columns = self._parse_table_columns(table_match.group(1))
            primary_key = self._find_primary_key(table_match.group(1))
            
            # Padrão para encontrar os dados da tabela
            copy_pattern = rf'COPY (?:public\.)?{re.escape(table_name)}\s*\([^)]+\) FROM stdin;(.*?)\\\.'
            copy_match = re.search(copy_pattern, content, re.DOTALL | re.IGNORECASE)
            
            if not copy_match:
                logger.debug(f"Dados da tabela {table_name} não encontrados")
                # Retorna tabela vazia se não há dados
                return TableData(
                    name=table_name,
                    columns=columns,
                    data=pd.DataFrame(columns=columns),
                    primary_key=primary_key or 'id'
                )
            
            # Processar dados
            data_lines = copy_match.group(1).strip().split('\n')
            data_rows = []
            
            for line in data_lines:
                if line.strip():
                    # Split por tab e remover caracteres de escape
                    row = [self._clean_field(field) for field in line.split('\t')]
                    data_rows.append(row)
            
            # Criar DataFrame
            df = pd.DataFrame(data_rows, columns=columns)
            
            return TableData(
                name=table_name,
                columns=columns,
                data=df,
                primary_key=primary_key or 'id'
            )
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da tabela {table_name}: {e}")
            return None
    
    def _parse_table_columns(self, table_definition: str) -> List[str]:
        """Extrai nomes das colunas da definição da tabela"""
        columns = []
        lines = table_definition.split('\n')
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith('CONSTRAINT') and not line.startswith('PRIMARY KEY'):
                # Extrair nome da coluna (primeira palavra)
                parts = line.split()
                if parts:
                    column_name = parts[0].strip('",')
                    if column_name and not column_name.upper() in ['PRIMARY', 'FOREIGN', 'CHECK', 'UNIQUE']:
                        columns.append(column_name)
        
        return columns
    
    def _find_primary_key(self, table_definition: str) -> Optional[str]:
        """Encontra a chave primária da tabela"""
        # Buscar PRIMARY KEY constraint
        pk_match = re.search(r'PRIMARY KEY\s*\(\s*([^)]+)\s*\)', table_definition, re.IGNORECASE)
        if pk_match:
            return pk_match.group(1).strip('"')
        
        # Buscar coluna com PRIMARY KEY inline
        lines = table_definition.split('\n')
        for line in lines:
            if 'PRIMARY KEY' in line.upper():
                parts = line.strip().split()
                if parts:
                    return parts[0].strip('",')
        
        return None
    
    def _clean_field(self, field: str) -> str:
        """Remove caracteres de escape dos campos"""
        if field == '\\N':
            return None
        return field.replace('\\t', '\t').replace('\\n', '\n').replace('\\r', '\r')
    
    def get_dump_info(self) -> Optional[DumpInfo]:
        """Retorna informações do último dump processado"""
        return self.dump_info


def list_dump_files(dump_directory: str) -> List[str]:
    """
    Lista arquivos de dump no diretório especificado
    
    Args:
        dump_directory: Diretório com arquivos de dump
        
    Returns:
        Lista de caminhos para arquivos de dump ordenados por nome
    """
    dump_dir = Path(dump_directory)
    
    if not dump_dir.exists():
        logger.error(f"Diretório de dumps não existe: {dump_directory}")
        return []
    
    # Buscar arquivos .sql e .dump
    dump_files = []
    for pattern in ['*.sql', '*.dump']:
        dump_files.extend(dump_dir.glob(pattern))
    
    # Ordenar por nome
    dump_files.sort(key=lambda x: x.name)
    
    logger.info(f"Encontrados {len(dump_files)} arquivos de dump em {dump_directory}")
    return [str(f) for f in dump_files]

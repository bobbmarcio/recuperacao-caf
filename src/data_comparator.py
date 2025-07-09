"""
Comparador de dados para análise incremental
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from loguru import logger

from dump_parser import TableData
from config import TableConfig


@dataclass
class DataChange:
    """Representa uma alteração detectada nos dados"""
    table_name: str
    primary_key_value: Any
    column_name: str
    old_value: Any
    new_value: Any
    timestamp: datetime
    dump_source: str
    dump_target: str


@dataclass
class ComparisonResult:
    """Resultado da comparação entre dois dumps"""
    changes: List[DataChange]
    tables_compared: List[str]
    total_records_compared: int
    comparison_timestamp: datetime


class DataComparator:
    """Compara dados entre dumps PostgreSQL para detectar alterações"""
    
    def __init__(self):
        self.last_comparison: Optional[ComparisonResult] = None
    
    def compare_dumps(
        self,
        previous_data: Dict[str, TableData],
        current_data: Dict[str, TableData],
        monitoring_config: Dict[str, TableConfig],
        previous_dump_name: str,
        current_dump_name: str
    ) -> ComparisonResult:
        """
        Compara dados entre dois dumps e identifica alterações
        
        Args:
            previous_data: Dados do dump anterior
            current_data: Dados do dump atual
            monitoring_config: Configuração de colunas a monitorar
            previous_dump_name: Nome do dump anterior
            current_dump_name: Nome do dump atual
            
        Returns:
            ComparisonResult com as alterações detectadas
        """
        logger.info(f"Iniciando comparação entre {previous_dump_name} e {current_dump_name}")
        
        changes = []
        tables_compared = []
        total_records = 0
        
        # Iterar sobre tabelas comuns aos dois dumps
        common_tables = set(previous_data.keys()) & set(current_data.keys())
        
        for table_name in common_tables:
            if table_name not in monitoring_config:
                logger.debug(f"Tabela {table_name} não está na configuração de monitoramento")
                continue
            
            logger.info(f"Comparando tabela: {table_name}")
            
            table_config = monitoring_config[table_name]
            prev_table = previous_data[table_name]
            curr_table = current_data[table_name]
            
            table_changes = self._compare_table_data(
                prev_table, curr_table, table_config,
                previous_dump_name, current_dump_name
            )
            
            changes.extend(table_changes)
            tables_compared.append(table_name)
            total_records += len(curr_table.data)
            
            logger.info(f"Tabela {table_name}: {len(table_changes)} alterações detectadas")
        
        result = ComparisonResult(
            changes=changes,
            tables_compared=tables_compared,
            total_records_compared=total_records,
            comparison_timestamp=datetime.now()
        )
        
        self.last_comparison = result
        logger.info(f"Comparação concluída: {len(changes)} alterações em {len(tables_compared)} tabelas")
        
        return result
    
    def _compare_table_data(
        self,
        prev_table: TableData,
        curr_table: TableData,
        table_config: TableConfig,
        previous_dump_name: str,
        current_dump_name: str
    ) -> List[DataChange]:
        """
        Compara dados de uma tabela específica
        
        Args:
            prev_table: Dados da tabela no dump anterior
            curr_table: Dados da tabela no dump atual
            table_config: Configuração de monitoramento da tabela
            previous_dump_name: Nome do dump anterior
            current_dump_name: Nome do dump atual
            
        Returns:
            Lista de alterações detectadas
        """
        changes = []
        timestamp = datetime.now()
        
        # Verificar se a chave primária existe em ambas as tabelas
        pk_column = table_config.primary_key
        if pk_column not in prev_table.columns or pk_column not in curr_table.columns:
            logger.warning(f"Chave primária {pk_column} não encontrada na tabela {prev_table.name}")
            return changes
        
        # Verificar se as colunas monitoradas existem
        valid_columns = []
        for col in table_config.columns:
            if col in prev_table.columns and col in curr_table.columns:
                valid_columns.append(col)
            else:
                logger.warning(f"Coluna {col} não encontrada na tabela {prev_table.name}")
        
        if not valid_columns:
            logger.warning(f"Nenhuma coluna válida para monitorar na tabela {prev_table.name}")
            return changes
        
        # Criar índices baseados na chave primária
        prev_df = prev_table.data.set_index(pk_column)
        curr_df = curr_table.data.set_index(pk_column)
        
        # Encontrar chaves primárias comuns
        common_pks = prev_df.index.intersection(curr_df.index)
        
        logger.debug(f"Comparando {len(common_pks)} registros comuns na tabela {prev_table.name}")
        
        # Comparar valores das colunas monitoradas
        for pk_value in common_pks:
            prev_row = prev_df.loc[pk_value]
            curr_row = curr_df.loc[pk_value]
            
            for column in valid_columns:
                prev_value = prev_row[column] if column in prev_row else None
                curr_value = curr_row[column] if column in curr_row else None
                
                # Normalizar valores para comparação
                prev_normalized = self._normalize_value(prev_value)
                curr_normalized = self._normalize_value(curr_value)
                
                if prev_normalized != curr_normalized:
                    change = DataChange(
                        table_name=prev_table.name,
                        primary_key_value=pk_value,
                        column_name=column,
                        old_value=prev_value,
                        new_value=curr_value,
                        timestamp=timestamp,
                        dump_source=previous_dump_name,
                        dump_target=current_dump_name
                    )
                    changes.append(change)
                    
                    logger.debug(f"Alteração detectada: {prev_table.name}.{column} [{pk_value}]: '{prev_value}' -> '{curr_value}'")
        
        return changes
    
    def _normalize_value(self, value: Any) -> Any:
        """
        Normaliza valores para comparação
        
        Args:
            value: Valor a ser normalizado
            
        Returns:
            Valor normalizado
        """
        if pd.isna(value) or value is None:
            return None
        
        if isinstance(value, str):
            # Remove espaços extras e converte para lowercase para comparação
            return value.strip().lower() if value.strip() else None
        
        return value
    
    def get_changes_by_table(self, table_name: str) -> List[DataChange]:
        """
        Retorna alterações detectadas para uma tabela específica
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista de alterações da tabela
        """
        if not self.last_comparison:
            return []
        
        return [change for change in self.last_comparison.changes 
                if change.table_name == table_name]
    
    def get_changes_by_column(self, table_name: str, column_name: str) -> List[DataChange]:
        """
        Retorna alterações detectadas para uma coluna específica
        
        Args:
            table_name: Nome da tabela
            column_name: Nome da coluna
            
        Returns:
            Lista de alterações da coluna
        """
        if not self.last_comparison:
            return []
        
        return [change for change in self.last_comparison.changes 
                if change.table_name == table_name and change.column_name == column_name]
    
    def generate_summary_report(self) -> str:
        """
        Gera um relatório resumido da última comparação
        
        Returns:
            String com o relatório
        """
        if not self.last_comparison:
            return "Nenhuma comparação realizada ainda."
        
        result = self.last_comparison
        report = []
        
        report.append("=" * 50)
        report.append("RELATÓRIO DE COMPARAÇÃO INCREMENTAL")
        report.append("=" * 50)
        report.append(f"Timestamp: {result.comparison_timestamp}")
        report.append(f"Tabelas comparadas: {len(result.tables_compared)}")
        report.append(f"Total de registros: {result.total_records_compared}")
        report.append(f"Total de alterações: {len(result.changes)}")
        report.append("")
        
        # Resumo por tabela
        table_summary = {}
        for change in result.changes:
            if change.table_name not in table_summary:
                table_summary[change.table_name] = 0
            table_summary[change.table_name] += 1
        
        if table_summary:
            report.append("ALTERAÇÕES POR TABELA:")
            report.append("-" * 30)
            for table, count in table_summary.items():
                report.append(f"  {table}: {count} alterações")
            report.append("")
        
        # Resumo por coluna
        column_summary = {}
        for change in result.changes:
            key = f"{change.table_name}.{change.column_name}"
            if key not in column_summary:
                column_summary[key] = 0
            column_summary[key] += 1
        
        if column_summary:
            report.append("ALTERAÇÕES POR COLUNA:")
            report.append("-" * 30)
            for column, count in sorted(column_summary.items()):
                report.append(f"  {column}: {count} alterações")
        
        return "\n".join(report)

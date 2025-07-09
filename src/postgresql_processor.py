"""
Estratégia de processamento de dumps PostgreSQL grandes
Usa bancos temporários para performance otimizada
"""

import os
import subprocess
import tempfile
import psycopg2
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from loguru import logger
from pathlib import Path

from config import DatabaseConfig, TableConfig
from data_comparator import DataChange


@dataclass
class DatabaseInstance:
    """Representa uma instância de banco de dados temporária"""
    name: str
    host: str
    port: int
    user: str
    password: str
    dump_file: str
    connection: Optional[psycopg2.extensions.connection] = None


class PostgreSQLDumpProcessor:
    """
    Processa dumps PostgreSQL grandes usando bancos temporários
    """
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.temp_databases: Dict[str, DatabaseInstance] = {}
    
    def process_large_dumps(
        self,
        dump_files: List[str],
        monitoring_config: Dict[str, TableConfig]
    ) -> List[DataChange]:
        """
        Processa dumps grandes usando estratégia de bancos temporários
        
        Args:
            dump_files: Lista de arquivos de dump ordenados cronologicamente
            monitoring_config: Configuração das tabelas a monitorar
            
        Returns:
            Lista de alterações detectadas
        """
        try:
            logger.info(f"Iniciando processamento de {len(dump_files)} dumps grandes")
            
            # Criar bancos temporários para cada dump
            db_instances = []
            for i, dump_file in enumerate(dump_files):
                db_name = f"temp_dump_{i}_{int(datetime.now().timestamp())}"
                db_instance = self._create_temp_database(db_name, dump_file)
                db_instances.append(db_instance)
            
            # Comparar dumps em pares
            all_changes = []
            for i in range(len(db_instances) - 1):
                prev_db = db_instances[i]
                curr_db = db_instances[i + 1]
                
                logger.info(f"Comparando {Path(prev_db.dump_file).name} → {Path(curr_db.dump_file).name}")
                
                changes = self._compare_databases(
                    prev_db, curr_db, monitoring_config,
                    Path(prev_db.dump_file).name,
                    Path(curr_db.dump_file).name
                )
                all_changes.extend(changes)
            
            logger.info(f"Processamento concluído: {len(all_changes)} alterações detectadas")
            return all_changes
            
        finally:
            # Limpar bancos temporários
            self._cleanup_temp_databases()
    
    def _create_temp_database(self, db_name: str, dump_file: str) -> DatabaseInstance:
        """
        Cria um banco temporário e restaura o dump
        
        Args:
            db_name: Nome do banco temporário
            dump_file: Caminho para o arquivo de dump
            
        Returns:
            Instância do banco criado
        """
        logger.info(f"Criando banco temporário: {db_name}")
        
        db_instance = DatabaseInstance(
            name=db_name,
            host=self.db_config.postgres_host,
            port=self.db_config.postgres_port,
            user=self.db_config.postgres_user,
            password=self.db_config.postgres_password,
            dump_file=dump_file
        )
        
        try:
            # Conectar ao PostgreSQL para criar o banco
            admin_conn = psycopg2.connect(
                host=self.db_config.postgres_host,
                port=self.db_config.postgres_port,
                user=self.db_config.postgres_user,
                password=self.db_config.postgres_password,
                database='postgres'  # Conectar ao banco padrão
            )
            admin_conn.autocommit = True
            
            with admin_conn.cursor() as cursor:
                # Criar banco temporário
                cursor.execute(f'CREATE DATABASE "{db_name}"')
                logger.info(f"Banco {db_name} criado com sucesso")
            
            admin_conn.close()
            
            # Restaurar dump no banco temporário
            self._restore_dump(db_instance, dump_file)
            
            # Conectar ao banco temporário
            db_instance.connection = psycopg2.connect(
                host=db_instance.host,
                port=db_instance.port,
                user=db_instance.user,
                password=db_instance.password,
                database=db_instance.name
            )
            
            self.temp_databases[db_name] = db_instance
            return db_instance
            
        except Exception as e:
            logger.error(f"Erro ao criar banco temporário {db_name}: {e}")
            # Tentar limpar se houve erro parcial
            self._drop_database(db_name)
            raise
    
    def _restore_dump(self, db_instance: DatabaseInstance, dump_file: str) -> None:
        """
        Restaura um dump SQL no banco temporário
        
        Args:
            db_instance: Instância do banco
            dump_file: Caminho para o arquivo de dump
        """
        logger.info(f"Restaurando dump {Path(dump_file).name} no banco {db_instance.name}")
        
        # Usar pg_restore ou psql dependendo do formato
        if dump_file.endswith('.sql'):
            # Arquivo SQL texto
            cmd = [
                'psql',
                '-h', db_instance.host,
                '-p', str(db_instance.port),
                '-U', db_instance.user,
                '-d', db_instance.name,
                '-f', dump_file,
                '-q'  # Modo silencioso
            ]
        else:
            # Arquivo binário
            cmd = [
                'pg_restore',
                '-h', db_instance.host,
                '-p', str(db_instance.port),
                '-U', db_instance.user,
                '-d', db_instance.name,
                '-v',  # Verbose
                dump_file
            ]
        
        # Configurar variáveis de ambiente
        env = os.environ.copy()
        env['PGPASSWORD'] = db_instance.password
        
        try:
            # Executar restauração
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600  # Timeout de 1 hora
            )
            
            if result.returncode != 0:
                logger.error(f"Erro na restauração: {result.stderr}")
                raise Exception(f"Falha na restauração do dump: {result.stderr}")
            
            logger.info(f"Dump restaurado com sucesso no banco {db_instance.name}")
            
        except subprocess.TimeoutExpired:
            logger.error("Timeout na restauração do dump")
            raise Exception("Timeout na restauração do dump")
    
    def _compare_databases(
        self,
        prev_db: DatabaseInstance,
        curr_db: DatabaseInstance,
        monitoring_config: Dict[str, TableConfig],
        prev_dump_name: str,
        curr_dump_name: str
    ) -> List[DataChange]:
        """
        Compara duas instâncias de banco usando SQL otimizado
        
        Args:
            prev_db: Banco com dados anteriores
            curr_db: Banco com dados atuais  
            monitoring_config: Configuração de monitoramento
            prev_dump_name: Nome do dump anterior
            curr_dump_name: Nome do dump atual
            
        Returns:
            Lista de alterações detectadas
        """
        all_changes = []
        comparison_time = datetime.now()
        
        for table_name, table_config in monitoring_config.items():
            logger.info(f"Comparando tabela: {table_name}")
            
            try:
                changes = self._compare_table_sql(
                    prev_db, curr_db, table_config,
                    prev_dump_name, curr_dump_name, comparison_time
                )
                all_changes.extend(changes)
                
                logger.info(f"Tabela {table_name}: {len(changes)} alterações detectadas")
                
            except Exception as e:
                logger.error(f"Erro ao comparar tabela {table_name}: {e}")
        
        return all_changes
    
    def _compare_table_sql(
        self,
        prev_db: DatabaseInstance,
        curr_db: DatabaseInstance,
        table_config: TableConfig,
        prev_dump_name: str,
        curr_dump_name: str,
        comparison_time: datetime
    ) -> List[DataChange]:
        """
        Compara uma tabela específica usando SQL com dblink
        
        Args:
            prev_db: Banco anterior
            curr_db: Banco atual
            table_config: Configuração da tabela
            prev_dump_name: Nome do dump anterior
            curr_dump_name: Nome do dump atual
            comparison_time: Timestamp da comparação
            
        Returns:
            Lista de alterações detectadas
        """
        changes = []
        table_name = table_config.name
        pk_column = table_config.primary_key
        monitored_columns = table_config.columns
        
        # Query SQL para detectar alterações
        sql_query = f"""
        WITH prev_data AS (
            SELECT {pk_column}, {', '.join(monitored_columns)}
            FROM {table_name}
        ),
        curr_data AS (
            SELECT {pk_column}, {', '.join(monitored_columns)}
            FROM {table_name}
        ),
        compared AS (
            SELECT 
                p.{pk_column} as pk_value,
                {self._build_column_comparison(monitored_columns, 'p', 'c')}
            FROM prev_data p
            INNER JOIN curr_data c ON p.{pk_column} = c.{pk_column}
            WHERE {self._build_where_different(monitored_columns, 'p', 'c')}
        )
        SELECT * FROM compared;
        """
        
        try:
            # Executar query no banco atual, comparando com o anterior
            with curr_db.connection.cursor() as cursor:
                # Criar conexão temporária para o banco anterior usando dblink
                # Por simplicidade, vamos usar uma abordagem diferente:
                # Buscar dados de ambos os bancos separadamente e comparar em Python
                
                # Dados do banco anterior
                prev_data = self._fetch_table_data(prev_db, table_name, pk_column, monitored_columns)
                
                # Dados do banco atual  
                curr_data = self._fetch_table_data(curr_db, table_name, pk_column, monitored_columns)
                
                # Comparar dados
                changes = self._compare_data_frames(
                    prev_data, curr_data, table_config,
                    prev_dump_name, curr_dump_name, comparison_time
                )
                
        except Exception as e:
            logger.error(f"Erro na query SQL para tabela {table_name}: {e}")
            
        return changes
    
    def _fetch_table_data(
        self,
        db_instance: DatabaseInstance,
        table_name: str,
        pk_column: str,
        columns: List[str]
    ) -> pd.DataFrame:
        """
        Busca dados de uma tabela como DataFrame
        
        Args:
            db_instance: Instância do banco
            table_name: Nome da tabela
            pk_column: Coluna de chave primária
            columns: Colunas a buscar
            
        Returns:
            DataFrame com os dados
        """
        all_columns = [pk_column] + [col for col in columns if col != pk_column]
        query = f"SELECT {', '.join(all_columns)} FROM {table_name} ORDER BY {pk_column}"
        
        return pd.read_sql_query(query, db_instance.connection)
    
    def _compare_data_frames(
        self,
        prev_df: pd.DataFrame,
        curr_df: pd.DataFrame,
        table_config: TableConfig,
        prev_dump_name: str,
        curr_dump_name: str,
        comparison_time: datetime
    ) -> List[DataChange]:
        """
        Compara dois DataFrames e detecta alterações
        
        Args:
            prev_df: DataFrame com dados anteriores
            curr_df: DataFrame com dados atuais
            table_config: Configuração da tabela
            prev_dump_name: Nome do dump anterior
            curr_dump_name: Nome do dump atual
            comparison_time: Timestamp da comparação
            
        Returns:
            Lista de alterações detectadas
        """
        changes = []
        pk_column = table_config.primary_key
        
        # Configurar índices
        prev_df = prev_df.set_index(pk_column)
        curr_df = curr_df.set_index(pk_column)
        
        # Encontrar chaves primárias comuns
        common_pks = prev_df.index.intersection(curr_df.index)
        
        for pk_value in common_pks:
            prev_row = prev_df.loc[pk_value]
            curr_row = curr_df.loc[pk_value]
            
            for column in table_config.columns:
                if column == pk_column:
                    continue
                    
                prev_value = prev_row[column] if column in prev_row else None
                curr_value = curr_row[column] if column in curr_row else None
                
                # Normalizar valores para comparação
                if pd.isna(prev_value):
                    prev_value = None
                if pd.isna(curr_value):
                    curr_value = None
                
                if prev_value != curr_value:
                    change = DataChange(
                        table_name=table_config.name,
                        primary_key_value=pk_value,
                        column_name=column,
                        old_value=prev_value,
                        new_value=curr_value,
                        timestamp=comparison_time,
                        dump_source=prev_dump_name,
                        dump_target=curr_dump_name
                    )
                    changes.append(change)
        
        return changes
    
    def _build_column_comparison(self, columns: List[str], alias1: str, alias2: str) -> str:
        """Constrói comparação de colunas para SQL"""
        comparisons = []
        for col in columns:
            comparisons.append(f"'{col}' as column_name, {alias1}.{col} as old_value, {alias2}.{col} as new_value")
        return ',\n        '.join(comparisons)
    
    def _build_where_different(self, columns: List[str], alias1: str, alias2: str) -> str:
        """Constrói condição WHERE para detectar diferenças"""
        conditions = []
        for col in columns:
            conditions.append(f"({alias1}.{col} IS DISTINCT FROM {alias2}.{col})")
        return ' OR '.join(conditions)
    
    def _drop_database(self, db_name: str) -> None:
        """
        Remove um banco temporário
        
        Args:
            db_name: Nome do banco a remover
        """
        try:
            admin_conn = psycopg2.connect(
                host=self.db_config.postgres_host,
                port=self.db_config.postgres_port,
                user=self.db_config.postgres_user,
                password=self.db_config.postgres_password,
                database='postgres'
            )
            admin_conn.autocommit = True
            
            with admin_conn.cursor() as cursor:
                # Encerrar conexões ativas
                cursor.execute(f"""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{db_name}' AND pid != pg_backend_pid()
                """)
                
                # Remover banco
                cursor.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
                logger.info(f"Banco temporário {db_name} removido")
            
            admin_conn.close()
            
        except Exception as e:
            logger.warning(f"Erro ao remover banco {db_name}: {e}")
    
    def _cleanup_temp_databases(self) -> None:
        """Remove todos os bancos temporários criados"""
        logger.info("Limpando bancos temporários...")
        
        for db_name, db_instance in self.temp_databases.items():
            try:
                if db_instance.connection:
                    db_instance.connection.close()
                    
                self._drop_database(db_name)
                
            except Exception as e:
                logger.warning(f"Erro na limpeza do banco {db_name}: {e}")
        
        self.temp_databases.clear()
        logger.info("Limpeza de bancos temporários concluída")
    
    def estimate_processing_time(self, dump_files: List[str]) -> dict:
        """
        Estima tempo de processamento baseado no tamanho dos arquivos
        
        Args:
            dump_files: Lista de arquivos de dump
            
        Returns:
            Dicionário com estimativas
        """
        total_size = 0
        file_info = []
        
        for dump_file in dump_files:
            size = Path(dump_file).stat().st_size
            total_size += size
            file_info.append({
                'file': Path(dump_file).name,
                'size_gb': size / (1024**3)
            })
        
        # Estimativas baseadas em experiência (ajustar conforme necessário)
        restore_time_per_gb = 120  # segundos por GB
        comparison_time_per_gb = 30  # segundos por GB
        
        estimated_restore = (total_size / (1024**3)) * restore_time_per_gb
        estimated_comparison = (total_size / (1024**3)) * comparison_time_per_gb
        
        return {
            'total_size_gb': total_size / (1024**3),
            'estimated_restore_time_minutes': estimated_restore / 60,
            'estimated_comparison_time_minutes': estimated_comparison / 60,
            'estimated_total_time_minutes': (estimated_restore + estimated_comparison) / 60,
            'files': file_info
        }

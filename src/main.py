"""
Análise incremental de dumps PostgreSQL com integração ao MongoDB
"""

import os
import sys
import click
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Adicionar src ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

from config import DatabaseConfig, MonitoringConfig, AppConfig, setup_logging
from dump_parser import PostgreSQLDumpParser
from data_comparator import DataComparator
from mongo_inserter import MongoAuditInserter
from postgresql_processor import PostgreSQLDumpProcessor


def list_dump_files(dump_dir: str) -> List[str]:
    """Lista arquivos de dump ordenados por data"""
    dump_path = Path(dump_dir)
    if not dump_path.exists():
        return []
    
    dump_files = []
    for ext in ['*.sql', '*.dump', '*.backup']:
        dump_files.extend(dump_path.glob(ext))
    
    # Ordenar por data de modificação
    return [str(f) for f in sorted(dump_files, key=lambda x: x.stat().st_mtime)]


def calculate_total_size(dump_files: List[str]) -> float:
    """Calcula tamanho total dos arquivos em GB"""
    total_size = sum(Path(f).stat().st_size for f in dump_files)
    return total_size / (1024**3)


class DumpAnalyzer:
    """Analisador principal de dumps"""
    
    def __init__(self, app_config: AppConfig, db_config: DatabaseConfig):
        self.app_config = app_config
        self.db_config = db_config
        self.dump_parser = PostgreSQLDumpParser()
        self.data_comparator = DataComparator()
        self.mongo_inserter = MongoAuditInserter(db_config)
        self.postgresql_processor = PostgreSQLDumpProcessor(db_config)
        
    def analyze_dumps(self, monitoring_config: MonitoringConfig) -> bool:
        """
        Executa análise incremental dos dumps
        
        Args:
            monitoring_config: Configuração de monitoramento
            
        Returns:
            True se análise foi bem-sucedida, False caso contrário
        """
        try:
            # Conectar ao MongoDB
            if not self.mongo_inserter.connect():
                click.echo("❌ Falha ao conectar com MongoDB", err=True)
                return False
            
            # Criar índices se necessário
            self.mongo_inserter.create_indexes()
            
            # Listar arquivos de dump
            dump_files = list_dump_files(self.app_config.dump_directory)
            
            if len(dump_files) < 2:
                click.echo("⚠️  Necessário pelo menos 2 dumps para comparação")
                return False
            
            # Verificar tamanho dos arquivos
            total_size_gb = calculate_total_size(dump_files)
            click.echo(f"📁 Encontrados {len(dump_files)} dumps para análise")
            click.echo(f"📊 Tamanho total: {total_size_gb:.2f} GB")
            
            # Escolher estratégia baseada no tamanho
            if total_size_gb > 10.0:  # Acima de 10GB, usar PostgreSQL (ajustado temporariamente)
                click.echo("🔄 Usando estratégia PostgreSQL para arquivos grandes...")
                return self._analyze_large_dumps(dump_files, monitoring_config)
            else:
                click.echo("🔄 Usando estratégia em memória para arquivos pequenos...")
                return self._analyze_small_dumps(dump_files, monitoring_config)
                
        except Exception as e:
            click.echo(f"❌ Erro durante análise: {e}", err=True)
            return False
        finally:
            self.mongo_inserter.disconnect()
    
    def _analyze_large_dumps(self, dump_files: List[str], monitoring_config: MonitoringConfig) -> bool:
        """
        Analisa dumps grandes usando PostgreSQL
        
        Args:
            dump_files: Lista de arquivos de dump
            monitoring_config: Configuração de monitoramento
            
        Returns:
            True se análise foi bem-sucedida
        """
        try:
            # Mostrar estimativa de tempo
            estimates = self.postgresql_processor.estimate_processing_time(dump_files)
            click.echo(f"⏱️  Tempo estimado: {estimates['estimated_total_time_minutes']:.1f} minutos")
            
            if estimates['estimated_total_time_minutes'] > 30:
                if not click.confirm("⚠️  Processamento pode demorar mais de 30 minutos. Continuar?"):
                    return False
            
            # Processar dumps usando PostgreSQL
            changes = self.postgresql_processor.process_large_dumps(
                dump_files, monitoring_config.tables
            )
            
            # Inserir alterações no MongoDB
            if changes:
                click.echo(f"📝 Detectadas {len(changes)} alterações")
                
                success = self.mongo_inserter.insert_changes(changes)
                if success:
                    click.echo("✅ Alterações inseridas no MongoDB")
                else:
                    click.echo("❌ Erro ao inserir alterações no MongoDB")
                    return False
            else:
                click.echo("ℹ️  Nenhuma alteração detectada")
            
            # Estatísticas finais
            click.echo("🎉 Análise concluída!")
            click.echo(f"📊 Total de alterações detectadas: {len(changes)}")
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Erro na análise: {e}", err=True)
            return False
    
    def _analyze_small_dumps(self, dump_files: List[str], monitoring_config: MonitoringConfig) -> bool:
        """
        Analisa dumps pequenos usando estratégia em memória (código atual)
        
        Args:
            dump_files: Lista de arquivos de dump
            monitoring_config: Configuração de monitoramento
            
        Returns:
            True se análise foi bem-sucedida
        """
        # Processar dumps incrementalmente
        tables_to_monitor = list(monitoring_config.tables.keys())
        previous_data = None
        previous_dump_name = None
        
        total_changes = 0
        
        for i, dump_file in enumerate(dump_files):
            dump_name = Path(dump_file).name
            click.echo(f"\n📊 Processando dump {i+1}/{len(dump_files)}: {dump_name}")
            
            # Parse do dump atual
            current_data = self.dump_parser.parse_dump_file(dump_file, tables_to_monitor)
            
            if not current_data:
                click.echo(f"⚠️  Nenhuma tabela encontrada no dump {dump_name}")
                continue
            
            # Inserir metadados do dump
            dump_info = {
                'file_path': dump_file,
                'file_name': dump_name,
                'tables_found': list(current_data.keys()),
                'total_records': sum(len(table.data) for table in current_data.values())
            }
            self.mongo_inserter.insert_dump_metadata(dump_info)
            
            # Comparar com dump anterior se disponível
            if previous_data and previous_dump_name:
                click.echo(f"🔍 Comparando {previous_dump_name} → {dump_name}")
                
                result = self.data_comparator.compare_dumps(
                    previous_data,
                    current_data,
                    monitoring_config.tables,
                    previous_dump_name,
                    dump_name
                )
                
                if result.changes:
                    click.echo(f"📝 Detectadas {len(result.changes)} alterações")
                    
                    # Inserir alterações no MongoDB
                    if self.mongo_inserter.insert_changes(result.changes):
                        click.echo("✅ Alterações inseridas no MongoDB")
                        total_changes += len(result.changes)
                    else:
                        click.echo("❌ Erro ao inserir alterações no MongoDB", err=True)
                else:
                    click.echo("✅ Nenhuma alteração detectada")
            
            # Atualizar referências para próxima iteração
            previous_data = current_data
            previous_dump_name = dump_name
        
        # Relatório final
        click.echo(f"\n🎉 Análise concluída!")
        click.echo(f"📊 Total de alterações detectadas: {total_changes}")
        
        if self.app_config.debug and self.data_comparator.last_comparison:
            click.echo("\n" + self.data_comparator.generate_summary_report())
        
        return True


@click.group()
@click.option('--debug', is_flag=True, help='Ativar modo debug')
@click.option('--log-level', default='INFO', help='Nível de log (DEBUG, INFO, WARNING, ERROR)')
@click.pass_context
def cli(ctx, debug, log_level):
    """Análise incremental de dumps PostgreSQL com auditoria MongoDB"""
    
    # Configurar logging
    setup_logging(log_level, debug)
    
    # Carregar configurações
    app_config = AppConfig(debug=debug, log_level=log_level)
    db_config = DatabaseConfig.from_env()
    
    # Armazenar no contexto
    ctx.ensure_object(dict)
    ctx.obj['app_config'] = app_config
    ctx.obj['db_config'] = db_config


@cli.command()
@click.option('--dump-dir', default='./dumps', help='Diretório com arquivos de dump')
@click.option('--config', required=True, help='Arquivo de configuração YAML')
@click.pass_context
def analyze(ctx, dump_dir, config):
    """Executa análise incremental dos dumps"""
    
    try:
        # Carregar configuração de monitoramento
        monitoring_config = MonitoringConfig.from_yaml(config)
        
        if not monitoring_config.tables:
            click.echo("❌ Nenhuma tabela configurada para monitoramento", err=True)
            return
        
        # Atualizar diretório de dumps
        app_config = ctx.obj['app_config']
        app_config.dump_directory = dump_dir
        
        # Inicializar analisador
        analyzer = DumpAnalyzer(app_config, ctx.obj['db_config'])
        
        # Executar análise
        success = analyzer.analyze_dumps(monitoring_config)
        
        if not success:
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"❌ Erro: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def stats(ctx):
    """Exibe estatísticas de auditoria"""
    
    analyzer = DumpAnalyzer(ctx.obj['app_config'], ctx.obj['db_config'])
    analyzer.show_statistics()


@cli.command()
@click.option('--table', help='Filtrar por tabela')
@click.option('--column', help='Filtrar por coluna')
@click.option('--limit', default=10, help='Limite de resultados')
@click.pass_context
def query(ctx, table, column, limit):
    """Consulta alterações no MongoDB"""
    
    try:
        mongo_inserter = MongoAuditInserter(ctx.obj['db_config'])
        
        if not mongo_inserter.connect():
            click.echo("❌ Falha ao conectar com MongoDB", err=True)
            return
        
        changes = mongo_inserter.query_changes(
            table_name=table,
            column_name=column,
            limit=limit
        )
        
        if not changes:
            click.echo("📭 Nenhuma alteração encontrada")
            return
        
        click.echo(f"\n📋 {len(changes)} alterações encontradas:")
        click.echo("-" * 80)
        
        for change in changes:
            timestamp = change.get('change_timestamp', 'N/A')
            table_name = change.get('table_name', 'N/A')
            column_name = change.get('column_name', 'N/A')
            pk_value = change.get('primary_key_value', 'N/A')
            old_value = change.get('old_value', 'N/A')
            new_value = change.get('new_value', 'N/A')
            
            click.echo(f"{timestamp} | {table_name}.{column_name} [{pk_value}]")
            click.echo(f"  Antes: {old_value}")
            click.echo(f"  Depois: {new_value}")
            click.echo()
        
    except Exception as e:
        click.echo(f"❌ Erro: {e}", err=True)
    
    finally:
        mongo_inserter.disconnect()


if __name__ == '__main__':
    cli()

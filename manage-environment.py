#!/usr/bin/env python3
"""
Script unificado para gerenciar todo o ambiente de análise CAF
MongoDB + PostgreSQL + Análise de Dumps
"""

import click
import docker
import psycopg2
import pymongo
import subprocess
import time
import os
from pathlib import Path
from typing import Dict, List


class CAFAnalysisManager:
    """Gerenciador completo do ambiente de análise CAF"""
    
    def __init__(self):
        self.docker_client = docker.from_env()
        self.compose_file = "docker-compose.yml"
        
        # Configurações dos bancos
        self.postgres_config = {
            'host': 'localhost',
            'port': 5433,
            'user': 'caf_user',
            'password': 'caf_password123',
            'database': 'caf_analysis'
        }
        
        self.mongo_config = {
            'host': 'localhost',
            'port': 27017,
            'username': 'admin',
            'password': 'admin123',
            'database': 'audit_db'
        }
    
    def start_environment(self) -> bool:
        """Inicia todo o ambiente (MongoDB + PostgreSQL + interfaces web)"""
        try:
            print("🚀 Iniciando ambiente completo de análise CAF...")
            print("   📊 MongoDB (auditoria)")
            print("   🐘 PostgreSQL (dumps grandes)")
            print("   🌐 Interfaces web")
            
            # Verificar se compose file existe
            if not Path(self.compose_file).exists():
                print(f"❌ Arquivo {self.compose_file} não encontrado!")
                return False
            
            # Executar docker-compose up
            result = subprocess.run([
                'docker-compose', 'up', '-d'
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ Erro ao iniciar containers: {result.stderr}")
                return False
            
            print("⏳ Aguardando serviços estarem prontos...")
            
            # Aguardar containers estarem saudáveis
            max_attempts = 60
            services = ['recuperacao-caf-mongo', 'postgres-caf-dumps']
            
            for attempt in range(max_attempts):
                all_healthy = True
                
                for service in services:
                    try:
                        container = self.docker_client.containers.get(service)
                        
                        # Verificar se tem healthcheck
                        health_status = container.attrs.get('State', {}).get('Health', {}).get('Status')
                        
                        if health_status:
                            if health_status != 'healthy':
                                all_healthy = False
                                break
                        else:
                            # Se não tem healthcheck, verificar se está running
                            if container.status != 'running':
                                all_healthy = False
                                break
                                
                    except docker.errors.NotFound:
                        all_healthy = False
                        break
                
                if all_healthy:
                    # Testar conexões
                    if self.test_postgres() and self.test_mongo():
                        print("✅ Ambiente iniciado com sucesso!")
                        self.show_connection_info()
                        return True
                
                time.sleep(2)
            
            print("⏰ Timeout aguardando serviços")
            return False
            
        except Exception as e:
            print(f"❌ Erro ao iniciar ambiente: {e}")
            return False
    
    def stop_environment(self) -> bool:
        """Para todo o ambiente"""
        try:
            print("🛑 Parando ambiente...")
            
            result = subprocess.run([
                'docker-compose', 'down'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Ambiente parado com sucesso!")
                return True
            else:
                print(f"❌ Erro ao parar: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao parar ambiente: {e}")
            return False
    
    def test_postgres(self) -> bool:
        """Testa conexão com PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.postgres_config, connect_timeout=5)
            conn.close()
            return True
        except Exception:
            return False
    
    def test_mongo(self) -> bool:
        """Testa conexão com MongoDB"""
        try:
            # Usar conexão administrativa primeiro
            client = pymongo.MongoClient('mongodb://admin:admin123@localhost:27017/', serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            return True
        except Exception:
            return False
    
    def status(self) -> None:
        """Mostra status completo do ambiente"""
        try:
            print("📊 Status do Ambiente de Análise CAF:")
            print("━" * 60)
            
            containers_info = [
                ('recuperacao-caf-mongo', 'MongoDB', '27017'),
                ('recuperacao-caf-mongo-express', 'Mongo Express', '8080'),
                ('postgres-caf-dumps', 'PostgreSQL', '5433'),
                ('pgadmin-caf', 'PgAdmin', '8082')
            ]
            
            for container_name, service_name, port in containers_info:
                try:
                    container = self.docker_client.containers.get(container_name)
                    status = container.status
                    
                    if status == 'running':
                        print(f"✅ {service_name}: {status}")
                        if port:
                            print(f"   🔗 http://localhost:{port}")
                    else:
                        print(f"⚠️  {service_name}: {status}")
                        
                except docker.errors.NotFound:
                    print(f"❌ {service_name}: não encontrado")
            
            # Testar conexões dos bancos
            print(f"\n🔍 Teste de Conexões:")
            
            if self.test_postgres():
                print(f"✅ PostgreSQL: conectado")
            else:
                print(f"❌ PostgreSQL: não conecta")
            
            if self.test_mongo():
                print(f"✅ MongoDB: conectado")
            else:
                print(f"❌ MongoDB: não conecta")
            
        except Exception as e:
            print(f"❌ Erro ao verificar status: {e}")
    
    def show_connection_info(self) -> None:
        """Mostra informações de conexão"""
        print("\n🔗 Informações de Conexão:")
        print("━" * 50)
        
        print("📊 MongoDB (Auditoria):")
        print("  Host: localhost:27017")
        print("  Admin: admin / admin123")
        print("  App: app_user / app_password")
        print("  Database: audit_db")
        print("  String: mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db")
        
        print("\n🐘 PostgreSQL (Dumps CAF):")
        print("  Host: localhost:5433")
        print("  User: caf_user")
        print("  Password: caf_password123")
        print("  Database: caf_analysis")
        print("  String: postgresql://caf_user:caf_password123@localhost:5433/caf_analysis")
        
        print("\n🌐 Interfaces Web:")
        print("  📊 Mongo Express: http://localhost:8080")
        print("  🐘 PgAdmin: http://localhost:8082")
        print("     Email: admin@caf.local")
        print("     Senha: admin123")
    
    def import_caf_dumps(self) -> bool:
        """Importa dumps CAF para PostgreSQL"""
        try:
            print("📥 Importando dumps CAF...")
            
            if not self.test_postgres():
                print("❌ PostgreSQL não está disponível")
                return False
            
            # Verificar dumps CAF disponíveis
            dumps = list(Path("dumps").glob("*caf*"))
            
            if not dumps:
                print("📭 Nenhum dump CAF encontrado na pasta dumps/")
                return False
            
            print(f"📁 Encontrados {len(dumps)} dumps CAF:")
            for dump in dumps:
                size_mb = dump.stat().st_size / (1024 * 1024)
                print(f"  - {dump.name} ({size_mb:.1f} MB)")
            
            # Executar script de importação dentro do container
            result = subprocess.run([
                'docker', 'exec', 'postgres-caf-dumps',
                'python3', '/docker-entrypoint-initdb.d/import_caf_dumps.py'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Importação concluída!")
                if result.stdout:
                    print(result.stdout)
                return True
            else:
                print(f"❌ Erro na importação: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao importar dumps: {e}")
            return False
    
    def run_analysis(self) -> bool:
        """Executa análise incremental usando dumps grandes"""
        try:
            print("🔍 Executando análise incremental de dumps CAF...")
            
            # Verificar se ambiente está rodando
            if not (self.test_postgres() and self.test_mongo()):
                print("❌ Ambiente não está completamente disponível")
                return False
            
            # Executar análise usando estratégia PostgreSQL
            result = subprocess.run([
                'python', 'src/main.py', 
                'analyze', 
                '--config', 'config/monitoring_config.yaml',
                '--dump-dir', 'dumps/'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Análise concluída!")
                print(result.stdout)
                return True
            else:
                print(f"❌ Erro na análise: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao executar análise: {e}")
            return False
    
    def query_changes(self, table: str = None, limit: int = 10) -> None:
        """Consulta alterações detectadas"""
        try:
            print(f"📋 Consultando alterações (limit: {limit})...")
            
            cmd = ['python', 'src/main.py', 'query', '--limit', str(limit)]
            if table:
                cmd.extend(['--table', table])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(result.stdout)
            else:
                print(f"❌ Erro na consulta: {result.stderr}")
                
        except Exception as e:
            print(f"❌ Erro ao consultar alterações: {e}")
    
    def logs(self, service: str = None) -> None:
        """Mostra logs dos serviços"""
        try:
            if service:
                print(f"📋 Logs do {service}:")
                result = subprocess.run([
                    'docker-compose', 'logs', '--tail', '50', service
                ], capture_output=True, text=True)
            else:
                print(f"📋 Logs de todos os serviços:")
                result = subprocess.run([
                    'docker-compose', 'logs', '--tail', '20'
                ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(result.stdout)
            else:
                print(f"❌ Erro ao obter logs: {result.stderr}")
                
        except Exception as e:
            print(f"❌ Erro ao mostrar logs: {e}")


@click.group()
def cli():
    """🔧 Gerenciador do Ambiente de Análise CAF"""
    pass


@cli.command()
def start():
    """🚀 Iniciar ambiente completo"""
    manager = CAFAnalysisManager()
    manager.start_environment()


@cli.command()
def stop():
    """🛑 Parar ambiente completo"""
    manager = CAFAnalysisManager()
    manager.stop_environment()


@cli.command()
def status():
    """📊 Status do ambiente"""
    manager = CAFAnalysisManager()
    manager.status()


@cli.command()
def info():
    """🔗 Informações de conexão"""
    manager = CAFAnalysisManager()
    manager.show_connection_info()


@cli.command()
def import_dumps():
    """📥 Importar dumps CAF"""
    manager = CAFAnalysisManager()
    manager.import_caf_dumps()


@cli.command()
def analyze():
    """🔍 Executar análise incremental"""
    manager = CAFAnalysisManager()
    manager.run_analysis()


@cli.command()
@click.option('--table', help='Filtrar por tabela')
@click.option('--limit', default=10, help='Limite de resultados')
def query(table, limit):
    """📋 Consultar alterações detectadas"""
    manager = CAFAnalysisManager()
    manager.query_changes(table, limit)


@cli.command()
@click.option('--service', help='Serviço específico (mongodb, postgres-caf, etc.)')
def logs(service):
    """📋 Ver logs dos serviços"""
    manager = CAFAnalysisManager()
    manager.logs(service)


@cli.command()
def shell_postgres():
    """🐘 Shell PostgreSQL"""
    print("🐘 Abrindo shell PostgreSQL...")
    print("💡 Digite \\q para sair")
    os.system("docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis")


@cli.command()
def shell_mongo():
    """📊 Shell MongoDB"""
    print("📊 Abrindo shell MongoDB...")
    print("💡 Digite exit para sair")
    os.system("docker exec -it recuperacao-caf-mongo mongosh audit_db -u app_user -p app_password")


@cli.command()
def reset():
    """🧹 Reset completo (remove todos os dados)"""
    if click.confirm("⚠️  Isso removerá TODOS os dados. Continuar?"):
        manager = CAFAnalysisManager()
        print("🛑 Parando containers...")
        subprocess.run(['docker-compose', 'down', '-v'])
        print("🗑️  Removendo volumes...")
        subprocess.run(['docker', 'volume', 'prune', '-f'])
        print("✅ Reset concluído!")


if __name__ == '__main__':
    cli()

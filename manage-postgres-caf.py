#!/usr/bin/env python3
"""
Script de gerenciamento do PostgreSQL para dumps CAF
"""

import click
import docker
import psycopg2
import os
import subprocess
import time
from pathlib import Path
from typing import List, Dict


class PostgreSQLCAFManager:
    """Gerenciador do PostgreSQL para dumps CAF"""
    
    def __init__(self):
        self.docker_client = docker.from_env()
        self.container_name = "postgres-caf-dumps"
        self.compose_file = "docker-compose-postgres.yml"
        
        self.db_config = {
            'host': 'localhost',
            'port': 5433,
            'user': 'caf_user',
            'password': 'caf_password123',
            'database': 'caf_analysis'
        }
    
    def start_postgres(self) -> bool:
        """Inicia o PostgreSQL com docker-compose"""
        try:
            print("🚀 Iniciando PostgreSQL para dumps CAF...")
            
            # Verificar se compose file existe
            if not Path(self.compose_file).exists():
                print(f"❌ Arquivo {self.compose_file} não encontrado!")
                return False
            
            # Executar docker-compose up
            result = subprocess.run([
                'docker-compose', '-f', self.compose_file, 'up', '-d'
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ Erro ao iniciar containers: {result.stderr}")
                return False
            
            print("⏳ Aguardando PostgreSQL estar pronto...")
            
            # Aguardar container estar saudável
            max_attempts = 30
            for attempt in range(max_attempts):
                try:
                    container = self.docker_client.containers.get(self.container_name)
                    health = container.attrs['State']['Health']['Status']
                    
                    if health == 'healthy':
                        print("✅ PostgreSQL iniciado com sucesso!")
                        return True
                    elif health == 'unhealthy':
                        print("❌ PostgreSQL falhou no healthcheck")
                        return False
                    
                    time.sleep(2)
                    
                except docker.errors.NotFound:
                    print("❌ Container não encontrado")
                    return False
                except KeyError:
                    # Container sem healthcheck, tentar conectar diretamente
                    if self.test_connection():
                        print("✅ PostgreSQL iniciado com sucesso!")
                        return True
                    time.sleep(2)
            
            print("⏰ Timeout aguardando PostgreSQL")
            return False
            
        except Exception as e:
            print(f"❌ Erro ao iniciar PostgreSQL: {e}")
            return False
    
    def stop_postgres(self) -> bool:
        """Para o PostgreSQL"""
        try:
            print("🛑 Parando PostgreSQL...")
            
            result = subprocess.run([
                'docker-compose', '-f', self.compose_file, 'down'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ PostgreSQL parado com sucesso!")
                return True
            else:
                print(f"❌ Erro ao parar: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao parar PostgreSQL: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Testa conexão com PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config, connect_timeout=5)
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                
            conn.close()
            return True
            
        except Exception:
            return False
    
    def status(self) -> None:
        """Mostra status dos containers"""
        try:
            print("📊 Status dos containers CAF:")
            print("-" * 50)
            
            containers = ['postgres-caf-dumps', 'pgadmin-caf']
            
            for container_name in containers:
                try:
                    container = self.docker_client.containers.get(container_name)
                    status = container.status
                    
                    if status == 'running':
                        print(f"✅ {container_name}: {status}")
                        
                        # Mostrar portas
                        ports = container.attrs['NetworkSettings']['Ports']
                        for internal, external in ports.items():
                            if external:
                                host_port = external[0]['HostPort']
                                print(f"   🔗 {internal} → localhost:{host_port}")
                    else:
                        print(f"⚠️  {container_name}: {status}")
                        
                except docker.errors.NotFound:
                    print(f"❌ {container_name}: não encontrado")
            
            # Testar conexão PostgreSQL
            print(f"\n🔍 Teste de conexão:")
            if self.test_connection():
                print(f"✅ PostgreSQL: conectado")
                print(f"   🔗 Conexão: postgresql://caf_user:***@localhost:5433/caf_analysis")
            else:
                print(f"❌ PostgreSQL: não conecta")
            
        except Exception as e:
            print(f"❌ Erro ao verificar status: {e}")
    
    def logs(self, service: str = "postgres-caf") -> None:
        """Mostra logs do serviço"""
        try:
            print(f"📋 Logs do {service}:")
            print("-" * 50)
            
            result = subprocess.run([
                'docker-compose', '-f', self.compose_file, 'logs', '--tail', '50', service
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(result.stdout)
            else:
                print(f"❌ Erro ao obter logs: {result.stderr}")
                
        except Exception as e:
            print(f"❌ Erro ao mostrar logs: {e}")
    
    def import_dumps(self) -> bool:
        """Executa importação dos dumps CAF"""
        try:
            print("📥 Iniciando importação de dumps CAF...")
            
            if not self.test_connection():
                print("❌ PostgreSQL não está disponível")
                return False
            
            # Executar script de importação dentro do container
            result = subprocess.run([
                'docker', 'exec', '-i', self.container_name,
                'python3', '/docker-entrypoint-initdb.d/import_caf_dumps.py'
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Importação concluída!")
                print(result.stdout)
                return True
            else:
                print(f"❌ Erro na importação: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao importar dumps: {e}")
            return False
    
    def list_schemas(self) -> None:
        """Lista schemas criados"""
        try:
            if not self.test_connection():
                print("❌ PostgreSQL não está disponível")
                return
            
            conn = psycopg2.connect(**self.db_config)
            
            with conn.cursor() as cur:
                # Listar schemas CAF
                cur.execute("""
                    SELECT schema_name, 
                           obj_description(oid, 'pg_namespace') as description
                    FROM information_schema.schemata s
                    JOIN pg_namespace n ON n.nspname = s.schema_name
                    WHERE schema_name LIKE 'caf_%'
                    ORDER BY schema_name
                """)
                
                schemas = cur.fetchall()
                
                if schemas:
                    print("📁 Schemas CAF criados:")
                    print("-" * 50)
                    
                    for schema, desc in schemas:
                        print(f"📂 {schema}")
                        if desc:
                            print(f"   {desc}")
                        
                        # Contar tabelas
                        cur.execute("""
                            SELECT COUNT(*) 
                            FROM information_schema.tables 
                            WHERE table_schema = %s AND table_type = 'BASE TABLE'
                        """, (schema,))
                        
                        table_count = cur.fetchone()[0]
                        print(f"   📊 {table_count} tabelas")
                        print()
                else:
                    print("📭 Nenhum schema CAF encontrado")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Erro ao listar schemas: {e}")
    
    def connect_info(self) -> None:
        """Mostra informações de conexão"""
        print("🔗 Informações de Conexão:")
        print("-" * 40)
        print(f"Host: localhost")
        print(f"Porta: 5433")
        print(f"Banco: caf_analysis")
        print(f"Usuário: caf_user")
        print(f"Senha: caf_password123")
        print()
        print(f"📋 String de conexão:")
        print(f"postgresql://caf_user:caf_password123@localhost:5433/caf_analysis")
        print()
        print(f"🌐 PgAdmin (interface web):")
        print(f"URL: http://localhost:8081")
        print(f"Email: admin@caf.local")
        print(f"Senha: admin123")


@click.group()
def cli():
    """Gerenciador PostgreSQL para dumps CAF"""
    pass


@cli.command()
def start():
    """Inicia PostgreSQL e PgAdmin"""
    manager = PostgreSQLCAFManager()
    
    if manager.start_postgres():
        print("\n🎉 PostgreSQL iniciado com sucesso!")
        manager.connect_info()
    else:
        print("\n❌ Falha ao iniciar PostgreSQL")


@cli.command()
def stop():
    """Para PostgreSQL e PgAdmin"""
    manager = PostgreSQLCAFManager()
    manager.stop_postgres()


@cli.command()
def status():
    """Mostra status dos containers"""
    manager = PostgreSQLCAFManager()
    manager.status()


@cli.command()
@click.option('--service', default='postgres-caf', help='Serviço para mostrar logs')
def logs(service):
    """Mostra logs dos containers"""
    manager = PostgreSQLCAFManager()
    manager.logs(service)


@cli.command()
def import_dumps():
    """Importa dumps CAF para PostgreSQL"""
    manager = PostgreSQLCAFManager()
    
    print("🔍 Verificando dumps disponíveis...")
    dumps = list(Path("dumps").glob("*caf*.sql")) + list(Path("dumps").glob("*caf*.dump"))
    
    if not dumps:
        print("📭 Nenhum dump CAF encontrado na pasta dumps/")
        return
    
    print(f"📁 Encontrados {len(dumps)} dumps CAF:")
    for dump in dumps:
        size_mb = dump.stat().st_size / (1024 * 1024)
        print(f"  - {dump.name} ({size_mb:.1f} MB)")
    
    if click.confirm("\n🚀 Importar todos os dumps?"):
        manager.import_dumps()


@cli.command()
def schemas():
    """Lista schemas CAF criados"""
    manager = PostgreSQLCAFManager()
    manager.list_schemas()


@cli.command()
def info():
    """Mostra informações de conexão"""
    manager = PostgreSQLCAFManager()
    manager.connect_info()


@cli.command()
def shell():
    """Abre shell psql no PostgreSQL"""
    manager = PostgreSQLCAFManager()
    
    if not manager.test_connection():
        print("❌ PostgreSQL não está disponível")
        return
    
    print("🐘 Abrindo shell PostgreSQL...")
    print("💡 Digite \\q para sair")
    
    os.system(f"docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis")


if __name__ == '__main__':
    cli()

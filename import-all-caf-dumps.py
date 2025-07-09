#!/usr/bin/env python3
"""
Script para importar automaticamente todos os dumps CAF encontrados na pasta dumps/
Detecta automaticamente se são arquivos comprimidos (gzip) ou texto SQL
Cria um schema separado para cada dump baseado na data extraída do nome
"""

import os
import re
import subprocess
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import time


class CAFDumpBatchImporter:
    """Importador em lote de dumps CAF"""
    
    def __init__(self, dumps_dir: str = "dumps"):
        self.dumps_dir = Path(dumps_dir)
        self.container_name = "postgres-caf-dumps"
        self.db_config = {
            'host': 'localhost',
            'port': 5433,
            'user': 'caf_user',
            'password': 'caf_password123',
            'database': 'caf_analysis'
        }
        
    def find_caf_dumps(self) -> List[Path]:
        """Encontra todos os dumps CAF"""
        if not self.dumps_dir.exists():
            return []
        
        patterns = ["*caf*.sql", "*caf*.dump", "*CAF*.sql", "*CAF*.dump"]
        caf_dumps = set()
        
        for pattern in patterns:
            for file_path in self.dumps_dir.glob(pattern):
                if not file_path.name.startswith('temp_'):
                    caf_dumps.add(file_path)
        
        return sorted(list(caf_dumps), key=lambda x: x.name)
    
    def is_gzip_file(self, file_path: Path) -> bool:
        """Verifica se arquivo é comprimido com gzip"""
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(3)
                return magic == b'\x1f\x8b\x08'
        except:
            return False
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """Extrai data do nome do arquivo"""
        patterns = [
            r'dump-caf.*?-(\d{8})-\d+',  # dump-caf_mapa-20250301-202506151151
            r'caf.*?(\d{8})',             # caf20250301
            r'(\d{4}-\d{2}-\d{2})',       # 2025-03-01
            r'(\d{8})',                   # 20250301
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                if '-' in date_str:
                    date_str = date_str.replace('-', '')
                
                if len(date_str) == 8 and date_str.isdigit():
                    return date_str
        
        return None
    
    def generate_schema_name(self, filename: str) -> str:
        """Gera nome do schema baseado no arquivo"""
        date_str = self.extract_date_from_filename(filename)
        if date_str:
            return f"caf_{date_str}"
        else:
            # Usar timestamp como fallback
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            return f"caf_{timestamp}"
    
    def check_container_running(self) -> bool:
        """Verifica se container PostgreSQL está rodando"""
        try:
            result = subprocess.run([
                'docker', 'ps', '--filter', f'name={self.container_name}', '--format', '{{.Status}}'
            ], capture_output=True, text=True)
            
            return result.returncode == 0 and result.stdout.strip()
        except:
            return False
    
    def test_connection(self) -> bool:
        """Testa conexão com PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            return True
        except:
            return False
    
    def schema_exists(self, schema_name: str) -> bool:
        """Verifica se schema já existe e tem tabelas"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM pg_tables 
                    WHERE schemaname = %s
                """, (schema_name,))
                
                count = cursor.fetchone()[0]
            conn.close()
            return count > 0
        except:
            return False
    
    def create_schema(self, schema_name: str) -> bool:
        """Cria schema no banco"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                
                # Adicionar comentário
                date_part = schema_name.replace('caf_', '')
                if len(date_part) == 8 and date_part.isdigit():
                    formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                    comment = f"Schema para dump CAF de {formatted_date}"
                else:
                    comment = f"Schema para dump CAF ({schema_name})"
                
                cursor.execute(f"COMMENT ON SCHEMA \"{schema_name}\" IS '{comment}'")
            
            conn.close()
            
            # Instalar extensões necessárias
            self.install_required_extensions(schema_name)
            return True
            
        except Exception as e:
            print(f"❌ Erro ao criar schema {schema_name}: {e}")
            return False
    
    def import_gzip_dump(self, dump_file: Path, schema_name: str) -> bool:
        """Importa dump comprimido com gzip"""
        try:
            print(f"📥 Importando dump comprimido: {dump_file.name}")
            print(f"⏱️  ATENÇÃO: Pode demorar 30-90 minutos para dumps grandes!")
            
            # Comando para descomprimir e importar
            cmd = f"""
            export PGPASSWORD='{self.db_config['password']}'
            gunzip -c /dumps/{dump_file.name} | \\
            grep -v 'SET transaction_timeout' | \\
            grep -v 'SET idle_in_transaction_session_timeout' | \\
            grep -v 'CREATE SCHEMA.*caf_mapa' | \\
            grep -v 'CREATE SCHEMA.*caf_reference' | \\
            sed 's/public\\./"{schema_name}"\\./g; s/SCHEMA public/SCHEMA "{schema_name}"/g; s/caf_mapa\\./"{schema_name}"\\./g; s/caf_reference\\./"{schema_name}"\\./g' | \\
            psql -U {self.db_config['user']} -d {self.db_config['database']} \\
                 -v ON_ERROR_STOP=1 --single-transaction
            """
            
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'sh', '-c', cmd
            ], capture_output=True, text=True, timeout=7200)  # 2 horas timeout
            
            if result.returncode == 0:
                print(f"✅ {dump_file.name} importado com sucesso para {schema_name}")
                return True
            else:
                print(f"❌ Erro na importação de {dump_file.name}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout na importação de {dump_file.name} (mais de 2 horas)")
            return False
        except Exception as e:
            print(f"❌ Erro ao importar {dump_file.name}: {e}")
            return False
    
    def import_sql_dump(self, dump_file: Path, schema_name: str) -> bool:
        """Importa dump SQL texto"""
        try:
            print(f"📥 Importando dump SQL: {dump_file.name}")
            
            # Copiar arquivo para container
            temp_file = f"/tmp/{dump_file.name}"
            copy_result = subprocess.run([
                'docker', 'cp', str(dump_file), f'{self.container_name}:{temp_file}'
            ], capture_output=True, text=True)
            
            if copy_result.returncode != 0:
                print(f"❌ Erro ao copiar {dump_file.name} para container")
                return False
            
            # Importar arquivo
            import_cmd = f"""
            export PGPASSWORD='{self.db_config['password']}'
            psql -U {self.db_config['user']} -d {self.db_config['database']} \\
                 -c "SET search_path TO \\"{schema_name}\\", public;" \\
                 -f {temp_file}
            """
            
            result = subprocess.run([
                'docker', 'exec', self.container_name, 'sh', '-c', import_cmd
            ], capture_output=True, text=True, timeout=3600)  # 1 hora timeout
            
            # Limpar arquivo temporário
            subprocess.run([
                'docker', 'exec', self.container_name, 'rm', temp_file
            ], capture_output=True)
            
            if result.returncode == 0:
                print(f"✅ {dump_file.name} importado com sucesso para {schema_name}")
                return True
            else:
                print(f"❌ Erro na importação de {dump_file.name}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout na importação de {dump_file.name}")
            return False
        except Exception as e:
            print(f"❌ Erro ao importar {dump_file.name}: {e}")
            return False
    
    def get_schema_info(self, schema_name: str) -> Dict:
        """Obtém informações do schema importado"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                # Contar tabelas
                cursor.execute("""
                    SELECT COUNT(*) FROM pg_tables 
                    WHERE schemaname = %s
                """, (schema_name,))
                table_count = cursor.fetchone()[0]
                
                # Calcular tamanho
                cursor.execute("""
                    SELECT COALESCE(
                        ROUND(SUM(pg_total_relation_size(c.oid))/1024/1024), 0
                    ) as size_mb
                    FROM pg_class c 
                    JOIN pg_namespace n ON n.oid = c.relnamespace 
                    WHERE n.nspname = %s
                """, (schema_name,))
                size_mb = cursor.fetchone()[0] or 0
                
                # Top 5 tabelas por tamanho
                cursor.execute("""
                    SELECT 
                        tablename,
                        COALESCE(n_tup_ins + n_tup_upd + n_tup_del, 0) as rows
                    FROM pg_stat_user_tables 
                    WHERE schemaname = %s 
                    ORDER BY rows DESC 
                    LIMIT 5
                """, (schema_name,))
                top_tables = cursor.fetchall()
            
            conn.close()
            
            return {
                'table_count': table_count,
                'size_mb': size_mb,
                'top_tables': top_tables
            }
        except:
            return {'table_count': 0, 'size_mb': 0, 'top_tables': []}
    
    def register_import_metadata(self, dump_file: Path, schema_name: str, success: bool, info: Dict):
        """Registra metadados da importação"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                date_str = self.extract_date_from_filename(dump_file.name)
                dump_date = None
                if date_str:
                    dump_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                
                file_size_mb = dump_file.stat().st_size / (1024 * 1024)
                is_gzip = self.is_gzip_file(dump_file)
                
                notes = []
                if success:
                    notes.append("Importado com sucesso")
                    notes.append(f"{info['table_count']} tabelas")
                    notes.append(f"{info['size_mb']} MB")
                else:
                    notes.append("Falha na importação")
                
                if is_gzip:
                    notes.append("arquivo gzip")
                
                cursor.execute("""
                    INSERT INTO caf_analysis.dump_metadata 
                    (dump_file, schema_name, dump_date, file_size_mb, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (dump_file) DO UPDATE SET
                        schema_name = EXCLUDED.schema_name,
                        imported_at = NOW(),
                        notes = EXCLUDED.notes,
                        file_size_mb = EXCLUDED.file_size_mb
                """, (
                    dump_file.name,
                    schema_name,
                    dump_date,
                    file_size_mb,
                    " | ".join(notes)
                ))
            
            conn.close()
            
        except Exception as e:
            print(f"⚠️  Erro ao registrar metadados: {e}")
    
    def install_required_extensions(self, schema_name: str) -> bool:
        """Instala extensões necessárias para o schema"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # Instalar extensão UUID na base de dados
                try:
                    cursor.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
                    print(f"   🔧 Extensão uuid-ossp instalada")
                except Exception as e:
                    print(f"   ⚠️  Aviso: Erro ao instalar uuid-ossp: {e}")
                
                # Criar funções UUID no schema específico se necessário
                try:
                    cursor.execute(f'''
                        CREATE OR REPLACE FUNCTION "{schema_name}".uuid_generate_v4()
                        RETURNS uuid AS 'SELECT public.uuid_generate_v4()'
                        LANGUAGE SQL VOLATILE;
                    ''')
                    print(f"   🔧 Função uuid_generate_v4 criada no schema {schema_name}")
                except Exception as e:
                    print(f"   ⚠️  Aviso: {e}")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao instalar extensões: {e}")
            return False

    def create_metadata_table(self) -> bool:
        """Cria tabela de metadados se não existir"""
        try:
            conn = psycopg2.connect(**self.db_config)
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # Verificar se schema caf_analysis existe
                cursor.execute("""
                    CREATE SCHEMA IF NOT EXISTS caf_analysis
                """)
                
                # Criar tabela de metadados
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS caf_analysis.dump_metadata (
                        id SERIAL PRIMARY KEY,
                        dump_file VARCHAR(255) NOT NULL,
                        schema_name VARCHAR(255) NOT NULL,
                        dump_date DATE,
                        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        file_size_mb DECIMAL(10,2),
                        notes TEXT,
                        UNIQUE(dump_file, schema_name)
                    )
                """)
                
                print(f"   📋 Tabela de metadados verificada/criada")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao criar tabela de metadados: {e}")
            return False

    def run_batch_import(self) -> None:
        """Executa importação em lote de todos os dumps CAF"""
        print("🐘 Importador em Lote de Dumps CAF")
        print("=" * 50)
        
        # Verificar pré-requisitos
        if not self.check_container_running():
            print("❌ Container PostgreSQL não está rodando")
            print("💡 Execute: python manage-environment.py start")
            return
        
        if not self.test_connection():
            print("❌ Não foi possível conectar ao PostgreSQL")
            return
        
        print("✅ Container PostgreSQL está rodando e acessível")
        
        # Criar tabela de metadados
        if not self.create_metadata_table():
            print("❌ Falha ao verificar/criar tabela de metadados")
            return
        
        # Encontrar dumps
        dumps = self.find_caf_dumps()
        if not dumps:
            print("📭 Nenhum dump CAF encontrado na pasta dumps/")
            return
        
        print(f"\n📁 Encontrados {len(dumps)} dumps CAF:")
        
        total_size_mb = 0
        dump_info = []
        
        for dump in dumps:
            size_mb = dump.stat().st_size / (1024 * 1024)
            is_gzip = self.is_gzip_file(dump)
            schema_name = self.generate_schema_name(dump.name)
            already_imported = self.schema_exists(schema_name)
            
            total_size_mb += size_mb
            dump_info.append({
                'file': dump,
                'size_mb': size_mb,
                'is_gzip': is_gzip,
                'schema_name': schema_name,
                'already_imported': already_imported
            })
            
            status = "✅ JÁ IMPORTADO" if already_imported else ("🗜️ GZIP" if is_gzip else "📄 SQL")
            print(f"  {status} {dump.name} → {schema_name} ({size_mb:.1f} MB)")
        
        print(f"\n📊 Tamanho total: {total_size_mb:.1f} MB")
        
        # Filtrar apenas dumps não importados
        pending_dumps = [d for d in dump_info if not d['already_imported']]
        
        if not pending_dumps:
            print("✅ Todos os dumps já foram importados!")
            self.show_summary()
            return
        
        print(f"🚀 {len(pending_dumps)} dumps pendentes para importação")
        
        # Estimativa de tempo
        estimated_minutes = sum(d['size_mb'] for d in pending_dumps) / 100  # ~100MB/min
        print(f"⏱️  Tempo estimado: {estimated_minutes:.1f} minutos")
        
        print("\n" + "=" * 50)
        
        # Processar cada dump pendente
        successful_imports = 0
        failed_imports = 0
        
        for i, dump_data in enumerate(pending_dumps, 1):
            dump_file = dump_data['file']
            schema_name = dump_data['schema_name']
            is_gzip = dump_data['is_gzip']
            
            print(f"\n[{i}/{len(pending_dumps)}] Processando: {dump_file.name}")
            print(f"   📊 Schema: {schema_name}")
            print(f"   📦 Tipo: {'GZIP Comprimido' if is_gzip else 'SQL Texto'}")
            print(f"   💾 Tamanho: {dump_data['size_mb']:.1f} MB")
            
            # Criar schema
            if not self.create_schema(schema_name):
                print(f"   ❌ Falha ao criar schema")
                failed_imports += 1
                continue
            
            # Instalar extensões necessárias
            if not self.install_required_extensions(schema_name):
                print(f"   ❌ Falha ao instalar extensões")
                failed_imports += 1
                continue
            
            # Importar baseado no tipo
            start_time = time.time()
            
            if is_gzip:
                success = self.import_gzip_dump(dump_file, schema_name)
            else:
                success = self.import_sql_dump(dump_file, schema_name)
            
            elapsed_time = time.time() - start_time
            
            # Obter informações do schema
            info = self.get_schema_info(schema_name) if success else {}
            
            # Registrar metadados
            self.register_import_metadata(dump_file, schema_name, success, info)
            
            if success:
                successful_imports += 1
                print(f"   ✅ Importado em {elapsed_time:.1f}s")
                if info.get('table_count', 0) > 0:
                    print(f"   📋 {info['table_count']} tabelas, {info['size_mb']} MB")
                    if info.get('top_tables'):
                        print(f"   🔝 Maior tabela: {info['top_tables'][0][0]} ({info['top_tables'][0][1]} rows)")
            else:
                failed_imports += 1
                print(f"   ❌ Falha na importação")
        
        # Resumo final
        print("\n" + "=" * 50)
        print("🎉 Importação em lote concluída!")
        print(f"✅ Sucessos: {successful_imports}")
        print(f"❌ Falhas: {failed_imports}")
        
        if successful_imports > 0:
            print(f"\n🔗 Acesse PgAdmin: http://localhost:8082")
            self.show_summary()
    
    def show_summary(self):
        """Mostra resumo dos schemas importados"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT schema_name, dump_file, dump_date, 
                           file_size_mb, imported_at, notes
                    FROM caf_analysis.dump_metadata 
                    WHERE notes LIKE '%sucesso%'
                    ORDER BY dump_date DESC, imported_at DESC
                """)
                
                rows = cursor.fetchall()
            conn.close()
            
            if rows:
                print(f"\n📊 Resumo dos schemas CAF importados:")
                print("-" * 80)
                
                for row in rows:
                    schema, file, date, size, imported, notes = row
                    print(f"🗂️  {schema}")
                    print(f"   📁 {file}")
                    print(f"   📅 Data: {date or 'N/A'}")
                    print(f"   💾 {size:.1f} MB | {notes}")
                    print()
                    
        except Exception as e:
            print(f"⚠️  Erro ao mostrar resumo: {e}")


def main():
    """Função principal"""
    importer = CAFDumpBatchImporter()
    importer.run_batch_import()


if __name__ == "__main__":
    main()

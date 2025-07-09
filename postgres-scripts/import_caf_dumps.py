#!/usr/bin/env python3
"""
Script para importar dumps CAF automaticamente no PostgreSQL
Extrai data do nome do arquivo e cria schemas organizados
"""

import os
import re
import subprocess
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional


class CAFDumpImporter:
    """Importador de dumps CAF para PostgreSQL"""
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.dumps_dir = Path("/dumps")
        
    def find_caf_dumps(self) -> List[Path]:
        """Encontra todos os dumps CAF no diretório"""
        caf_dumps = []
        
        if self.dumps_dir.exists():
            # Buscar arquivos que contenham "caf" no nome
            patterns = ["*caf*.sql", "*caf*.dump", "*CAF*.sql", "*CAF*.dump"]
            
            for pattern in patterns:
                caf_dumps.extend(self.dumps_dir.glob(pattern))
        
        # Ordenar por nome (que geralmente contém data)
        return sorted(caf_dumps, key=lambda x: x.name)
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """
        Extrai data do nome do arquivo dump
        Suporta formatos: dump-caf_mapa-20250301-202506151151.sql
        """
        patterns = [
            r'dump-caf.*?-(\d{8})-\d+',  # dump-caf_mapa-20250301-202506151151
            r'caf.*?(\d{8})',             # caf20250301 ou caf_20250301
            r'(\d{4}-\d{2}-\d{2})',       # 2025-03-01
            r'(\d{8})',                   # 20250301
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                date_str = match.group(1)
                # Converter para formato padrão YYYYMMDD
                if '-' in date_str:
                    date_str = date_str.replace('-', '')
                
                if len(date_str) == 8 and date_str.isdigit():
                    return date_str
        
        return None
    
    def generate_schema_name(self, date_str: Optional[str]) -> str:
        """Gera nome do schema baseado na data"""
        if date_str:
            return f"caf_{date_str}"
        else:
            # Usar data atual se não conseguir extrair
            return f"caf_{datetime.now().strftime('%Y%m%d')}"
    
    def get_file_size_mb(self, file_path: Path) -> float:
        """Retorna tamanho do arquivo em MB"""
        return file_path.stat().st_size / (1024 * 1024)
    
    def create_schema(self, schema_name: str) -> bool:
        """Cria schema no banco se não existir"""
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cur:
                # Criar schema
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                
                # Adicionar comentário
                date_part = schema_name.replace('caf_', '')
                if len(date_part) == 8:
                    formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                    comment = f"Schema para dump CAF de {formatted_date}"
                else:
                    comment = f"Schema para dump CAF ({schema_name})"
                
                cur.execute(f"COMMENT ON SCHEMA \"{schema_name}\" IS '{comment}'")
                
                conn.commit()
                
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Erro ao criar schema {schema_name}: {e}")
            return False
    
    def import_dump(self, dump_file: Path, schema_name: str) -> bool:
        """Importa dump para o schema especificado"""
        try:
            print(f"📥 Importando {dump_file.name} para schema {schema_name}...")
            
            # Configurar variáveis de ambiente
            env = os.environ.copy()
            env.update({
                'PGPASSWORD': self.db_config['password'],
                'PGUSER': self.db_config['user'],
                'PGHOST': self.db_config['host'],
                'PGPORT': str(self.db_config['port']),
                'PGDATABASE': self.db_config['database']
            })
            
            # Comando para importar dump
            if dump_file.suffix == '.sql':
                # Arquivo SQL texto
                cmd = [
                    'psql',
                    '-v', f'schema_name={schema_name}',
                    '-f', str(dump_file),
                    '-q'  # Modo silencioso
                ]
            else:
                # Arquivo binário (pg_dump custom format)
                cmd = [
                    'pg_restore',
                    '-n', schema_name,  # Restaurar apenas para esse schema
                    '-v',
                    str(dump_file)
                ]
            
            # Executar importação
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=3600  # Timeout de 1 hora
            )
            
            if result.returncode == 0:
                print(f"✅ {dump_file.name} importado com sucesso!")
                return True
            else:
                print(f"❌ Erro na importação: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"❌ Erro ao importar {dump_file.name}: {e}")
            return False
    
    def register_dump_metadata(self, dump_file: Path, schema_name: str, 
                             success: bool, tables_count: int = 0, 
                             records_count: int = 0) -> None:
        """Registra metadados do dump importado"""
        try:
            conn = psycopg2.connect(**self.db_config)
            
            # Extrair data do arquivo
            date_str = self.extract_date_from_filename(dump_file.name)
            dump_date = None
            if date_str:
                dump_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO caf_analysis.dump_metadata 
                    (dump_file, schema_name, dump_date, file_size_mb, 
                     tables_count, records_count, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    dump_file.name,
                    schema_name,
                    dump_date,
                    self.get_file_size_mb(dump_file),
                    tables_count,
                    records_count,
                    'Importado com sucesso' if success else 'Falha na importação'
                ))
                
                conn.commit()
                
            conn.close()
            
        except Exception as e:
            print(f"⚠️  Erro ao registrar metadados: {e}")
    
    def count_schema_objects(self, schema_name: str) -> Dict[str, int]:
        """Conta tabelas e registros no schema"""
        try:
            conn = psycopg2.connect(**self.db_config)
            
            with conn.cursor() as cur:
                # Contar tabelas
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """, (schema_name,))
                tables_count = cur.fetchone()[0]
                
                # Contar registros totais (aproximado)
                cur.execute("""
                    SELECT COALESCE(SUM(n_tup_ins + n_tup_upd), 0)
                    FROM pg_stat_user_tables 
                    WHERE schemaname = %s
                """, (schema_name,))
                records_count = cur.fetchone()[0] or 0
                
            conn.close()
            
            return {
                'tables_count': tables_count,
                'records_count': records_count
            }
            
        except Exception as e:
            print(f"⚠️  Erro ao contar objetos do schema: {e}")
            return {'tables_count': 0, 'records_count': 0}
    
    def import_all_caf_dumps(self) -> None:
        """Importa todos os dumps CAF encontrados"""
        dumps = self.find_caf_dumps()
        
        if not dumps:
            print("📭 Nenhum dump CAF encontrado no diretório /dumps")
            return
        
        print(f"📁 Encontrados {len(dumps)} dumps CAF:")
        for dump in dumps:
            size_mb = self.get_file_size_mb(dump)
            print(f"  - {dump.name} ({size_mb:.1f} MB)")
        
        print("\n🚀 Iniciando importação...")
        
        for dump_file in dumps:
            # Extrair data e gerar nome do schema
            date_str = self.extract_date_from_filename(dump_file.name)
            schema_name = self.generate_schema_name(date_str)
            
            print(f"\n📋 Processando: {dump_file.name}")
            print(f"   Schema: {schema_name}")
            
            # Criar schema
            if self.create_schema(schema_name):
                # Importar dump
                success = self.import_dump(dump_file, schema_name)
                
                # Contar objetos importados
                if success:
                    stats = self.count_schema_objects(schema_name)
                    print(f"   📊 {stats['tables_count']} tabelas, ~{stats['records_count']:,} registros")
                else:
                    stats = {'tables_count': 0, 'records_count': 0}
                
                # Registrar metadados
                self.register_dump_metadata(
                    dump_file, schema_name, success,
                    stats['tables_count'], stats['records_count']
                )
            else:
                print(f"   ❌ Falha ao criar schema")
        
        print(f"\n🎉 Importação concluída!")
        
        # Mostrar resumo
        self.show_import_summary()
    
    def show_import_summary(self) -> None:
        """Mostra resumo dos dumps importados"""
        try:
            conn = psycopg2.connect(**self.db_config)
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT schema_name, dump_file, dump_date, 
                           file_size_mb, tables_count, records_count,
                           imported_at, notes
                    FROM caf_analysis.dump_metadata 
                    ORDER BY dump_date DESC, imported_at DESC
                """)
                
                rows = cur.fetchall()
                
            conn.close()
            
            if rows:
                print(f"\n📊 Resumo dos dumps importados:")
                print("-" * 80)
                
                for row in rows:
                    schema, file, date, size, tables, records, imported, notes = row
                    print(f"Schema: {schema}")
                    print(f"  Arquivo: {file}")
                    print(f"  Data: {date or 'N/A'}")
                    print(f"  Tamanho: {size:.1f} MB")
                    print(f"  Tabelas: {tables}, Registros: {records:,}")
                    print(f"  Importado: {imported}")
                    print(f"  Status: {notes}")
                    print()
                    
        except Exception as e:
            print(f"⚠️  Erro ao mostrar resumo: {e}")


def main():
    """Função principal"""
    print("🐘 Importador de Dumps CAF para PostgreSQL\n")
    
    # Configuração do banco
    db_config = {
        'host': os.getenv('PGHOST', 'localhost'),
        'port': int(os.getenv('PGPORT', '5432')),
        'user': os.getenv('PGUSER', 'caf_user'),
        'password': os.getenv('PGPASSWORD', 'caf_password123'),
        'database': os.getenv('PGDATABASE', 'caf_analysis')
    }
    
    # Aguardar banco estar pronto
    print("⏳ Aguardando PostgreSQL estar disponível...")
    import time
    time.sleep(10)
    
    # Criar importador e executar
    importer = CAFDumpImporter(db_config)
    importer.import_all_caf_dumps()


if __name__ == "__main__":
    main()

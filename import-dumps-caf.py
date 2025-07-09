#!/usr/bin/env python3
"""
Script simplificado para importar dumps CAF no PostgreSQL
Funciona dentro ou fora do container
"""

import os
import re
import subprocess
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import List, Optional


def find_caf_dumps(dumps_dir: str = "/dumps") -> List[Path]:
    """Encontra todos os dumps CAF"""
    dumps_path = Path(dumps_dir)
    if not dumps_path.exists():
        dumps_path = Path("dumps")  # Fallback para execução local
    
    if not dumps_path.exists():
        return []
    
    # Buscar arquivos que contenham "caf" no nome (evitar duplicatas)
    patterns = ["*caf*.sql", "*caf*.dump", "*CAF*.sql", "*CAF*.dump"]
    caf_dumps_set = set()  # Usar set para evitar duplicatas
    
    for pattern in patterns:
        for file_path in dumps_path.glob(pattern):
            # Ignorar arquivos temporários
            if not file_path.name.startswith('temp_'):
                caf_dumps_set.add(file_path)
    
    # Converter de volta para lista e ordenar
    caf_dumps = list(caf_dumps_set)
    return sorted(caf_dumps, key=lambda x: x.name)


def extract_date_from_filename(filename: str) -> Optional[str]:
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


def generate_schema_name(date_str: Optional[str]) -> str:
    """Gera nome do schema baseado na data"""
    if date_str:
        return f"caf_{date_str}"
    else:
        return f"caf_{datetime.now().strftime('%Y%m%d')}"


def create_schema(cursor, schema_name: str) -> bool:
    """Cria schema no banco"""
    try:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        
        # Adicionar comentário
        date_part = schema_name.replace('caf_', '')
        if len(date_part) == 8:
            formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
            comment = f"Schema para dump CAF de {formatted_date}"
        else:
            comment = f"Schema para dump CAF ({schema_name})"
        
        cursor.execute(f"COMMENT ON SCHEMA \"{schema_name}\" IS '{comment}'")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao criar schema {schema_name}: {e}")
        return False


def import_dump_sql(dump_file: Path, schema_name: str) -> bool:
    """Importa dump SQL modificando o schema"""
    try:
        print(f"📥 Processando {dump_file.name}...")
        
        # Ler e modificar o conteúdo do dump
        with open(dump_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Substituir referências ao schema público pelo novo schema
        content = content.replace('public.', f'"{schema_name}".')
        content = content.replace('SCHEMA public', f'SCHEMA "{schema_name}"')
        
        # Adicionar comando para usar o schema
        content = f'SET search_path TO "{schema_name}", public;\n\n' + content
        
        # Salvar arquivo temporário
        temp_file = dump_file.parent / f"temp_{schema_name}_{dump_file.name}"
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Importar usando psql
        env = os.environ.copy()
        env.update({
            'PGPASSWORD': 'caf_password123',
            'PGUSER': 'caf_user',
            'PGHOST': 'localhost',
            'PGPORT': '5433',
            'PGDATABASE': 'caf_analysis'
        })
        
        cmd = [
            'psql',
            '-f', str(temp_file),
            '-q'
        ]
        
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutos
        )
        
        # Remover arquivo temporário
        temp_file.unlink(missing_ok=True)
        
        if result.returncode == 0:
            print(f"✅ {dump_file.name} importado para schema {schema_name}")
            return True
        else:
            print(f"❌ Erro na importação: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao importar {dump_file.name}: {e}")
        return False


def register_dump_metadata(cursor, dump_file: Path, schema_name: str, success: bool):
    """Registra metadados do dump"""
    try:
        date_str = extract_date_from_filename(dump_file.name)
        dump_date = None
        if date_str:
            dump_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        file_size_mb = dump_file.stat().st_size / (1024 * 1024)
        
        cursor.execute("""
            INSERT INTO caf_analysis.dump_metadata 
            (dump_file, schema_name, dump_date, file_size_mb, notes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dump_file, schema_name) DO UPDATE SET
                imported_at = NOW(),
                notes = EXCLUDED.notes
        """, (
            dump_file.name,
            schema_name,
            dump_date,
            file_size_mb,
            'Importado com sucesso' if success else 'Falha na importação'
        ))
        
    except Exception as e:
        print(f"⚠️  Erro ao registrar metadados: {e}")


def main():
    """Função principal"""
    print("🐘 Importador de Dumps CAF para PostgreSQL\n")
    
    # Verificar se estamos dentro do container ou local
    is_container = os.path.exists('/dumps')
    dumps_dir = '/dumps' if is_container else 'dumps'
    
    print(f"📁 Buscando dumps em: {dumps_dir}")
    
    # Encontrar dumps CAF
    dumps = find_caf_dumps(dumps_dir)
    
    if not dumps:
        print("📭 Nenhum dump CAF encontrado")
        return
    
    print(f"📁 Encontrados {len(dumps)} dumps CAF:")
    for dump in dumps:
        size_mb = dump.stat().st_size / (1024 * 1024)
        print(f"  - {dump.name} ({size_mb:.1f} MB)")
    
    # Conectar ao banco
    try:
        conn = psycopg2.connect(
            host='localhost' if not is_container else 'postgres-caf',
            port=5433 if not is_container else 5432,
            user='caf_user',
            password='caf_password123',
            database='caf_analysis'
        )
        conn.autocommit = True
        
        print("\n✅ Conectado ao PostgreSQL")
        
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return
    
    # Processar cada dump
    print(f"\n🚀 Iniciando importação...")
    
    processed_schemas = set()  # Para evitar processar o mesmo schema duas vezes
    
    with conn.cursor() as cursor:
        for dump_file in dumps:
            # Extrair data e gerar nome do schema
            date_str = extract_date_from_filename(dump_file.name)
            schema_name = generate_schema_name(date_str)
            
            # Verificar se já processamos este schema
            if schema_name in processed_schemas:
                print(f"\n⚠️  Schema {schema_name} já foi processado, pulando {dump_file.name}")
                continue
            
            # Verificar se o dump já foi importado antes
            cursor.execute("""
                SELECT COUNT(*) FROM caf_analysis.dump_metadata 
                WHERE dump_file = %s AND schema_name = %s AND notes LIKE '%%sucesso%%'
            """, (dump_file.name, schema_name))
            
            if cursor.fetchone()[0] > 0:
                print(f"\n✅ {dump_file.name} já foi importado para {schema_name}")
                processed_schemas.add(schema_name)
                continue
            
            print(f"\n📋 Processando: {dump_file.name}")
            print(f"   Schema: {schema_name}")
            
            processed_schemas.add(schema_name)
            
            # Criar schema
            if create_schema(cursor, schema_name):
                # Importar dump
                success = import_dump_sql(dump_file, schema_name)
                
                # Registrar metadados
                register_dump_metadata(cursor, dump_file, schema_name, success)
            else:
                print(f"   ❌ Falha ao criar schema")
    
    conn.close()
    
    print(f"\n🎉 Importação concluída!")
    
    # Mostrar resumo
    show_summary()


def show_summary():
    """Mostra resumo dos dumps importados"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5433,
            user='caf_user',
            password='caf_password123',
            database='caf_analysis'
        )
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT schema_name, dump_file, dump_date, 
                       file_size_mb, imported_at, notes
                FROM caf_analysis.dump_metadata 
                ORDER BY dump_date DESC, imported_at DESC
            """)
            
            rows = cursor.fetchall()
        
        conn.close()
        
        if rows:
            print(f"\n📊 Resumo dos dumps importados:")
            print("-" * 70)
            
            for row in rows:
                schema, file, date, size, imported, notes = row
                print(f"Schema: {schema}")
                print(f"  Arquivo: {file}")
                print(f"  Data: {date or 'N/A'}")
                print(f"  Tamanho: {size:.1f} MB")
                print(f"  Status: {notes}")
                print()
                
    except Exception as e:
        print(f"⚠️  Erro ao mostrar resumo: {e}")


if __name__ == "__main__":
    main()

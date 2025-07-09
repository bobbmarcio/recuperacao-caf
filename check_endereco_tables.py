#!/usr/bin/env python3
"""
Verificar tabelas de endereço
"""

import psycopg2

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

def check_endereco_tables():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Verificar tabelas com ENDERECO
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'caf_20250301'
        AND table_name LIKE '%ENDERECO%'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print("Tabelas com ENDERECO:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Verificar tabelas com MUNICIPIO
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'caf_20250301'
        AND table_name LIKE '%MUNICIPIO%'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print("\nTabelas com MUNICIPIO:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Verificar tabelas com UF
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'caf_20250301'
        AND table_name LIKE '%UF%'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print("\nTabelas com UF:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Verificar algumas linhas da S_UNIDADE_FAMILIAR para ver se tem campos de endereço
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'caf_20250301'
        AND table_name = 'S_UNIDADE_FAMILIAR'
        AND column_name LIKE '%endereco%'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    print("\nColunas relacionadas a endereço em S_UNIDADE_FAMILIAR:")
    for col, dtype in columns:
        print(f"  - {col}: {dtype}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_endereco_tables()

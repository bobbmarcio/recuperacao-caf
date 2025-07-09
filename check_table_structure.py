#!/usr/bin/env python3
"""
Verificar estrutura das tabelas CAF
"""

import psycopg2

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

def check_table_structure():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Verificar tabelas disponíveis no schema
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'caf_20250301'
        AND table_name LIKE '%SITUACAO%'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print("Tabelas com SITUACAO:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Verificar colunas da tabela S_TIPO_SITUACAO_UNIDADE_FAMILIAR
    try:
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'caf_20250301'
            AND table_name = 'S_TIPO_SITUACAO_UNIDADE_FAMILIAR'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("\nColunas de S_TIPO_SITUACAO_UNIDADE_FAMILIAR:")
        for col, dtype in columns:
            print(f"  - {col}: {dtype}")
    
    except Exception as e:
        print(f"Erro: {e}")
    
    # Verificar tabelas de terreno
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'caf_20250301'
        AND table_name LIKE '%TERRENO%'
        ORDER BY table_name
    """)
    
    tables = cursor.fetchall()
    print("\nTabelas com TERRENO:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Verificar colunas da tabela S_TIPO_TERRENO_UFPR
    try:
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'caf_20250301'
            AND table_name = 'S_TIPO_TERRENO_UFPR'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("\nColunas de S_TIPO_TERRENO_UFPR:")
        for col, dtype in columns:
            print(f"  - {col}: {dtype}")
    
    except Exception as e:
        print(f"Erro: {e}")
    
    # Verificar colunas da tabela S_CARACTERIZACAO_AREA
    try:
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'caf_20250301'
            AND table_name = 'S_CARACTERIZACAO_AREA'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("\nColunas de S_CARACTERIZACAO_AREA:")
        for col, dtype in columns:
            print(f"  - {col}: {dtype}")
    
    except Exception as e:
        print(f"Erro: {e}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_table_structure()

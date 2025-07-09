#!/usr/bin/env python3
"""
Verificar tabela de enquadramento
"""

import psycopg2

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

def check_enquadramento_table():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Verificar colunas da tabela S_TIPO_ENQUADRAMENTO_RENDA
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'caf_20250301'
        AND table_name = 'S_TIPO_ENQUADRAMENTO_RENDA'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    print("Colunas de S_TIPO_ENQUADRAMENTO_RENDA:")
    for col, dtype in columns:
        print(f"  - {col}: {dtype}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_enquadramento_table()

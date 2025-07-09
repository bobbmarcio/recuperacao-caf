#!/usr/bin/env python3
"""
Contar total de alterações entre schemas
"""

import psycopg2

POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

def count_total_changes():
    """Conta total de alterações entre schemas"""
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    schema1 = 'caf_20250301'
    schema2 = 'caf_20250401'
    table_name = 'S_UNIDADE_FAMILIAR'
    primary_key = 'id_unidade_familiar'
    
    # Contar total de alterações
    query = f"""
    SELECT COUNT(*) as total_changes
    FROM (
        SELECT 
            COALESCE(t1."{primary_key}", t2."{primary_key}") as id
        FROM "{schema1}"."{table_name}" t1
        FULL OUTER JOIN "{schema2}"."{table_name}" t2 
            ON t1."{primary_key}" = t2."{primary_key}"
        WHERE 
            t1."{primary_key}" IS NULL OR 
            t2."{primary_key}" IS NULL OR
            (t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao")
    ) changes
    """
    
    cursor.execute(query)
    total_changes = cursor.fetchone()[0]
    
    print(f"📊 Total de alterações entre {schema1} → {schema2}: {total_changes:,}")
    
    # Contar por tipo de alteração
    query_details = f"""
    SELECT 
        CASE 
            WHEN t1."{primary_key}" IS NULL THEN 'INSERT'
            WHEN t2."{primary_key}" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type,
        COUNT(*) as count
    FROM "{schema1}"."{table_name}" t1
    FULL OUTER JOIN "{schema2}"."{table_name}" t2 
        ON t1."{primary_key}" = t2."{primary_key}"
    WHERE 
        t1."{primary_key}" IS NULL OR 
        t2."{primary_key}" IS NULL OR
        (t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao")
    GROUP BY 
        CASE 
            WHEN t1."{primary_key}" IS NULL THEN 'INSERT'
            WHEN t2."{primary_key}" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END
    ORDER BY count DESC
    """
    
    cursor.execute(query_details)
    details = cursor.fetchall()
    
    print("\n📈 Detalhamento por tipo:")
    for change_type, count in details:
        print(f"  {change_type}: {count:,}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    count_total_changes()

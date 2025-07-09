#!/usr/bin/env python3
"""
Teste simples para verificar análise incremental entre schemas CAF
"""

import psycopg2
import pandas as pd
from datetime import datetime
import json

# Configurações do banco
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

def test_schema_comparison():
    """Testa comparação entre os dois schemas CAF"""
    
    print("🔍 Testando análise incremental entre schemas CAF")
    print("=" * 60)
    
    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Verificar schemas disponíveis
        print("📊 Schemas CAF disponíveis:")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT schemaname, COUNT(*) as table_count
            FROM pg_tables 
            WHERE schemaname LIKE 'caf_2025%' 
            GROUP BY schemaname 
            ORDER BY schemaname
        """)
        
        schemas = cursor.fetchall()
        for schema, count in schemas:
            print(f"   - {schema}: {count} tabelas")
        
        if len(schemas) < 2:
            print("❌ Não há schemas suficientes para comparação")
            return
        
        schema1, schema2 = schemas[0][0], schemas[1][0]
        print(f"\n🔄 Comparando {schema1} → {schema2}")
        
        # Testar comparação na tabela S_UNIDADE_FAMILIAR
        print(f"\n📋 Analisando tabela S_UNIDADE_FAMILIAR:")
        
        # Contar registros em cada schema
        cursor.execute(f'SELECT COUNT(*) FROM "{schema1}"."S_UNIDADE_FAMILIAR"')
        count1 = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM "{schema2}"."S_UNIDADE_FAMILIAR"')
        count2 = cursor.fetchone()[0]
        
        print(f"   - {schema1}: {count1:,} registros")
        print(f"   - {schema2}: {count2:,} registros")
        print(f"   - Diferença: {count2 - count1:+,} registros")
        
        # Detectar alterações usando LEFT JOIN
        print(f"\n🔍 Detectando alterações...")
        
        query = f"""
        SELECT 
            COUNT(*) as total_changes
        FROM "{schema1}"."S_UNIDADE_FAMILIAR" uf1
        FULL OUTER JOIN "{schema2}"."S_UNIDADE_FAMILIAR" uf2 
            ON uf1.id_unidade_familiar = uf2.id_unidade_familiar
        WHERE 
            uf1.id_unidade_familiar IS NULL OR 
            uf2.id_unidade_familiar IS NULL OR
            uf1.dt_atualizacao IS DISTINCT FROM uf2.dt_atualizacao OR
            uf1.st_possui_mao_obra IS DISTINCT FROM uf2.st_possui_mao_obra OR
            uf1.dt_validade IS DISTINCT FROM uf2.dt_validade OR
            uf1.id_tipo_situacao_unidade_familiar IS DISTINCT FROM uf2.id_tipo_situacao_unidade_familiar
        """
        
        cursor.execute(query)
        changes = cursor.fetchone()[0]
        print(f"   ✅ {changes:,} alterações detectadas!")
        
        # Mostrar exemplo de alteração
        if changes > 0:
            print(f"\n📝 Exemplo de alterações (primeiras 5):")
            
            sample_query = f"""
            SELECT 
                COALESCE(uf1.id_unidade_familiar, uf2.id_unidade_familiar) as id,
                uf1.dt_atualizacao as dt_antes,
                uf2.dt_atualizacao as dt_depois,
                CASE 
                    WHEN uf1.id_unidade_familiar IS NULL THEN 'NOVO'
                    WHEN uf2.id_unidade_familiar IS NULL THEN 'REMOVIDO'
                    ELSE 'ALTERADO'
                END as tipo_mudanca
            FROM "{schema1}"."S_UNIDADE_FAMILIAR" uf1
            FULL OUTER JOIN "{schema2}"."S_UNIDADE_FAMILIAR" uf2 
                ON uf1.id_unidade_familiar = uf2.id_unidade_familiar
            WHERE 
                uf1.id_unidade_familiar IS NULL OR 
                uf2.id_unidade_familiar IS NULL OR
                uf1.dt_atualizacao IS DISTINCT FROM uf2.dt_atualizacao OR
                uf1.st_possui_mao_obra IS DISTINCT FROM uf2.st_possui_mao_obra OR
                uf1.dt_validade IS DISTINCT FROM uf2.dt_validade OR
                uf1.id_tipo_situacao_unidade_familiar IS DISTINCT FROM uf2.id_tipo_situacao_unidade_familiar
            LIMIT 5
            """
            
            cursor.execute(sample_query)
            samples = cursor.fetchall()
            
            for i, (id_uf, dt_antes, dt_depois, tipo) in enumerate(samples, 1):
                print(f"   {i}. {tipo}: {str(id_uf)[:8]}... ({dt_antes} → {dt_depois})")
        
        # Testar tabela S_PESSOA_FISICA
        print(f"\n👤 Analisando tabela S_PESSOA_FISICA:")
        
        cursor.execute(f'SELECT COUNT(*) FROM "{schema1}"."S_PESSOA_FISICA"')
        pf_count1 = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT COUNT(*) FROM "{schema2}"."S_PESSOA_FISICA"')
        pf_count2 = cursor.fetchone()[0]
        
        print(f"   - {schema1}: {pf_count1:,} pessoas")
        print(f"   - {schema2}: {pf_count2:,} pessoas")
        print(f"   - Diferença: {pf_count2 - pf_count1:+,} pessoas")
        
        cursor.close()
        conn.close()
        
        print(f"\n🎉 Teste concluído com sucesso!")
        print(f"✅ Sistema pode comparar schemas CAF adequadamente")
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_schema_comparison()

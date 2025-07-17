#!/usr/bin/env python3
"""
Script para extrair mapeamento da aba renda do arquivo ODS
"""

import pandas as pd
from pathlib import Path

def extract_renda_mapping():
    """Extrai o mapeamento da aba renda"""
    
    ods_file = 'de_para_mongo_postgres_caf.ods'
    csv_file = 'de_para_renda.csv'
    
    try:
        # Ler a aba renda
        print("📖 Lendo aba 'renda' do arquivo ODS...")
        df = pd.read_excel(ods_file, sheet_name='renda', engine='odf')
        
        print(f"✅ Carregadas {len(df)} linhas da aba renda")
        print(f"📋 Colunas encontradas: {list(df.columns)}")
        
        # Mostrar as primeiras linhas para verificar a estrutura
        print("\n📊 Primeiras 5 linhas:")
        print(df.head())
        
        # Salvar como CSV
        df.to_csv(csv_file, index=False)
        print(f"💾 Mapeamento salvo em: {csv_file}")
        
        # Estatísticas básicas
        print(f"\n📈 Estatísticas:")
        print(f"   - Total de linhas: {len(df)}")
        print(f"   - Colunas: {len(df.columns)}")
        
        # Verificar campos não nulos na primeira coluna (campos Mongo)
        if len(df.columns) > 0:
            primeira_coluna = df.columns[0]
            campos_validos = df[primeira_coluna].dropna()
            print(f"   - Campos Mongo válidos: {len(campos_validos)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao extrair mapeamento renda: {e}")
        return False

if __name__ == "__main__":
    extract_renda_mapping()

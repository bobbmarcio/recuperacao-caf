#!/usr/bin/env python3
"""
Script para extrair o mapeamento da aba endereco do arquivo ODS
"""

import pandas as pd
from pathlib import Path

def extract_endereco_mapping():
    """Extrai o mapeamento da aba endereco"""
    
    try:
        # Ler arquivo ODS - aba endereco
        file_path = "de_para_mongo_postgres_caf.ods"
        
        print("📋 Extraindo mapeamento da aba: endereco")
        print("=" * 70)
        
        # Tentar ler com pandas
        df = pd.read_excel(file_path, sheet_name='endereco', engine='odf')
        
        # Mostrar as colunas disponíveis
        print("🔍 Colunas encontradas:")
        for i, col in enumerate(df.columns):
            print(f"   {i+1}. {col}")
        
        print(f"\n📊 Total de registros: {len(df)}")
        
        # Mostrar primeiras linhas
        print(f"\n📄 Primeiras 15 linhas:")
        print(df.head(15).to_string())
        
        # Salvar como CSV
        csv_file = "de_para_endereco.csv"
        df.to_csv(csv_file, index=False)
        
        print(f"\n💾 Mapeamento salvo como: {csv_file}")
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao ler arquivo ODS: {e}")
        import traceback
        print(traceback.format_exc())
        return None

if __name__ == "__main__":
    extract_endereco_mapping()

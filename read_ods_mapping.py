#!/usr/bin/env python3
"""
Script para ler o arquivo ODS e extrair o mapeamento de campos da aba unidade_familiar
"""

import pandas as pd
from pathlib import Path

def read_ods_mapping():
    """Lê o arquivo ODS e extrai o mapeamento de campos"""
    
    try:
        # Ler arquivo ODS - aba unidade_familiar
        file_path = "de_para_mongo_postgres_caf.ods"
        
        # Tentar ler com pandas
        df = pd.read_excel(file_path, sheet_name='unidade_familiar', engine='odf')
        
        print("📋 Mapeamento PostgreSQL → MongoDB (Aba: unidade_familiar)")
        print("=" * 70)
        
        # Mostrar as colunas disponíveis
        print("🔍 Colunas encontradas:")
        for i, col in enumerate(df.columns):
            print(f"   {i+1}. {col}")
        
        print(f"\n📊 Total de registros: {len(df)}")
        
        # Mostrar primeiras linhas
        print(f"\n📄 Primeiras 10 linhas:")
        print(df.head(10).to_string())
        
        return df
        
    except Exception as e:
        print(f"❌ Erro ao ler arquivo ODS: {e}")
        
        # Alternativa: pedir para o usuário converter para CSV
        print("\n💡 ALTERNATIVAS:")
        print("1. Converter o arquivo ODS para CSV e colocar na pasta")
        print("2. Exportar a aba 'unidade_familiar' como CSV")
        print("3. Fornecer o mapeamento manualmente")
        
        return None

def install_odf_support():
    """Instala suporte para arquivos ODF se necessário"""
    
    try:
        import odf
        print("✅ Suporte ODF já instalado")
    except ImportError:
        print("📦 Instalando suporte para arquivos ODF...")
        import subprocess
        subprocess.run(["pip", "install", "odfpy"], check=True)
        print("✅ Suporte ODF instalado")

if __name__ == "__main__":
    print("🔍 Lendo arquivo de mapeamento PostgreSQL → MongoDB")
    
    # Instalar suporte ODF se necessário
    install_odf_support()
    
    # Ler mapeamento
    mapping_df = read_ods_mapping()
    
    if mapping_df is not None:
        # Salvar como CSV para facilitar uso futuro
        csv_path = "de_para_unidade_familiar.csv"
        mapping_df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"\n💾 Mapeamento salvo como: {csv_path}")
    else:
        print("\n❌ Não foi possível ler o arquivo ODS")
        print("📝 Por favor, exporte a aba 'unidade_familiar' como CSV")

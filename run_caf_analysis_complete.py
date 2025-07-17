#!/usr/bin/env python3
"""
Script unificado para análise incremental CAF
Executa análise tanto para unidade_familiar quanto para unidade_familiar_pessoa
"""

import sys
import subprocess
from datetime import datetime

def run_unified_analysis(limit: int = None):
    """Executa análise completa do sistema CAF"""
    
    print("🎯 ANÁLISE INCREMENTAL CAF - SISTEMA COMPLETO")
    print("=" * 80)
    print(f"🕐 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if limit:
        print(f"🔢 LIMITE: {limit} alterações por schema")
    
    # Executar análise de unidade familiar
    print("\n" + "="*80)
    print("📊 FASE 1: ANÁLISE UNIDADE FAMILIAR")
    print("="*80)
    
    try:
        if limit:
            result1 = subprocess.run([
                'python', 'run_caf_analysis_mapped.py', str(limit)
            ], capture_output=False, text=True)
        else:
            result1 = subprocess.run([
                'python', 'run_caf_analysis_mapped.py'
            ], capture_output=False, text=True)
        
        if result1.returncode == 0:
            print("✅ Análise de unidade familiar concluída com sucesso")
        else:
            print("❌ Erro na análise de unidade familiar")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar análise de unidade familiar: {e}")
        return False
    
    # Executar análise de pessoa
    print("\n" + "="*80)
    print("👥 FASE 2: ANÁLISE UNIDADE FAMILIAR PESSOA")
    print("="*80)
    
    try:
        if limit:
            result2 = subprocess.run([
                'python', 'run_caf_analysis_pessoa.py', str(limit)
            ], capture_output=False, text=True)
        else:
            result2 = subprocess.run([
                'python', 'run_caf_analysis_pessoa.py'
            ], capture_output=False, text=True)
        
        if result2.returncode == 0:
            print("✅ Análise de unidade familiar pessoa concluída com sucesso")
        else:
            print("❌ Erro na análise de unidade familiar pessoa")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar análise de pessoa: {e}")
        return False
    
    # Executar análise de endereços
    print("\n" + "="*80)
    print("📍 FASE 3: ANÁLISE ENDEREÇOS")
    print("="*80)
    
    try:
        if limit:
            result3 = subprocess.run([
                'python', 'run_caf_analysis_endereco.py', str(limit)
            ], capture_output=False, text=True)
        else:
            result3 = subprocess.run([
                'python', 'run_caf_analysis_endereco.py'
            ], capture_output=False, text=True)
        
        if result3.returncode == 0:
            print("✅ Análise de endereços concluída com sucesso")
        else:
            print("❌ Erro na análise de endereços")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar análise de endereços: {e}")
        return False
    
    # Executar análise de área imóvel
    print("\n" + "="*80)
    print("🏠 FASE 4: ANÁLISE ÁREA IMÓVEL")
    print("="*80)
    
    try:
        if limit:
            result4 = subprocess.run([
                'python', 'run_caf_analysis_area_imovel.py', str(limit)
            ], capture_output=False, text=True)
        else:
            result4 = subprocess.run([
                'python', 'run_caf_analysis_area_imovel.py'
            ], capture_output=False, text=True)
        
        if result4.returncode == 0:
            print("✅ Análise de área imóvel concluída com sucesso")
        else:
            print("❌ Erro na análise de área imóvel")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar análise de área imóvel: {e}")
        return False
    
    # Executar análise de renda
    print("\n" + "="*80)
    print("💰 FASE 5: ANÁLISE RENDA")
    print("="*80)
    
    try:
        if limit:
            result5 = subprocess.run([
                'python', 'run_caf_analysis_renda.py', str(limit)
            ], capture_output=False, text=True)
        else:
            result5 = subprocess.run([
                'python', 'run_caf_analysis_renda.py'
            ], capture_output=False, text=True)
        
        if result5.returncode == 0:
            print("✅ Análise de renda concluída com sucesso")
        else:
            print("❌ Erro na análise de renda")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar análise de renda: {e}")
        return False
    
    # Relatório final
    print("\n" + "="*80)
    print("📋 RELATÓRIO FINAL")
    print("="*80)
    
    try:
        print("\n📊 RESUMO UNIDADE FAMILIAR:")
        subprocess.run(['python', 'verify_updates.py'], capture_output=False)
        
        print("\n👥 RESUMO UNIDADE FAMILIAR PESSOA:")
        subprocess.run(['python', 'verify_pessoa_updates.py'], capture_output=False)
        
        print("\n📍 RESUMO ENDEREÇOS:")
        subprocess.run(['python', 'verify_endereco_updates.py'], capture_output=False)
        
        print("\n🏠 RESUMO ÁREA IMÓVEL:")
        subprocess.run(['python', 'verify_area_imovel_updates.py'], capture_output=False)
        
        print("\n💰 RESUMO RENDA:")
        subprocess.run(['python', 'verify_renda_updates.py'], capture_output=False)
        
    except Exception as e:
        print(f"⚠️  Erro ao gerar relatório: {e}")
    
    print(f"\n🎉 ANÁLISE COMPLETA CONCLUÍDA!")
    print(f"🕐 Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔗 Acesse Mongo Express: http://localhost:8080")
    
    return True

def show_help():
    """Mostra ajuda de uso"""
    print("""
🎯 ANÁLISE INCREMENTAL CAF - SISTEMA COMPLETO

USAGE:
    python run_caf_analysis_complete.py [limite]

PARÂMETROS:
    limite (opcional): Número máximo de alterações por schema (padrão: ilimitado)

EXEMPLOS:
    python run_caf_analysis_complete.py        # Análise completa
    python run_caf_analysis_complete.py 10     # Máximo 10 alterações por schema

FUNÇÕES:
    - Executa análise incremental para unidade_familiar
    - Executa análise incremental para unidade_familiar_pessoa
    - Executa análise incremental para endereços
    - Executa análise incremental para área imóvel
    - Executa análise incremental para renda
    - Gera relatório unificado
    - Aplica as mesmas regras de negócio para todas as análises

COLEÇÕES MONGODB:
    - caf_unidade_familiar: Unidades familiares
    - caf_unidade_familiar_pessoa: Pessoas das unidades familiares
    - caf_endereco: Endereços
    - caf_area_imovel: Áreas de imóveis
    - caf_renda: Rendas das unidades familiares
""")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help', 'help']:
            show_help()
        else:
            try:
                limit = int(sys.argv[1])
                run_unified_analysis(limit)
            except ValueError:
                print("❌ Limite deve ser um número inteiro")
                print("Use: python run_caf_analysis_complete.py --help")
    else:
        run_unified_analysis()

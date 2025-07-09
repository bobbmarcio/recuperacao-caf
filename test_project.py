#!/usr/bin/env python
"""
Script de teste para verificar a funcionalidade básica do projeto
"""

import sys
import os
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

def test_imports():
    """Testa se todos os módulos podem ser importados"""
    print("🔧 Testando imports dos módulos...")
    
    try:
        from config import DatabaseConfig, MonitoringConfig, AppConfig, setup_logging
        print("✅ config.py - OK")
    except Exception as e:
        print(f"❌ config.py - Erro: {e}")
        return False
    
    try:
        from dump_parser import PostgreSQLDumpParser, list_dump_files
        print("✅ dump_parser.py - OK")
    except Exception as e:
        print(f"❌ dump_parser.py - Erro: {e}")
        return False
    
    try:
        from data_comparator import DataComparator
        print("✅ data_comparator.py - OK")
    except Exception as e:
        print(f"❌ data_comparator.py - Erro: {e}")
        return False
    
    try:
        from mongo_inserter import MongoAuditInserter
        print("✅ mongo_inserter.py - OK")
    except Exception as e:
        print(f"❌ mongo_inserter.py - Erro: {e}")
        return False
    
    return True

def test_config_loading():
    """Testa carregamento de configuração"""
    print("\n📋 Testando carregamento de configuração...")
    
    try:
        from config import MonitoringConfig
        
        config_path = project_root / 'config' / 'monitoring_config.yaml'
        config = MonitoringConfig.from_yaml(str(config_path))
        
        print(f"✅ Configuração carregada com {len(config.tables)} tabelas")
        
        for table_name, table_config in config.tables.items():
            print(f"  📊 {table_name}: {len(table_config.columns)} colunas monitoradas")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao carregar configuração: {e}")
        return False

def test_dump_listing():
    """Testa listagem de dumps"""
    print("\n📁 Testando listagem de dumps...")
    
    try:
        from dump_parser import list_dump_files
        
        dumps_dir = project_root / 'dumps'
        dump_files = list_dump_files(str(dumps_dir))
        
        print(f"✅ Encontrados {len(dump_files)} arquivos de dump")
        
        for dump_file in dump_files:
            file_name = Path(dump_file).name
            print(f"  📄 {file_name}")
        
        return len(dump_files) > 0
        
    except Exception as e:
        print(f"❌ Erro ao listar dumps: {e}")
        return False

def test_basic_parsing():
    """Testa parsing básico de um dump"""
    print("\n🔍 Testando parsing básico...")
    
    try:
        from dump_parser import PostgreSQLDumpParser, list_dump_files
        
        dumps_dir = project_root / 'dumps'
        dump_files = list_dump_files(str(dumps_dir))
        
        if not dump_files:
            print("⚠️  Nenhum dump encontrado para teste")
            return False
        
        parser = PostgreSQLDumpParser()
        tables_to_monitor = ['usuarios', 'produtos']
        
        first_dump = dump_files[0]
        print(f"  📄 Processando: {Path(first_dump).name}")
        
        tables_data = parser.parse_dump_file(first_dump, tables_to_monitor)
        
        print(f"✅ Parse concluído - {len(tables_data)} tabelas encontradas")
        
        for table_name, table_data in tables_data.items():
            print(f"  📊 {table_name}: {len(table_data.data)} registros")
        
        return len(tables_data) > 0
        
    except Exception as e:
        print(f"❌ Erro durante parsing: {e}")
        return False

def main():
    """Executa todos os testes"""
    print("🚀 INICIANDO TESTES DO PROJETO")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_config_loading,
        test_dump_listing,
        test_basic_parsing
    ]
    
    results = []
    
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Erro inesperado no teste: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 RESULTADOS DOS TESTES")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    print(f"✅ Passou: {passed}/{total} testes")
    
    if passed == total:
        print("🎉 Todos os testes passaram! O projeto está funcionando corretamente.")
        print("\n📚 Para usar o projeto:")
        print("1. Configure as variáveis de ambiente no arquivo .env")
        print("2. Ajuste a configuração em config/monitoring_config.yaml")
        print("3. Execute: python src/main.py analyze --config config/monitoring_config.yaml")
    else:
        print("⚠️  Alguns testes falharam. Verifique as dependências e configurações.")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())

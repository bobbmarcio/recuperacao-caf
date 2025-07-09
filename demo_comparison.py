#!/usr/bin/env python
"""
Demonstração da funcionalidade de comparação de dumps sem MongoDB
"""

import sys
import os
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

def demo_comparison():
    """Demonstra a comparação entre dois dumps"""
    print("🔍 DEMONSTRAÇÃO DE COMPARAÇÃO DE DUMPS")
    print("=" * 50)
    
    try:
        from config import MonitoringConfig, setup_logging
        from dump_parser import PostgreSQLDumpParser, list_dump_files
        from data_comparator import DataComparator
        
        # Configurar logging
        setup_logging("INFO", debug=True)
        
        # Carregar configuração
        config_path = project_root / 'config' / 'monitoring_config.yaml'
        monitoring_config = MonitoringConfig.from_yaml(str(config_path))
        
        print(f"📋 Configuração carregada - monitorando {len(monitoring_config.tables)} tabelas")
        
        # Listar dumps
        dumps_dir = project_root / 'dumps'
        dump_files = list_dump_files(str(dumps_dir))
        
        if len(dump_files) < 2:
            print("❌ Necessário pelo menos 2 dumps para comparação")
            return
        
        # Processar os dois primeiros dumps
        parser = PostgreSQLDumpParser()
        comparator = DataComparator()
        
        tables_to_monitor = list(monitoring_config.tables.keys())
        
        print(f"\n📊 Processando dump anterior: {Path(dump_files[0]).name}")
        previous_data = parser.parse_dump_file(dump_files[0], tables_to_monitor)
        
        print(f"📊 Processando dump atual: {Path(dump_files[1]).name}")
        current_data = parser.parse_dump_file(dump_files[1], tables_to_monitor)
        
        # Comparar
        print(f"\n🔍 Comparando dumps...")
        result = comparator.compare_dumps(
            previous_data,
            current_data,
            monitoring_config.tables,
            Path(dump_files[0]).name,
            Path(dump_files[1]).name
        )
        
        # Exibir resultados
        print(f"\n📈 RESULTADOS DA COMPARAÇÃO")
        print("-" * 40)
        print(f"Tabelas comparadas: {len(result.tables_compared)}")
        print(f"Total de registros: {result.total_records_compared}")
        print(f"Alterações detectadas: {len(result.changes)}")
        
        if result.changes:
            print(f"\n📝 DETALHES DAS ALTERAÇÕES:")
            print("-" * 40)
            
            for i, change in enumerate(result.changes[:10]):  # Mostrar até 10 alterações
                print(f"\n{i+1}. Tabela: {change.table_name}")
                print(f"   Registro ID: {change.primary_key_value}")
                print(f"   Coluna: {change.column_name}")
                print(f"   Valor antigo: '{change.old_value}'")
                print(f"   Valor novo: '{change.new_value}'")
                print(f"   Timestamp: {change.timestamp}")
            
            if len(result.changes) > 10:
                print(f"\n... e mais {len(result.changes) - 10} alterações")
        else:
            print("✅ Nenhuma alteração detectada entre os dumps")
        
        # Exibir relatório resumido
        print(f"\n📊 RELATÓRIO RESUMIDO")
        print("-" * 40)
        report = comparator.generate_summary_report()
        print(report)
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print("1. Configure um MongoDB para armazenar as alterações")
        print("2. Execute: python src/main.py analyze --config config/monitoring_config.yaml")
        print("3. Use: python src/main.py stats para ver estatísticas")
        print("4. Use: python src/main.py query --table usuarios para consultar alterações")
        
    except Exception as e:
        print(f"❌ Erro durante demonstração: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    demo_comparison()

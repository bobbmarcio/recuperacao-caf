#!/usr/bin/env python3
"""
Análise detalhada das diferenças entre documentos
"""

import pymongo
import json
from run_caf_analysis import get_unidade_familiar_complete_data, convert_to_mongodb_document

MONGODB_CONFIG = {
    'connection_string': 'mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db',
    'database': 'audit_db',
    'collection': 'caf_changes'
}

def analyze_differences():
    """Análise detalhada das diferenças"""
    
    print("🔬 Análise Detalhada de Diferenças")
    print("=" * 50)
    
    # Conectar ao MongoDB
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Pegar alguns documentos para análise
    docs = list(collection.find().limit(3))
    
    for i, existing_doc in enumerate(docs, 1):
        unidade_id = existing_doc['idUnidadeFamiliar']
        print(f"\n📄 Documento {i}: {unidade_id}")
        
        # Buscar dados atuais do PostgreSQL
        complete_data = get_unidade_familiar_complete_data(unidade_id, 'caf_20250401')
        if not complete_data:
            print("   ❌ Dados não encontrados no PostgreSQL")
            continue
        
        # Converter para documento MongoDB
        new_document = convert_to_mongodb_document(complete_data)
        if not new_document:
            print("   ❌ Erro ao converter documento")
            continue
        
        # Comparar valores específicos que podem estar causando diferenças
        problematic_fields = []
        
        def deep_compare(obj1, obj2, path=""):
            if type(obj1) != type(obj2):
                problematic_fields.append(f"{path}: tipo diferente ({type(obj1)} vs {type(obj2)})")
                return
            
            if isinstance(obj1, dict):
                for key in set(obj1.keys()) | set(obj2.keys()):
                    current_path = f"{path}.{key}" if path else key
                    if key in ['_id'] or current_path.endswith(('.dataCriacao', '.dataAtualizacao')):
                        continue
                    
                    if key not in obj1:
                        problematic_fields.append(f"{current_path}: ausente no primeiro objeto")
                    elif key not in obj2:
                        problematic_fields.append(f"{current_path}: ausente no segundo objeto")
                    else:
                        deep_compare(obj1[key], obj2[key], current_path)
            
            elif isinstance(obj1, list):
                if len(obj1) != len(obj2):
                    problematic_fields.append(f"{path}: tamanho diferente ({len(obj1)} vs {len(obj2)})")
                else:
                    for idx, (item1, item2) in enumerate(zip(obj1, obj2)):
                        deep_compare(item1, item2, f"{path}[{idx}]")
            
            elif obj1 != obj2:
                # Comparação especial para datas
                if hasattr(obj1, 'isoformat') and hasattr(obj2, 'isoformat'):
                    if obj1.isoformat() != obj2.isoformat():
                        problematic_fields.append(f"{path}: {obj1.isoformat()} vs {obj2.isoformat()}")
                else:
                    problematic_fields.append(f"{path}: {obj1} vs {obj2}")
        
        deep_compare(new_document, existing_doc)
        
        if problematic_fields:
            print("   🔄 Diferenças encontradas:")
            for field in problematic_fields[:5]:  # Mostrar apenas primeiras 5
                print(f"      - {field}")
            if len(problematic_fields) > 5:
                print(f"      ... e mais {len(problematic_fields) - 5} diferenças")
        else:
            print("   ✅ Nenhuma diferença encontrada")
    
    client.close()

if __name__ == "__main__":
    analyze_differences()

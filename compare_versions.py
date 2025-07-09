#!/usr/bin/env python3
"""
Script para comparar duas versões específicas de uma unidade familiar
"""

import pymongo
import sys
import json
from typing import Dict, Any

# Configurações
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar'
}

def compare_documents(doc1: Dict, doc2: Dict, prefix="") -> Dict[str, Any]:
    """Compara dois documentos e retorna as diferenças"""
    differences = {}
    
    # Todos os campos de ambos os documentos
    all_keys = set(doc1.keys()) | set(doc2.keys())
    
    for key in all_keys:
        field_name = f"{prefix}{key}" if prefix else key
        
        # Campos que existem em apenas um documento
        if key not in doc1:
            differences[field_name] = {"status": "only_in_doc2", "value": doc2[key]}
        elif key not in doc2:
            differences[field_name] = {"status": "only_in_doc1", "value": doc1[key]}
        else:
            # Campos que existem em ambos
            val1, val2 = doc1[key], doc2[key]
            
            # Se ambos são dicionários, comparar recursivamente
            if isinstance(val1, dict) and isinstance(val2, dict):
                nested_diff = compare_documents(val1, val2, f"{field_name}.")
                if nested_diff:
                    differences.update(nested_diff)
            
            # Se ambos são listas
            elif isinstance(val1, list) and isinstance(val2, list):
                if val1 != val2:
                    differences[field_name] = {
                        "status": "different", 
                        "doc1_value": val1, 
                        "doc2_value": val2
                    }
            
            # Valores simples diferentes
            elif val1 != val2:
                differences[field_name] = {
                    "status": "different",
                    "doc1_value": val1,
                    "doc2_value": val2
                }
    
    return differences

def compare_unit_versions(unidade_id: str):
    """Compara todas as versões de uma unidade familiar"""
    
    print(f"🔍 COMPARAÇÃO DETALHADA DAS VERSÕES: {unidade_id}")
    print("=" * 80)
    
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Buscar todas as versões
    versions = list(collection.find(
        {'idUnidadeFamiliar': unidade_id}
    ).sort('_versao', 1))
    
    if len(versions) < 2:
        print(f"❌ Necessário pelo menos 2 versões para comparação. Encontradas: {len(versions)}")
        client.close()
        return
    
    print(f"📊 Comparando {len(versions)} versões...")
    print()
    
    # Comparar versões consecutivas
    for i in range(len(versions) - 1):
        v1 = versions[i]
        v2 = versions[i + 1]
        
        print(f"🔸 COMPARAÇÃO: v{v1.get('_versao', 1)} → v{v2.get('_versao', 2)}")
        print(f"   Timestamp v1: {v1.get('_timestamp_versao', 'N/A')}")
        print(f"   Timestamp v2: {v2.get('_timestamp_versao', 'N/A')}")
        print(f"   Schema v1: {v1.get('_schema_origem', 'N/A')}")
        print(f"   Schema v2: {v2.get('_schema_origem', 'N/A')}")
        print()
        
        # Comparar documentos ignorando campos de controle
        v1_clean = {k: v for k, v in v1.items() if not k.startswith('_') and k != '_id'}
        v2_clean = {k: v for k, v in v2.items() if not k.startswith('_') and k != '_id'}
        
        differences = compare_documents(v1_clean, v2_clean)
        
        if differences:
            print("   📋 DIFERENÇAS ENCONTRADAS:")
            for field, diff in differences.items():
                if diff["status"] == "different":
                    print(f"      🔄 {field}:")
                    print(f"         v1: {diff['doc1_value']}")
                    print(f"         v2: {diff['doc2_value']}")
                elif diff["status"] == "only_in_doc1":
                    print(f"      ➖ {field}: apenas em v1 = {diff['value']}")
                elif diff["status"] == "only_in_doc2":
                    print(f"      ➕ {field}: apenas em v2 = {diff['value']}")
            print()
        else:
            print("   ✅ NENHUMA DIFERENÇA ENCONTRADA (exceto campos de controle)")
            print("   ⚠️  Isso indica possível duplicação ou erro na lógica de comparação")
            print()
        
        # Verificar campos de controle específicos
        print("   🔧 CAMPOS DE CONTROLE:")
        control_fields = ['_versao', '_versao_anterior', '_schema_origem', '_timestamp_versao']
        for field in control_fields:
            val1 = v1.get(field)
            val2 = v2.get(field)
            print(f"      {field}: {val1} → {val2}")
        print()
    
    # Mostrar resumo dos timestamps
    print("📅 RESUMO DOS TIMESTAMPS:")
    for i, version in enumerate(versions, 1):
        timestamp = version.get('_timestamp_versao', 'N/A')
        schema = version.get('_schema_origem', 'N/A')
        print(f"   v{i}: {timestamp} (schema: {schema})")
    
    client.close()

def main():
    if len(sys.argv) < 2:
        print("🔍 COMPARAÇÃO DE VERSÕES")
        print("=" * 30)
        print("Uso: python compare_versions.py <unidade_id>")
        print()
        print("Exemplo:")
        print("  python compare_versions.py 00008da6-ab22-4592-a88b-650c88655468")
        return
    
    unidade_id = sys.argv[1]
    compare_unit_versions(unidade_id)

if __name__ == "__main__":
    main()

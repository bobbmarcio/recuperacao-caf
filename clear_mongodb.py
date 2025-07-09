#!/usr/bin/env python3
"""
Script para limpar a coleção MongoDB de unidades familiares
"""

import pymongo

# Configurações
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar'
}

def clear_collection():
    """Limpa a coleção MongoDB"""
    
    print("🧹 LIMPANDO COLEÇÃO MONGODB")
    print("=" * 40)
    
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Contar documentos antes
    count_before = collection.count_documents({})
    print(f"📊 Documentos antes da limpeza: {count_before}")
    
    # Limpar
    result = collection.delete_many({})
    print(f"🗑️  Documentos removidos: {result.deleted_count}")
    
    # Contar depois
    count_after = collection.count_documents({})
    print(f"📊 Documentos após limpeza: {count_after}")
    
    client.close()
    print("✅ Limpeza concluída!")

if __name__ == "__main__":
    clear_collection()
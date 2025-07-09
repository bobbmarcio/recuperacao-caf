#!/usr/bin/env python3
"""
Verificação das atualizações no MongoDB
"""

import pymongo
from datetime import datetime, timedelta
from typing import List, Dict

# Configurações
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar'
}

def verify_recent_updates():
    """Verifica documentos atualizados recentemente"""
    
    print("🔍 VERIFICAÇÃO DE VERSIONAMENTO NO MONGODB")
    print("=" * 60)
    
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Estatísticas gerais
    total_docs = collection.count_documents({})
    unique_units = collection.distinct('idUnidadeFamiliar')
    
    print(f"📊 Total de documentos (todas as versões): {total_docs}")
    print(f"🏠 Unidades familiares únicas: {len(unique_units)}")
    print(f"📈 Média de versões por unidade: {total_docs / len(unique_units):.2f}")
    
    # Buscar unidades com múltiplas versões
    pipeline = [
        {'$group': {
            '_id': '$idUnidadeFamiliar',
            'count': {'$sum': 1},
            'max_version': {'$max': '$_versao'},
            'schemas': {'$addToSet': '$_schema_origem'}
        }},
        {'$match': {'count': {'$gt': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 10}
    ]
    
    versioned_units = list(collection.aggregate(pipeline))
    
    print(f"\n🔄 UNIDADES COM MÚLTIPLAS VERSÕES (top 10):")
    for unit in versioned_units:
        print(f"   📍 ID: {unit['_id']}")
        print(f"      Versões: {unit['count']} (máx: v{unit['max_version']})")
        print(f"      Schemas: {', '.join(unit['schemas'])}")
        print()
    
    # Buscar as últimas versões criadas
    latest_versions = list(collection.find({}).sort('_timestamp_versao', -1).limit(10))
    
    print("\n🆕 ÚLTIMAS 10 VERSÕES CRIADAS:")
    for i, doc in enumerate(latest_versions, 1):
        print(f"   {i}. ID: {doc['idUnidadeFamiliar']} (v{doc.get('_versao', 1)})")
        print(f"      Schema: {doc.get('_schema_origem', 'N/A')}")
        print(f"      Timestamp: {doc.get('_timestamp_versao', 'N/A')}")
        print(f"      CAF: {doc.get('caf', {}).get('numeroCaf', 'N/A')}")
        if doc.get('_versao_anterior'):
            print(f"      Versão anterior: v{doc.get('_versao_anterior')}")
        print()
    
    # Estatísticas por versão
    version_stats = list(collection.aggregate([
        {'$group': {
            '_id': '$_versao',
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id': 1}}
    ]))
    
    print("� DISTRIBUIÇÃO POR VERSÃO:")
    for stat in version_stats:
        version = stat['_id']
        count = stat['count']
        print(f"   v{version}: {count} documentos")
    
    client.close()

if __name__ == "__main__":
    verify_recent_updates()

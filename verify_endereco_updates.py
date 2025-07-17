#!/usr/bin/env python3
"""
Script para verificar atualizações no MongoDB - Endereços
"""

import pymongo
from datetime import datetime
import json

# Configuração MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_endereco'
}

def verify_endereco_updates():
    """Verifica estado das atualizações no MongoDB"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        print("🔍 VERIFICAÇÃO DE VERSIONAMENTO NO MONGODB - ENDEREÇOS")
        print("=" * 70)
        
        # Estatísticas gerais
        total_docs = collection.count_documents({})
        unique_enderecos = len(collection.distinct('idEndereco'))
        
        print(f"📊 Total de documentos (todas as versões): {total_docs}")
        print(f"📍 Endereços únicos: {unique_enderecos}")
        
        if total_docs > 0:
            avg_versions = total_docs / unique_enderecos
            print(f"📈 Média de versões por endereço: {avg_versions:.2f}")
            
            # Endereços com múltiplas versões
            pipeline = [
                {'$group': {
                    '_id': '$idEndereco',
                    'count': {'$sum': 1},
                    'max_version': {'$max': '$_versao'},
                    'schemas': {'$addToSet': '$_schema_origem'}
                }},
                {'$match': {'count': {'$gt': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            multiple_versions = list(collection.aggregate(pipeline))
            
            if multiple_versions:
                print(f"🔄 ENDEREÇOS COM MÚLTIPLAS VERSÕES (top 10):")
                for endereco in multiple_versions:
                    schemas_str = ', '.join(endereco['schemas'])
                    print(f"   📍 ID: {endereco['_id']}")
                    print(f"      Versões: {endereco['count']} (máx: v{endereco['max_version']})")
                    print(f"      Schemas: {schemas_str}")
            
            # Últimas versões criadas
            latest_versions = list(collection.find(
                {}, 
                {'idEndereco': 1, '_versao': 1, '_schema_origem': 1, 
                 '_timestamp_versao': 1, 'logradouro': 1, 'cep': 1, 'uf': 1}
            ).sort('_timestamp_versao', -1).limit(10))
            
            if latest_versions:
                print(f"🆕 ÚLTIMAS 10 VERSÕES CRIADAS:")
                for i, doc in enumerate(latest_versions, 1):
                    timestamp = doc.get('_timestamp_versao', 'N/A')
                    if isinstance(timestamp, datetime):
                        timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
                    
                    logradouro = doc.get('logradouro', 'N/A')
                    cep = doc.get('cep', 'N/A')
                    uf = doc.get('uf', 'N/A')
                    
                    print(f"   {i}. ID: {doc['idEndereco']} (v{doc['_versao']})")
                    print(f"      Schema: {doc.get('_schema_origem', 'N/A')}")
                    print(f"      Timestamp: {timestamp}")
                    print(f"      Endereço: {logradouro} - CEP {cep} - {uf}")
            
            # Distribuição por versão
            version_stats = {}
            for doc in collection.find({}, {'_versao': 1}):
                version = f"v{doc['_versao']}"
                version_stats[version] = version_stats.get(version, 0) + 1
            
            print(f"📊 DISTRIBUIÇÃO POR VERSÃO:")
            for version in sorted(version_stats.keys(), key=lambda x: int(x[1:])):
                print(f"   {version}: {version_stats[version]} documentos")
        else:
            print("⚠️  Nenhum documento encontrado na coleção")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao verificar MongoDB: {e}")

def get_endereco_version_history(endereco_id: str):
    """Obtém histórico de versões de um endereço"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        versions = list(collection.find(
            {'idEndereco': endereco_id}
        ).sort('_versao', 1))
        
        if versions:
            print(f"📋 HISTÓRICO DO ENDEREÇO: {endereco_id}")
            print("=" * 80)
            print(f"📊 Total de versões encontradas: {len(versions)}")
            
            for version in versions:
                print(f"🔸 VERSÃO {version['_versao']}")
                print(f"   Schema origem: {version.get('_schema_origem', 'N/A')}")
                print(f"   Timestamp: {version.get('_timestamp_versao', 'N/A')}")
                print(f"   Unidade Familiar: {version.get('idUnidadeFamiliar', 'N/A')}")
                print(f"   Logradouro: {version.get('logradouro', 'N/A')}")
                print(f"   Número: {version.get('numero', 'N/A')}")
                print(f"   CEP: {version.get('cep', 'N/A')}")
                print(f"   UF: {version.get('uf', 'N/A')}")
                
                municipio = version.get('municipio', {})
                if municipio:
                    print(f"   Município: {municipio.get('nome', 'N/A')} ({municipio.get('siglaUf', 'N/A')})")
                
                print(f"   Documento MongoDB ID: {version['_id']}")
                print()
        else:
            print(f"❌ Nenhuma versão encontrada para endereço: {endereco_id}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao buscar histórico: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        endereco_id = sys.argv[1]
        get_endereco_version_history(endereco_id)
    else:
        verify_endereco_updates()

#!/usr/bin/env python3
"""
Script para verificar atualizações no MongoDB - Unidade Familiar Pessoa
"""

import pymongo
from datetime import datetime
import json

# Configuração MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar_pessoa'
}

def verify_pessoa_updates():
    """Verifica estado das atualizações no MongoDB"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        print("🔍 VERIFICAÇÃO DE VERSIONAMENTO NO MONGODB - UNIDADE FAMILIAR PESSOA")
        print("=" * 80)
        
        # Estatísticas gerais
        total_docs = collection.count_documents({})
        unique_pessoas = len(collection.distinct('idMembroFamiliar'))
        
        print(f"📊 Total de documentos (todas as versões): {total_docs}")
        print(f"👥 Pessoas únicas: {unique_pessoas}")
        
        if total_docs > 0:
            avg_versions = total_docs / unique_pessoas
            print(f"📈 Média de versões por pessoa: {avg_versions:.2f}")
            
            # Pessoas com múltiplas versões
            pipeline = [
                {'$group': {
                    '_id': '$idMembroFamiliar',
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
                print(f"🔄 PESSOAS COM MÚLTIPLAS VERSÕES (top 10):")
                for pessoa in multiple_versions:
                    schemas_str = ', '.join(pessoa['schemas'])
                    print(f"   📍 ID: {pessoa['_id']}")
                    print(f"      Versões: {pessoa['count']} (máx: v{pessoa['max_version']})")
                    print(f"      Schemas: {schemas_str}")
            
            # Últimas versões criadas
            latest_versions = list(collection.find(
                {}, 
                {'idMembroFamiliar': 1, '_versao': 1, '_schema_origem': 1, 
                 '_timestamp_versao': 1, 'pessoaFisica.nome': 1, 'pessoaFisica.cpf': 1}
            ).sort('_timestamp_versao', -1).limit(10))
            
            if latest_versions:
                print(f"🆕 ÚLTIMAS 10 VERSÕES CRIADAS:")
                for i, doc in enumerate(latest_versions, 1):
                    timestamp = doc.get('_timestamp_versao', 'N/A')
                    if isinstance(timestamp, datetime):
                        timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
                    
                    nome = doc.get('pessoaFisica', {}).get('nome', 'N/A')
                    cpf = doc.get('pessoaFisica', {}).get('cpf', 'N/A')
                    
                    print(f"   {i}. ID: {doc['idMembroFamiliar']} (v{doc['_versao']})")
                    print(f"      Schema: {doc.get('_schema_origem', 'N/A')}")
                    print(f"      Timestamp: {timestamp}")
                    print(f"      Nome: {nome}")
                    print(f"      CPF: {cpf}")
            
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

def get_pessoa_version_history(pessoa_id: str):
    """Obtém histórico de versões de uma pessoa"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        versions = list(collection.find(
            {'idMembroFamiliar': pessoa_id}
        ).sort('_versao', 1))
        
        if versions:
            print(f"📋 HISTÓRICO DA PESSOA: {pessoa_id}")
            print("=" * 80)
            print(f"📊 Total de versões encontradas: {len(versions)}")
            
            for version in versions:
                print(f"🔸 VERSÃO {version['_versao']}")
                print(f"   Schema origem: {version.get('_schema_origem', 'N/A')}")
                print(f"   Timestamp: {version.get('_timestamp_versao', 'N/A')}")
                print(f"   Unidade Familiar: {version.get('idUnidadeFamiliar', 'N/A')}")
                print(f"   Excluído: {version.get('excluido', False)}")
                
                pessoa_fisica = version.get('pessoaFisica', {})
                if pessoa_fisica:
                    print(f"   Nome: {pessoa_fisica.get('nome', 'N/A')}")
                    print(f"   CPF: {pessoa_fisica.get('cpf', 'N/A')}")
                    print(f"   UF: {pessoa_fisica.get('uf', 'N/A')}")
                
                print(f"   Documento MongoDB ID: {version['_id']}")
                print()
        else:
            print(f"❌ Nenhuma versão encontrada para pessoa: {pessoa_id}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao buscar histórico: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        pessoa_id = sys.argv[1]
        get_pessoa_version_history(pessoa_id)
    else:
        verify_pessoa_updates()

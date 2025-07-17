#!/usr/bin/env python3
"""
Script para verificar atualizações na coleção area_imovel do MongoDB
"""

import pymongo
from datetime import datetime
import json

# Configuração MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_area_imovel'
}

def verify_area_imovel_updates():
    """Verifica o estado da coleção area_imovel"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        print("🏠 VERIFICAÇÃO DA COLEÇÃO ÁREA IMÓVEL")
        print("=" * 50)
        
        # Contagem total
        total_count = collection.count_documents({})
        print(f"📊 Total de documentos: {total_count}")
        
        if total_count == 0:
            print("ℹ️  Coleção está vazia")
            return
        
        # Estatísticas por versão
        print("\n📈 ESTATÍSTICAS POR VERSÃO:")
        pipeline = [
            {"$group": {"_id": "$_versao", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        version_stats = list(collection.aggregate(pipeline))
        for stat in version_stats:
            version = stat['_id']
            count = stat['count']
            print(f"   v{version}: {count} documentos")
        
        # Estatísticas por schema origem
        print("\n📋 ESTATÍSTICAS POR SCHEMA:")
        pipeline = [
            {"$group": {"_id": "$_schema_origem", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        schema_stats = list(collection.aggregate(pipeline))
        for stat in schema_stats:
            schema = stat['_id']
            count = stat['count']
            print(f"   {schema}: {count} documentos")
        
        # Últimas atualizações
        print("\n🕐 ÚLTIMAS ATUALIZAÇÕES:")
        recent = collection.find().sort("_timestamp_criacao", -1).limit(5)
        
        for i, doc in enumerate(recent, 1):
            area_id = doc.get('_id', 'N/A')
            versao = doc.get('_versao', 'N/A')
            schema = doc.get('_schema_origem', 'N/A')
            timestamp = doc.get('_timestamp_criacao', 'N/A')
            area = doc.get('area', 'N/A')
            uf = doc.get('uf', 'N/A')
            
            print(f"   {i}. ID: {area_id} (v{versao})")
            print(f"      Schema: {schema}")
            print(f"      Timestamp: {timestamp}")
            print(f"      Área: {area} - UF: {uf}")
        
        # Distribuição por versão (resumo)
        print("\n📊 DISTRIBUIÇÃO POR VERSÃO:")
        for stat in version_stats:
            version = stat['_id']
            count = stat['count']
            print(f"   v{version}: {count} documentos")
        
        # Campos mais comuns
        print("\n🏷️  CAMPOS MAIS PRESENTES:")
        sample_doc = collection.find_one()
        if sample_doc:
            fields = list(sample_doc.keys())
            # Remover campos técnicos para contagem
            data_fields = [f for f in fields if not f.startswith('_')]
            print(f"   Campos de dados: {len(data_fields)}")
            print(f"   Campos técnicos: {len(fields) - len(data_fields)}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao verificar coleção: {e}")

if __name__ == "__main__":
    verify_area_imovel_updates()

#!/usr/bin/env python3
"""
Script para visualizar dados salvos no MongoDB
"""

import pymongo
import json
from datetime import datetime
from typing import Dict, List, Any

# Configuração MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db',
    'database': 'audit_db',
    'collection': 'caf_changes'
}

def connect_mongodb():
    """Conecta ao MongoDB"""
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        return client, collection
    except Exception as e:
        print(f"❌ Erro ao conectar MongoDB: {e}")
        return None, None

def show_collection_stats():
    """Mostra estatísticas da coleção"""
    client, collection = connect_mongodb()
    if collection is None:
        return
    
    print("📊 ESTATÍSTICAS DA COLEÇÃO")
    print("=" * 50)
    
    # Contagem total
    total_docs = collection.count_documents({})
    print(f"📄 Total de documentos: {total_docs:,}")
    
    if total_docs == 0:
        print("ℹ️  Nenhum documento encontrado na coleção")
        client.close()
        return
    
    # Contagem por tipo de alteração
    print(f"\n📈 Alterações por tipo:")
    pipeline_types = [
        {"$group": {"_id": "$change_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    for result in collection.aggregate(pipeline_types):
        print(f"   {result['_id']}: {result['count']:,}")
    
    # Contagem por tabela
    print(f"\n📋 Alterações por tabela:")
    pipeline_tables = [
        {"$group": {"_id": "$table_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    for result in collection.aggregate(pipeline_tables):
        print(f"   {result['_id']}: {result['count']:,}")
    
    # Período dos dados
    print(f"\n📅 Período dos dados:")
    oldest = collection.find_one(sort=[("timestamp", 1)])
    newest = collection.find_one(sort=[("timestamp", -1)])
    
    if oldest and newest:
        print(f"   Mais antigo: {oldest['timestamp']}")
        print(f"   Mais recente: {newest['timestamp']}")
    
    client.close()

def show_sample_documents(limit=5):
    """Mostra documentos de exemplo"""
    client, collection = connect_mongodb()
    if collection is None:
        return
    
    print(f"\n📋 DOCUMENTOS DE EXEMPLO (últimos {limit})")
    print("=" * 60)
    
    # Buscar documentos mais recentes
    docs = collection.find().sort("timestamp", -1).limit(limit)
    
    for i, doc in enumerate(docs, 1):
        print(f"\n--- Documento {i} ---")
        print(f"🆔 ID: {doc.get('_id')}")
        print(f"📋 Tabela: {doc.get('table_name')}")
        print(f"🔗 Registro ID: {doc.get('record_id')}")
        print(f"🔄 Tipo: {doc.get('change_type')}")
        print(f"📊 Schema: {doc.get('schema_from')} → {doc.get('schema_to')}")
        print(f"⏰ Timestamp: {doc.get('timestamp')}")
        
        # Mostrar algumas alterações
        old_values = doc.get('old_values', {})
        new_values = doc.get('new_values', {})
        
        if old_values or new_values:
            print(f"📝 Alterações:")
            # Mostrar apenas primeiros campos para não poluir
            count = 0
            for key in set(list(old_values.keys()) + list(new_values.keys())):
                if count >= 3:  # Limitar a 3 campos
                    remaining = len(set(list(old_values.keys()) + list(new_values.keys()))) - 3
                    if remaining > 0:
                        print(f"      ... e mais {remaining} campos")
                    break
                
                old_val = old_values.get(key, 'N/A')
                new_val = new_values.get(key, 'N/A')
                print(f"      {key}: {old_val} → {new_val}")
                count += 1
    
    client.close()

def show_table_details(table_name=None):
    """Mostra detalhes de uma tabela específica"""
    client, collection = connect_mongodb()
    if collection is None:
        return
    
    if not table_name:
        # Listar tabelas disponíveis
        print(f"\n📋 TABELAS DISPONÍVEIS")
        print("=" * 30)
        
        pipeline = [
            {"$group": {"_id": "$table_name", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        tables = list(collection.aggregate(pipeline))
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table['_id']} ({table['count']:,} alterações)")
        
        client.close()
        return tables
    
    print(f"\n📊 DETALHES DA TABELA: {table_name}")
    print("=" * 50)
    
    # Estatísticas da tabela
    total = collection.count_documents({"table_name": table_name})
    print(f"📄 Total de alterações: {total:,}")
    
    # Por tipo de mudança
    pipeline_types = [
        {"$match": {"table_name": table_name}},
        {"$group": {"_id": "$change_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    print(f"\n📈 Por tipo de alteração:")
    for result in collection.aggregate(pipeline_types):
        print(f"   {result['_id']}: {result['count']:,}")
    
    # Exemplos específicos
    print(f"\n📋 Exemplos de alterações:")
    docs = collection.find({"table_name": table_name}).limit(3)
    
    for i, doc in enumerate(docs, 1):
        print(f"\n   Exemplo {i}:")
        print(f"   🔗 ID: {doc.get('record_id')}")
        print(f"   🔄 Tipo: {doc.get('change_type')}")
        
        # Mostrar campos alterados
        old_values = doc.get('old_values', {})
        new_values = doc.get('new_values', {})
        
        changed_fields = set(old_values.keys()) | set(new_values.keys())
        if changed_fields:
            print(f"   📝 Campos alterados: {', '.join(list(changed_fields)[:5])}")
            if len(changed_fields) > 5:
                print(f"       ... e mais {len(changed_fields) - 5} campos")
    
    client.close()

def show_unidade_familiar_structure():
    """Mostra estrutura específica para S_UNIDADE_FAMILIAR"""
    client, collection = connect_mongodb()
    if collection is None:
        return
    
    print(f"\n🏠 ESTRUTURA S_UNIDADE_FAMILIAR")
    print("=" * 50)
    
    # Buscar um documento de exemplo
    doc = collection.find_one({"table_name": "S_UNIDADE_FAMILIAR"})
    
    if not doc:
        print("❌ Nenhum documento S_UNIDADE_FAMILIAR encontrado")
        client.close()
        return
    
    print(f"📋 Documento de exemplo:")
    print(f"🆔 MongoDB ID: {doc.get('_id')}")
    print(f"🔗 Unidade Familiar ID: {doc.get('record_id')}")
    print(f"🔄 Tipo de alteração: {doc.get('change_type')}")
    print(f"📊 Schemas: {doc.get('schema_from')} → {doc.get('schema_to')}")
    
    # Mostrar mapeamento esperado vs atual
    print(f"\n📝 Campos monitorados:")
    
    old_values = doc.get('old_values', {})
    new_values = doc.get('new_values', {})
    
    # Campos conforme configuração
    expected_fields = [
        'st_possui_mao_obra',          # → possuiMaoObraContratada
        'dt_validade',                 # → dataValidade
        'dt_atualizacao',              # → dataAtualizacao
        'id_tipo_situacao_unidade_familiar',  # → tipoSituacao
        'dt_primeira_ativacao',        # → dataPrimeiraAtivacao
        'dt_bloqueio',                 # → dataBloqueio
        'dt_inativacao',               # → dataInativacao
        'ds_inativacao',               # → descricaoInativacao
        'id_tipo_terreno_ufpr',        # → tipoTerreno
        'id_caracterizacao_area',      # → caracterizacaoArea
        'dt_ativacao',                 # → dataAtivacao
        'st_migrada_caf_2',            # → migradaCaf2
        'st_possui_versao_caf3',       # → possuiVersaoCaf3
        'st_migrada_incra'             # → migradaIncra
    ]
    
    for field in expected_fields:
        old_val = old_values.get(field, 'N/A')
        new_val = new_values.get(field, 'N/A')
        
        # Indicar se campo foi alterado
        changed = "🔄" if old_val != new_val else "➖"
        print(f"   {changed} {field}: {old_val} → {new_val}")
    
    # Mostrar estrutura JSON completa de um documento
    print(f"\n🔍 ESTRUTURA JSON COMPLETA:")
    print("-" * 30)
    
    # Criar cópia sem _id para melhor visualização
    display_doc = dict(doc)
    if '_id' in display_doc:
        display_doc['_id'] = str(display_doc['_id'])
    
    print(json.dumps(display_doc, indent=2, default=str, ensure_ascii=False))
    
    client.close()

def main():
    """Função principal"""
    print("🔍 VISUALIZAÇÃO DOS DADOS MONGODB - CAF")
    print("=" * 60)
    
    # 1. Estatísticas gerais
    show_collection_stats()
    
    # 2. Documentos de exemplo
    show_sample_documents(3)
    
    # 3. Detalhes das tabelas
    tables = show_table_details()
    
    # 4. Foco na S_UNIDADE_FAMILIAR
    show_unidade_familiar_structure()
    
    # 5. Menu interativo
    print(f"\n🎯 OPÇÕES ADICIONAIS:")
    print("1. Ver mais detalhes de uma tabela específica")
    print("2. Ver mais documentos de exemplo")
    print("3. Acessar Mongo Express: http://localhost:8080")
    
    print(f"\n✅ Dados disponíveis no MongoDB!")
    print(f"📊 Database: {MONGODB_CONFIG['database']}")
    print(f"📋 Collection: {MONGODB_CONFIG['collection']}")

if __name__ == "__main__":
    main()

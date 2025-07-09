#!/usr/bin/env python3
"""
Teste de conexão MongoDB
"""

import pymongo
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

connection_string = os.getenv('MONGODB_CONNECTION_STRING')
print(f"String de conexão: {connection_string}")

try:
    print("🔗 Conectando ao MongoDB...")
    client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    
    print("🏓 Testando ping...")
    client.admin.command('ping')
    print("✅ Ping bem-sucedido!")
    
    print("📊 Acessando database...")
    db = client['audit_db']
    
    print("📄 Acessando coleção...")
    collection = db['data_changes']
    
    print("🔍 Testando inserção...")
    test_doc = {
        'table_name': 'test',
        'primary_key_value': '1',
        'column_name': 'test_col',
        'old_value': 'old',
        'new_value': 'new',
        'change_timestamp': '2024-01-01T00:00:00',
        'dump_source': 'test_source',
        'dump_target': 'test_target'
    }
    
    result = collection.insert_one(test_doc)
    print(f"✅ Documento inserido com ID: {result.inserted_id}")
    
    print("🗑️ Removendo documento de teste...")
    collection.delete_one({'_id': result.inserted_id})
    print("✅ Documento removido!")
    
    client.close()
    print("✅ Teste de conexão bem-sucedido!")
    
except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()

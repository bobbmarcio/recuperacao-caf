#!/usr/bin/env python3
"""
Visualizar dados salvos no MongoDB para verificar estrutura
"""

import pymongo
import json
from datetime import datetime

MONGODB_CONFIG = {
    'connection_string': 'mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db',
    'database': 'audit_db',
    'collection': 'caf_changes'
}

def view_mongodb_data():
    """Visualizar dados salvos no MongoDB"""
    
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        # Contar documentos
        total_docs = collection.count_documents({})
        print(f"📊 Total de documentos na coleção: {total_docs:,}")
        
        if total_docs > 0:
            # Buscar alguns documentos recentes
            recent_docs = collection.find().sort("_id", -1).limit(3)
            
            print("\n🔍 Estrutura dos últimos documentos salvos:")
            print("=" * 80)
            
            for i, doc in enumerate(recent_docs, 1):
                print(f"\n📄 Documento {i}:")
                print("-" * 40)
                
                # Converter ObjectId para string para exibição
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                # Converter datetime para string
                def convert_dates(obj):
                    if isinstance(obj, dict):
                        return {k: convert_dates(v) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        return [convert_dates(item) for item in obj]
                    elif isinstance(obj, datetime):
                        return obj.isoformat()
                    else:
                        return obj
                
                doc_clean = convert_dates(doc)
                
                # Mostrar estrutura do documento
                print(json.dumps(doc_clean, indent=2, ensure_ascii=False))
                
                # Verificar campos principais
                required_fields = [
                    '_versao', 'idUnidadeFamiliar', 'possuiMaoObraContratada', 
                    'dataValidade', 'dataCriacao', 'tipoTerreno', 'caracterizacaoArea',
                    'tipoSituacao', 'caf', 'entidadeEmissora', 'enderecoPessoa'
                ]
                
                print(f"\n✅ Campos obrigatórios presentes:")
                for field in required_fields:
                    status = "✓" if field in doc else "✗"
                    print(f"  {status} {field}")
                
                if i < 3:
                    print("\n" + "=" * 80)
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao conectar ao MongoDB: {e}")

if __name__ == "__main__":
    view_mongodb_data()

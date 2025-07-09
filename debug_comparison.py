#!/usr/bin/env python3
"""
Debug da comparação de documentos MongoDB
"""

import pymongo
import json
from run_caf_analysis import get_unidade_familiar_complete_data, convert_to_mongodb_document, documents_are_different

MONGODB_CONFIG = {
    'connection_string': 'mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db',
    'database': 'audit_db',
    'collection': 'caf_changes'
}

def debug_document_comparison():
    """Debug da comparação de documentos"""
    
    print("🔍 Debug de Comparação de Documentos")
    print("=" * 50)
    
    # Conectar ao MongoDB
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Pegar primeiro documento do MongoDB
    existing_doc = collection.find_one()
    if not existing_doc:
        print("❌ Nenhum documento encontrado no MongoDB")
        return
    
    unidade_id = existing_doc['idUnidadeFamiliar']
    print(f"📄 Testando unidade familiar: {unidade_id}")
    
    # Buscar dados atuais do PostgreSQL
    complete_data = get_unidade_familiar_complete_data(unidade_id, 'caf_20250401')
    if not complete_data:
        print("❌ Dados não encontrados no PostgreSQL")
        return
    
    # Converter para documento MongoDB
    new_document = convert_to_mongodb_document(complete_data)
    if not new_document:
        print("❌ Erro ao converter documento")
        return
    
    # Comparar
    are_different = documents_are_different(new_document, existing_doc)
    
    print(f"\n🔍 Resultado da comparação: {'DIFERENTES' if are_different else 'IGUAIS'}")
    
    if are_different:
        print("\n📊 Detalhes das diferenças:")
        
        # Comparar campo por campo (simplificado)
        for key in new_document.keys():
            if key in ['_id']:
                continue
            
            new_val = new_document.get(key)
            old_val = existing_doc.get(key)
            
            if new_val != old_val:
                print(f"   🔄 {key}:")
                print(f"      Novo: {new_val}")
                print(f"      Antigo: {old_val}")
    
    # Salvar documentos para análise manual
    with open('debug_new_doc.json', 'w', encoding='utf-8') as f:
        json.dump(new_document, f, indent=2, ensure_ascii=False, default=str)
    
    with open('debug_existing_doc.json', 'w', encoding='utf-8') as f:
        # Converter ObjectId para string
        existing_doc_clean = dict(existing_doc)
        existing_doc_clean['_id'] = str(existing_doc_clean['_id'])
        json.dump(existing_doc_clean, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Documentos salvos para análise:")
    print(f"   - debug_new_doc.json (do PostgreSQL)")
    print(f"   - debug_existing_doc.json (do MongoDB)")
    
    client.close()

if __name__ == "__main__":
    debug_document_comparison()

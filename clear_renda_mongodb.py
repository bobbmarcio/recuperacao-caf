#!/usr/bin/env python3
"""
Script para limpar a coleção de renda do MongoDB
"""

import pymongo

# Configuração MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_renda'
}

def clear_renda_collection():
    """Limpa a coleção de renda"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        # Contar documentos antes
        count_before = collection.count_documents({})
        print(f"📊 Documentos antes da limpeza: {count_before}")
        
        if count_before > 0:
            # Confirmar limpeza
            print("⚠️  ATENÇÃO: Esta operação irá apagar TODOS os documentos da coleção!")
            print(f"   Coleção: {MONGODB_CONFIG['collection']}")
            print(f"   Database: {MONGODB_CONFIG['database']}")
            
            confirm = input("Digite 'CONFIRMAR' para prosseguir: ")
            
            if confirm == 'CONFIRMAR':
                # Deletar todos os documentos
                result = collection.delete_many({})
                print(f"✅ {result.deleted_count} documentos removidos com sucesso")
                
                # Verificar se limpeza foi completa
                count_after = collection.count_documents({})
                print(f"📊 Documentos após limpeza: {count_after}")
                
                if count_after == 0:
                    print("🎉 Coleção limpa com sucesso!")
                else:
                    print("⚠️  Alguns documentos podem não ter sido removidos")
            else:
                print("❌ Operação cancelada")
        else:
            print("ℹ️  Coleção já está vazia")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Erro ao limpar coleção: {e}")

if __name__ == "__main__":
    clear_renda_collection()

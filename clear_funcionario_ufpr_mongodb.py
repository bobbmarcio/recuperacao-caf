#!/usr/bin/env python3
"""
Script para limpeza da coleção caf_funcionario_ufpr no MongoDB
"""

from pymongo import MongoClient
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações do MongoDB
MONGO_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db'
}

def clear_funcionario_ufpr_collection():
    """Remove todos os documentos da coleção caf_funcionario_ufpr"""
    try:
        # Conectar ao MongoDB
        client = MongoClient(MONGO_CONFIG['connection_string'])
        db = client[MONGO_CONFIG['database']]
        collection = db['caf_funcionario_ufpr']
        
        # Verificar quantos documentos existem antes da limpeza
        count_before = collection.count_documents({})
        logging.info(f"Documentos na coleção antes da limpeza: {count_before}")
        
        if count_before > 0:
            # Remover todos os documentos
            result = collection.delete_many({})
            logging.info(f"Documentos removidos: {result.deleted_count}")
        else:
            logging.info("A coleção já estava vazia")
        
        # Verificar se a limpeza foi bem-sucedida
        count_after = collection.count_documents({})
        logging.info(f"Documentos na coleção após limpeza: {count_after}")
        
        if count_after == 0:
            logging.info("Limpeza da coleção caf_funcionario_ufpr concluída com sucesso!")
        else:
            logging.warning(f"Ainda restam {count_after} documentos na coleção")
        
        client.close()
        
    except Exception as e:
        logging.error(f"Erro ao limpar coleção funcionario_ufpr: {e}")
        raise

def main():
    """Função principal"""
    logging.info("Iniciando limpeza da coleção caf_funcionario_ufpr...")
    clear_funcionario_ufpr_collection()
    logging.info("Processo de limpeza finalizado.")

if __name__ == "__main__":
    main()

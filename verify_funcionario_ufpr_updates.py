#!/usr/bin/env python3
"""
Script para verificar atualizações de funcionários UFPR no MongoDB
"""

from pymongo import MongoClient
import psycopg2
import logging
from datetime import datetime, timezone
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações de conexão
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'caf_analysis',
    'user': 'caf_user',
    'password': 'caf_password123'
}

MONGO_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db'
}

def verify_funcionario_ufpr_updates():
    """Verifica estado das atualizações de funcionário UFPR"""
    try:
        # Conectar ao MongoDB
        mongo_client = MongoClient(MONGO_CONFIG['connection_string'])
        mongo_db = mongo_client[MONGO_CONFIG['database']]
        funcionario_collection = mongo_db['caf_funcionario_ufpr']
        
        # Conectar ao PostgreSQL
        postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = postgres_conn.cursor()
        
        # Verificar estatísticas MongoDB
        mongo_count = funcionario_collection.count_documents({})
        logging.info(f"Total de documentos no MongoDB: {mongo_count}")
        
        if mongo_count > 0:
            # Funcionários únicos
            unique_funcionarios = len(funcionario_collection.distinct('idMaoDeObra'))
            logging.info(f"Funcionários únicos no MongoDB: {unique_funcionarios}")
            
            # Estatísticas por schema de origem
            pipeline = [
                {"$group": {
                    "_id": "$_metadata.schemaOrigin",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
            schemas_stats = list(funcionario_collection.aggregate(pipeline))
            logging.info("Distribuição por schema de origem:")
            for stat in schemas_stats:
                logging.info(f"  - {stat['_id']}: {stat['count']} documentos")
            
            # Últimas inserções
            latest_docs = list(funcionario_collection.find().sort([("_metadata.insertedAt", -1)]).limit(3))
            logging.info("Últimas 3 inserções:")
            for i, doc in enumerate(latest_docs, 1):
                metadata = doc.get('_metadata', {})
                inserted_at = metadata.get('insertedAt', 'N/A')
                schema_origin = metadata.get('schemaOrigin', 'N/A')
                funcionario_id = doc.get('idMaoDeObra', 'N/A')
                logging.info(f"  {i}. Funcionário {funcionario_id} - Schema: {schema_origin} - Data: {inserted_at}")
        
        # Verificar totais no PostgreSQL
        schemas = ['caf_20250301', 'caf_20250401', 'caf_20250501', 'caf_20250601']
        logging.info("\nTotais no PostgreSQL (apenas unidades ativas):")
        
        for schema in schemas:
            try:
                query = f'''
                SELECT COUNT(*)
                FROM {schema}."S_FUNCIONARIO_UFPR" fu
                INNER JOIN {schema}."S_UNIDADE_FAMILIAR" uf ON fu.id_unidade_familiar = uf.id_unidade_familiar
                WHERE uf.id_tipo_situacao_unidade_familiar = 1;
                '''
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logging.info(f"  - {schema}: {count} funcionários")
            except Exception as e:
                logging.warning(f"  - {schema}: Erro ao consultar ({e})")
        
        # Verificar exemplos de documentos
        if mongo_count > 0:
            logging.info("\nExemplos de documentos:")
            sample_doc = funcionario_collection.find_one()
            if sample_doc:
                # Remover _id do ObjectId para melhor visualização
                if '_id' in sample_doc:
                    sample_doc['_id'] = str(sample_doc['_id'])
                logging.info(f"Documento exemplo: {json.dumps(sample_doc, default=str, indent=2, ensure_ascii=False)}")
        
        cursor.close()
        postgres_conn.close()
        mongo_client.close()
        
        logging.info("Verificação concluída!")
        
    except Exception as e:
        logging.error(f"Erro durante verificação: {e}")
        raise

def main():
    """Função principal"""
    logging.info("Iniciando verificação de atualizações de funcionário UFPR...")
    verify_funcionario_ufpr_updates()

if __name__ == "__main__":
    main()

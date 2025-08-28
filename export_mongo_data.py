#!/usr/bin/env python3
"""
Script para exportar dados do MongoDB para arquivos JSON
Exporta todas as coleções CAF para facilitar migração entre VMs
"""

import pymongo
import json
import os
from datetime import datetime
from pathlib import Path
import gzip
from typing import Dict, List
from loguru import logger

# Configuração de logging
logger.add("logs/export_mongo_{time}.log", rotation="1 day")

# Configurações do MongoDB
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db'
}

# Coleções CAF para exportar
CAF_COLLECTIONS = [
    'caf_unidade_familiar',
    'caf_unidade_familiar_pessoa', 
    'caf_endereco',
    'caf_area_imovel',
    'caf_renda',
    'caf_funcionario_ufpr'
]

def connect_mongodb():
    """Conecta ao MongoDB"""
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        # Testa a conexão
        client.admin.command('ping')
        logger.info(f"✅ Conectado ao MongoDB: {MONGODB_CONFIG['database']}")
        return client, db
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao MongoDB: {e}")
        raise

def export_collection_to_json(db, collection_name: str, output_dir: Path) -> Dict:
    """Exporta uma coleção para arquivo JSON comprimido"""
    logger.info(f"📤 Exportando coleção: {collection_name}")
    
    try:
        collection = db[collection_name]
        
        # Conta documentos
        total_docs = collection.count_documents({})
        logger.info(f"📊 Total de documentos: {total_docs:,}")
        
        if total_docs == 0:
            logger.warning(f"⚠️ Coleção {collection_name} está vazia")
            return {"collection": collection_name, "documents": 0, "file": None}
        
        # Nome do arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{collection_name}_{timestamp}.json.gz"
        filepath = output_dir / filename
        
        # Exporta documentos em lotes
        batch_size = 1000
        exported_count = 0
        
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            f.write('[\n')
            first_doc = True
            
            for doc in collection.find().batch_size(batch_size):
                # Remove o _id do MongoDB (será regenerado na importação)
                if '_id' in doc:
                    del doc['_id']
                
                # Adiciona vírgula entre documentos (exceto o primeiro)
                if not first_doc:
                    f.write(',\n')
                else:
                    first_doc = False
                
                # Escreve o documento
                json.dump(doc, f, ensure_ascii=False, default=str)
                exported_count += 1
                
                # Log de progresso
                if exported_count % 10000 == 0:
                    logger.info(f"📝 Exportados {exported_count:,} documentos...")
            
            f.write('\n]')
        
        # Verifica o tamanho do arquivo
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        
        logger.success(f"✅ Exportação concluída: {collection_name}")
        logger.info(f"📁 Arquivo: {filename}")
        logger.info(f"📊 Documentos exportados: {exported_count:,}")
        logger.info(f"💾 Tamanho do arquivo: {file_size_mb:.2f} MB")
        
        return {
            "collection": collection_name,
            "documents": exported_count,
            "file": filename,
            "size_mb": file_size_mb
        }
        
    except Exception as e:
        logger.error(f"❌ Erro ao exportar {collection_name}: {e}")
        return {"collection": collection_name, "error": str(e)}

def create_metadata_file(exports: List[Dict], output_dir: Path):
    """Cria arquivo de metadados da exportação"""
    timestamp = datetime.now().isoformat()
    
    metadata = {
        "export_timestamp": timestamp,
        "total_collections": len(exports),
        "mongodb_config": {
            "database": MONGODB_CONFIG['database']
        },
        "collections": exports,
        "summary": {
            "total_documents": sum(exp.get("documents", 0) for exp in exports),
            "total_size_mb": sum(exp.get("size_mb", 0) for exp in exports),
            "successful_exports": len([exp for exp in exports if "error" not in exp]),
            "failed_exports": len([exp for exp in exports if "error" in exp])
        }
    }
    
    metadata_file = output_dir / f"export_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"📋 Metadados salvos: {metadata_file.name}")
    return metadata

def main():
    """Função principal de exportação"""
    print("🚀 EXPORTAÇÃO DE DADOS MONGODB CAF")
    print("=" * 50)
    
    # Cria diretório de saída
    output_dir = Path("export_mongodb")
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"📂 Diretório de saída: {output_dir.absolute()}")
    
    try:
        # Conecta ao MongoDB
        client, db = connect_mongodb()
        
        # Lista coleções disponíveis
        available_collections = db.list_collection_names()
        logger.info(f"📋 Coleções disponíveis: {len(available_collections)}")
        
        # Verifica quais coleções CAF existem
        existing_caf_collections = [col for col in CAF_COLLECTIONS if col in available_collections]
        missing_collections = [col for col in CAF_COLLECTIONS if col not in available_collections]
        
        if missing_collections:
            logger.warning(f"⚠️ Coleções não encontradas: {missing_collections}")
        
        logger.info(f"✅ Coleções CAF encontradas: {len(existing_caf_collections)}")
        
        # Exporta cada coleção
        exports = []
        for collection_name in existing_caf_collections:
            export_result = export_collection_to_json(db, collection_name, output_dir)
            exports.append(export_result)
            print(f"✅ {collection_name}: {export_result.get('documents', 0):,} documentos")
        
        # Cria arquivo de metadados
        metadata = create_metadata_file(exports, output_dir)
        
        # Relatório final
        print("\n📊 RELATÓRIO DE EXPORTAÇÃO")
        print("=" * 50)
        print(f"📂 Diretório: {output_dir.absolute()}")
        print(f"🗃️ Coleções exportadas: {metadata['summary']['successful_exports']}")
        print(f"📄 Total de documentos: {metadata['summary']['total_documents']:,}")
        print(f"💾 Tamanho total: {metadata['summary']['total_size_mb']:.2f} MB")
        
        if metadata['summary']['failed_exports'] > 0:
            print(f"❌ Falhas: {metadata['summary']['failed_exports']}")
        
        print(f"\n🎉 Exportação concluída!")
        print(f"📋 Metadados: export_metadata_*.json")
        print(f"📦 Arquivos: *.json.gz")
        
        # Fecha conexão
        client.close()
        
    except Exception as e:
        logger.error(f"💥 Erro na exportação: {e}")
        print(f"❌ Erro: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python3
"""
Script para importar dados do MongoDB a partir de arquivos JSON
Importa todas as coleções CAF exportadas de outra VM
"""

import pymongo
import json
import gzip
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from loguru import logger
import argparse

# Configuração de logging
logger.add("logs/import_mongo_{time}.log", rotation="1 day")

# Configurações do MongoDB (VM destino)
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db'
}

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

def import_collection_from_json(db, filepath: Path, collection_name: str, mode: str = "replace") -> Dict:
    """Importa uma coleção a partir de arquivo JSON comprimido"""
    logger.info(f"📥 Importando coleção: {collection_name}")
    logger.info(f"📁 Arquivo: {filepath.name}")
    
    try:
        collection = db[collection_name]
        
        # Verifica se a coleção já existe
        existing_count = collection.count_documents({})
        if existing_count > 0:
            if mode == "replace":
                logger.warning(f"🗑️ Removendo {existing_count:,} documentos existentes...")
                collection.delete_many({})
            elif mode == "skip":
                logger.info(f"⏭️ Coleção já existe ({existing_count:,} docs), pulando...")
                return {"collection": collection_name, "skipped": True, "existing_documents": existing_count}
            elif mode == "append":
                logger.info(f"➕ Adicionando aos {existing_count:,} documentos existentes...")
        
        # Lê e processa o arquivo
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            logger.info(f"📖 Carregando dados do arquivo...")
            documents = json.load(f)
        
        total_docs = len(documents)
        logger.info(f"📊 Documentos para importar: {total_docs:,}")
        
        if total_docs == 0:
            logger.warning(f"⚠️ Arquivo {filepath.name} está vazio")
            return {"collection": collection_name, "documents": 0}
        
        # Importa em lotes
        batch_size = 1000
        imported_count = 0
        
        for i in range(0, total_docs, batch_size):
            batch = documents[i:i + batch_size]
            
            try:
                # Insere o lote
                result = collection.insert_many(batch, ordered=False)
                imported_count += len(result.inserted_ids)
                
                # Log de progresso
                if imported_count % 10000 == 0:
                    logger.info(f"📝 Importados {imported_count:,}/{total_docs:,} documentos...")
                    
            except Exception as e:
                logger.error(f"❌ Erro no lote {i//batch_size + 1}: {e}")
                # Continua com o próximo lote
                continue
        
        # Verifica o resultado
        final_count = collection.count_documents({})
        
        logger.success(f"✅ Importação concluída: {collection_name}")
        logger.info(f"📊 Documentos importados: {imported_count:,}")
        logger.info(f"📊 Total na coleção: {final_count:,}")
        
        return {
            "collection": collection_name,
            "documents_imported": imported_count,
            "total_documents": final_count,
            "success": True
        }
        
    except Exception as e:
        logger.error(f"❌ Erro ao importar {collection_name}: {e}")
        return {"collection": collection_name, "error": str(e), "success": False}

def find_export_files(import_dir: Path) -> Dict[str, Path]:
    """Encontra arquivos de exportação no diretório"""
    files = {}
    
    # Procura por arquivos .json.gz
    for filepath in import_dir.glob("*.json.gz"):
        # Extrai o nome da coleção do nome do arquivo
        filename = filepath.stem.replace('.json', '')  # Remove .json do .json.gz
        
        # Identifica a coleção baseada no nome do arquivo
        for collection_name in ['caf_unidade_familiar', 'caf_unidade_familiar_pessoa', 
                               'caf_endereco', 'caf_area_imovel', 'caf_renda', 'caf_funcionario_ufpr']:
            if collection_name in filename:
                files[collection_name] = filepath
                break
    
    return files

def load_metadata(import_dir: Path) -> Dict:
    """Carrega arquivo de metadados da exportação"""
    metadata_files = list(import_dir.glob("export_metadata_*.json"))
    
    if not metadata_files:
        logger.warning("⚠️ Arquivo de metadados não encontrado")
        return {}
    
    # Pega o arquivo mais recente
    metadata_file = max(metadata_files, key=lambda f: f.stat().st_mtime)
    
    try:
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        logger.info(f"📋 Metadados carregados: {metadata_file.name}")
        return metadata
    except Exception as e:
        logger.error(f"❌ Erro ao carregar metadados: {e}")
        return {}

def main():
    """Função principal de importação"""
    parser = argparse.ArgumentParser(description="Importar dados MongoDB CAF")
    parser.add_argument("--dir", "-d", default="export_mongodb", 
                       help="Diretório com os arquivos de exportação")
    parser.add_argument("--mode", "-m", choices=["replace", "append", "skip"], 
                       default="replace", help="Modo de importação")
    parser.add_argument("--collection", "-c", help="Importar apenas uma coleção específica")
    
    args = parser.parse_args()
    
    print("🚀 IMPORTAÇÃO DE DADOS MONGODB CAF")
    print("=" * 50)
    
    import_dir = Path(args.dir)
    
    if not import_dir.exists():
        print(f"❌ Diretório não encontrado: {import_dir}")
        return 1
    
    logger.info(f"📂 Diretório de importação: {import_dir.absolute()}")
    logger.info(f"🔄 Modo: {args.mode}")
    
    try:
        # Carrega metadados
        metadata = load_metadata(import_dir)
        if metadata:
            print(f"📋 Exportação original: {metadata.get('export_timestamp', 'N/A')}")
            print(f"📊 Total documentos: {metadata.get('summary', {}).get('total_documents', 'N/A'):,}")
        
        # Encontra arquivos de exportação
        export_files = find_export_files(import_dir)
        
        if not export_files:
            print(f"❌ Nenhum arquivo de exportação encontrado em {import_dir}")
            return 1
        
        print(f"📁 Arquivos encontrados: {len(export_files)}")
        for collection, filepath in export_files.items():
            print(f"  • {collection}: {filepath.name}")
        
        # Filtra por coleção específica se solicitado
        if args.collection:
            if args.collection in export_files:
                export_files = {args.collection: export_files[args.collection]}
                print(f"🎯 Importando apenas: {args.collection}")
            else:
                print(f"❌ Coleção não encontrada: {args.collection}")
                return 1
        
        # Conecta ao MongoDB
        client, db = connect_mongodb()
        
        # Importa cada coleção
        imports = []
        for collection_name, filepath in export_files.items():
            import_result = import_collection_from_json(db, filepath, collection_name, args.mode)
            imports.append(import_result)
            
            if import_result.get("success"):
                print(f"✅ {collection_name}: {import_result.get('documents_imported', 0):,} documentos")
            elif import_result.get("skipped"):
                print(f"⏭️ {collection_name}: pulado ({import_result.get('existing_documents', 0):,} docs existentes)")
            else:
                print(f"❌ {collection_name}: erro")
        
        # Relatório final
        print("\n📊 RELATÓRIO DE IMPORTAÇÃO")
        print("=" * 50)
        
        successful = [imp for imp in imports if imp.get("success")]
        skipped = [imp for imp in imports if imp.get("skipped")]
        failed = [imp for imp in imports if not imp.get("success") and not imp.get("skipped")]
        
        print(f"✅ Sucesso: {len(successful)}")
        print(f"⏭️ Pulados: {len(skipped)}")
        print(f"❌ Falhas: {len(failed)}")
        
        total_imported = sum(imp.get("documents_imported", 0) for imp in successful)
        print(f"📄 Total importado: {total_imported:,} documentos")
        
        if failed:
            print("\n❌ FALHAS:")
            for imp in failed:
                print(f"  • {imp['collection']}: {imp.get('error', 'Erro desconhecido')}")
        
        print(f"\n🎉 Importação concluída!")
        
        # Fecha conexão
        client.close()
        
    except Exception as e:
        logger.error(f"💥 Erro na importação: {e}")
        print(f"❌ Erro: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

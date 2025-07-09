#!/usr/bin/env python3
"""
Script para consultar histórico de versões de uma unidade familiar específica
"""

import pymongo
import sys
from datetime import datetime

# Configurações
MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar'
}

def show_unit_history(unidade_id: str):
    """Mostra histórico completo de uma unidade familiar"""
    
    print(f"📋 HISTÓRICO DA UNIDADE FAMILIAR: {unidade_id}")
    print("=" * 80)
    
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Buscar todas as versões
    versions = list(collection.find(
        {'idUnidadeFamiliar': unidade_id}
    ).sort('_versao', 1))
    
    if not versions:
        print(f"❌ Nenhuma versão encontrada para a unidade {unidade_id}")
        client.close()
        return
    
    print(f"📊 Total de versões encontradas: {len(versions)}")
    print()
    
    for i, version in enumerate(versions, 1):
        version_num = version.get('_versao', 1)
        schema = version.get('_schema_origem', 'N/A')
        timestamp = version.get('_timestamp_versao', 'N/A')
        prev_version = version.get('_versao_anterior')
        
        print(f"🔸 VERSÃO {version_num}")
        print(f"   Schema origem: {schema}")
        print(f"   Timestamp: {timestamp}")
        if prev_version:
            print(f"   Versão anterior: v{prev_version}")
        print(f"   Data criação: {version.get('dataCriacao', 'N/A')}")
        print(f"   Data atualização: {version.get('dataAtualizacao', 'N/A')}")
        
        # Informações do CAF
        caf_info = version.get('caf', {})
        if caf_info:
            print(f"   CAF: {caf_info.get('numeroCaf', 'N/A')}")
            print(f"   UF CAF: {caf_info.get('uf', 'N/A')}")
        
        # Situação
        situacao = version.get('tipoSituacao', {})
        if situacao:
            print(f"   Situação: {situacao.get('descricao', 'N/A')}")
        
        # Entidade emissora
        entidade = version.get('entidadeEmissora', {})
        if entidade:
            print(f"   Entidade: {entidade.get('razaoSocial', 'N/A')}")
            print(f"   CNPJ: {entidade.get('cnpj', 'N/A')}")
        
        print(f"   Documento MongoDB ID: {version.get('_id')}")
        print()
    
    client.close()

def list_recent_versions():
    """Lista unidades com versões recentes para facilitar a consulta"""
    
    print("🔍 UNIDADES COM VERSÕES RECENTES")
    print("=" * 50)
    
    client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
    db = client[MONGODB_CONFIG['database']]
    collection = db[MONGODB_CONFIG['collection']]
    
    # Buscar unidades com múltiplas versões
    pipeline = [
        {'$group': {
            '_id': '$idUnidadeFamiliar',
            'count': {'$sum': 1},
            'max_version': {'$max': '$_versao'},
            'latest_timestamp': {'$max': '$_timestamp_versao'},
            'caf_number': {'$first': '$caf.numeroCaf'}
        }},
        {'$sort': {'latest_timestamp': -1}},
        {'$limit': 20}
    ]
    
    units = list(collection.aggregate(pipeline))
    
    for i, unit in enumerate(units, 1):
        print(f"{i:2d}. {unit['_id']}")
        print(f"    Versões: {unit['count']} (máx: v{unit['max_version']})")
        print(f"    CAF: {unit.get('caf_number', 'N/A')}")
        print(f"    Última alteração: {unit.get('latest_timestamp', 'N/A')}")
        print()
    
    client.close()

def main():
    if len(sys.argv) < 2:
        print("🔍 CONSULTA DE HISTÓRICO DE VERSÕES")
        print("=" * 40)
        print("Uso:")
        print("  python version_history.py <unidade_id>  - Mostra histórico de uma unidade específica")
        print("  python version_history.py list          - Lista unidades com versões recentes")
        print()
        print("Exemplo:")
        print("  python version_history.py 00008da6-ab22-4592-a88b-650c88655468")
        return
    
    if sys.argv[1] == "list":
        list_recent_versions()
    else:
        unidade_id = sys.argv[1]
        show_unit_history(unidade_id)

if __name__ == "__main__":
    main()

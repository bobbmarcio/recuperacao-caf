#!/usr/bin/env python3
"""
Script para análise incremental de funcionários UFPR.
Compara mudanças nos funcionários UFPR entre dumps consecutivos do CAF e salva no MongoDB.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

import psycopg2
import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timezone
import uuid
import logging
from typing import Dict, List, Any, Optional
import json

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/funcionario_ufpr_analysis.log'),
        logging.StreamHandler()
    ]
)

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

# Mapeamento de campos baseado no CSV extraído
FIELD_MAPPING = {
    'idUnidadeFamiliar': 'id_unidade_familiar',
    'dataAtualizacao': 'dt_atualizacao',
    'idMaoDeObra': 'id_funcionario_ufpr',
    'pessoaFisica.id': 'id_pessoa_fisica',
    'pessoaFisica.nome': 'nm_pessoa_fisica',
    'pessoaFisica.dataNascimento': 'dt_nascimento',
    'pessoaFisica.cpf': 'nr_cpf',
    'pessoaFisica.dataAtualizacao': 'dt_atualizacao',
    'pessoaFisica.sexo.id': 'id_sexo',
    'pessoaFisica.sexo.nome': 'nm_sexo'
}

class FuncionarioUfprAnalyzer:
    def __init__(self):
        self.postgres_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.collection = None
        
    def connect_databases(self):
        """Conecta aos bancos PostgreSQL e MongoDB"""
        try:
            # PostgreSQL
            self.postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info("Conectado ao PostgreSQL")
            
            # MongoDB
            self.mongo_client = MongoClient(MONGO_CONFIG['connection_string'])
            self.mongo_db = self.mongo_client[MONGO_CONFIG['database']]
            self.collection = self.mongo_db['caf_funcionario_ufpr']
            logging.info("Conectado ao MongoDB")
            
        except Exception as e:
            logging.error(f"Erro ao conectar aos bancos: {e}")
            raise
    
    def get_funcionario_data(self, schema: str) -> List[Dict[str, Any]]:
        """Busca dados dos funcionários UFPR com JOIN nas tabelas relacionadas"""
        query = f"""
        SELECT DISTINCT
            fu.id_funcionario_ufpr,
            fu.id_unidade_familiar,
            fu.id_pessoa_fisica,
            fu.dt_criacao,
            pf.nm_pessoa_fisica,
            pf.dt_nascimento,
            pf.nr_cpf,
            pf.dt_atualizacao,
            s.id_sexo,
            s.nm_sexo
        FROM {schema}."S_FUNCIONARIO_UFPR" fu
        INNER JOIN {schema}."S_PESSOA_FISICA" pf ON fu.id_pessoa_fisica = pf.id_pessoa_fisica
        INNER JOIN {schema}."S_UNIDADE_FAMILIAR" uf ON fu.id_unidade_familiar = uf.id_unidade_familiar
        LEFT JOIN {schema}."S_SEXO" s ON pf.id_sexo = s.id_sexo
        WHERE uf.id_tipo_situacao_unidade_familiar = 1
        ORDER BY fu.id_funcionario_ufpr;
        """
        
        cursor = self.postgres_conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            results.append(row_dict)
        
        cursor.close()
        return results
    
    def convert_to_mongo_format(self, postgres_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Converte dados do PostgreSQL para formato MongoDB"""
        mongo_docs = []
        
        for row in postgres_data:
            # Converter datas para string ou datetime para compatibilidade MongoDB
            data_atualizacao = row.get('dt_atualizacao')
            if isinstance(data_atualizacao, datetime):
                data_atualizacao = data_atualizacao.isoformat()
            
            data_nascimento = row.get('dt_nascimento')
            if data_nascimento and hasattr(data_nascimento, 'isoformat'):
                data_nascimento = data_nascimento.isoformat()
            
            doc = {
                '_id': str(uuid.uuid4()),
                'idUnidadeFamiliar': str(row.get('id_unidade_familiar', '')),
                'dataAtualizacao': data_atualizacao,
                'idMaoDeObra': str(row.get('id_funcionario_ufpr', '')),
                'dataExclusao': None,
                'excluido': False,
                'pessoaFisica': {
                    'id': str(row.get('id_pessoa_fisica', '')),
                    'nome': row.get('nm_pessoa_fisica', ''),
                    'dataNascimento': data_nascimento,
                    'cpf': row.get('nr_cpf', ''),
                    'dataAtualizacao': data_atualizacao,
                    'sexo': {
                        'id': row.get('id_sexo'),
                        'nome': row.get('nm_sexo', '') if row.get('nm_sexo') else None
                    }
                }
            }
            mongo_docs.append(doc)
        
        return mongo_docs
    
    def get_mapped_fields(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extrai apenas os campos mapeados do documento para comparação"""
        mapped = {}
        
        for mongo_field, postgres_field in FIELD_MAPPING.items():
            if '.' in mongo_field:
                # Campo aninhado
                parts = mongo_field.split('.')
                if parts[0] == 'pessoaFisica':
                    if parts[1] == 'sexo' and len(parts) == 3:
                        # pessoaFisica.sexo.id ou pessoaFisica.sexo.nome
                        if doc.get('pessoaFisica', {}).get('sexo'):
                            mapped[mongo_field] = doc['pessoaFisica']['sexo'].get(parts[2])
                    else:
                        # pessoaFisica.campo
                        if doc.get('pessoaFisica'):
                            mapped[mongo_field] = doc['pessoaFisica'].get(parts[1])
            else:
                # Campo raiz
                mapped[mongo_field] = doc.get(mongo_field)
        
        return mapped
    
    def has_changes(self, old_doc: Dict[str, Any], new_doc: Dict[str, Any]) -> bool:
        """Verifica se houve mudanças nos campos mapeados"""
        old_mapped = self.get_mapped_fields(old_doc)
        new_mapped = self.get_mapped_fields(new_doc)
        
        return old_mapped != new_mapped
    
    def process_incremental_analysis(self, old_schema: str, new_schema: str, limit: Optional[int] = None):
        """Processa análise incremental entre dois schemas"""
        logging.info(f"Iniciando análise incremental: {old_schema} -> {new_schema}")
        
        # Buscar dados dos dois períodos
        old_data = self.get_funcionario_data(old_schema)
        new_data = self.get_funcionario_data(new_schema)
        
        logging.info(f"Período anterior ({old_schema}): {len(old_data)} funcionários")
        logging.info(f"Período atual ({new_schema}): {len(new_data)} funcionários")
        
        # Converter para formato MongoDB
        old_docs = self.convert_to_mongo_format(old_data)
        new_docs = self.convert_to_mongo_format(new_data)
        
        # Criar índices por ID para comparação rápida
        old_by_id = {doc['idMaoDeObra']: doc for doc in old_docs}
        new_by_id = {doc['idMaoDeObra']: doc for doc in new_docs}
        
        # Análise de mudanças
        changes_found = 0
        new_versions_created = 0
        
        for funcionario_id, new_doc in new_by_id.items():
            if limit and changes_found >= limit:
                logging.info(f"Limite de {limit} alterações atingido")
                break
                
            old_doc = old_by_id.get(funcionario_id)
            
            if old_doc and self.has_changes(old_doc, new_doc):
                changes_found += 1
                
                # Verificar se já existe uma versão no MongoDB
                existing_doc = self.collection.find_one({
                    'idMaoDeObra': funcionario_id,
                    'dataAtualizacao': old_doc['dataAtualizacao']
                })
                
                if not existing_doc:
                    # Adicionar metadados de versionamento
                    old_doc['_metadata'] = {
                        'schemaOrigin': old_schema,
                        'schemaComparison': new_schema,
                        'insertedAt': datetime.now(timezone.utc),
                        'reason': 'incremental_change_detected'
                    }
                    
                    # Inserir versão anterior no MongoDB
                    self.collection.insert_one(old_doc)
                    new_versions_created += 1
                    
                    logging.info(f"Nova versão criada para funcionário {funcionario_id}")
        
        logging.info(f"Resumo da análise:")
        logging.info(f"   - Funcionários com alterações: {changes_found}")
        logging.info(f"   - Novas versões criadas: {new_versions_created}")
        
        return {
            'changes_found': changes_found,
            'new_versions_created': new_versions_created,
            'old_total': len(old_data),
            'new_total': len(new_data)
        }
    
    def close_connections(self):
        """Fecha conexões com os bancos"""
        if self.postgres_conn:
            self.postgres_conn.close()
            logging.info("Conexão PostgreSQL fechada")
        
        if self.mongo_client:
            self.mongo_client.close()
            logging.info("Conexão MongoDB fechada")

def main():
    """Função principal"""
    analyzer = FuncionarioUfprAnalyzer()
    
    try:
        # Conectar aos bancos
        analyzer.connect_databases()
        
        # Definir schemas para comparação
        old_schema = 'caf_20250301'
        new_schema = 'caf_20250401'
        
        # Executar análise incremental com limite para teste
        limit = None  # Remover para análise completa
        
        result = analyzer.process_incremental_analysis(old_schema, new_schema, limit)
        
        logging.info("Análise concluída com sucesso!")
        logging.info(f"Estatísticas finais: {json.dumps(result, indent=2)}")
        
    except Exception as e:
        logging.error(f"Erro durante análise: {e}")
        raise
    finally:
        analyzer.close_connections()

if __name__ == "__main__":
    main()

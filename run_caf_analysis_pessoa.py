#!/usr/bin/env python3
"""
Análise incremental CAF - Unidade Familiar Pessoa
Implementa as mesmas regras da análise unidade_familiar:
1. Apenas registros ATIVOS (st_excluido = false)
2. Apenas campos que realmente mudaram entre dumps
3. Mapeamento baseado no arquivo de_para_unidade_familiar_pessoa.csv
"""

import psycopg2
import pymongo
import pandas as pd
from datetime import datetime
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Configurações
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'user': 'caf_user',
    'password': 'caf_password123',
    'database': 'caf_analysis'
}

MONGODB_CONFIG = {
    'connection_string': 'mongodb://admin:admin123@localhost:27017/audit_db?authSource=admin',
    'database': 'audit_db',
    'collection': 'caf_unidade_familiar_pessoa'
}

class CAFPessoaFieldMapper:
    """Mapeia campos entre PostgreSQL e MongoDB para unidade_familiar_pessoa"""
    
    def __init__(self):
        self.mapping = self.load_field_mapping()
    
    def load_field_mapping(self) -> Dict[str, Dict]:
        """Carrega mapeamento de campos do arquivo CSV"""
        try:
            df = pd.read_csv('de_para_unidade_familiar_pessoa.csv')
            
            # Limpar e processar dados
            mapping = {}
            
            for _, row in df.iterrows():
                mongo_field = row.iloc[0]  # Campo Mongo
                postgres_table = row.iloc[2]  # Tabela Postgres
                postgres_field = row.iloc[3]  # Campo Postgres
                
                # Pular linhas vazias ou cabeçalhos
                if pd.isna(mongo_field) or mongo_field in ['Campo (Mongo)', '']:
                    continue
                
                # Pular campos técnicos
                if mongo_field in ['_id']:
                    continue
                
                # Pular campos que não se aplicam
                if pd.isna(postgres_table) or postgres_table == 'Não se aplica':
                    continue
                
                # Limpar espaços em branco
                mongo_field = str(mongo_field).strip()
                postgres_table = str(postgres_table).strip()
                postgres_field = str(postgres_field).strip()
                
                mapping[mongo_field] = {
                    'postgres_table': postgres_table,
                    'postgres_field': postgres_field,
                    'is_object': '.' in mongo_field,
                    'parent_field': mongo_field.split('.')[0] if '.' in mongo_field else None
                }
            
            print(f"✅ Carregado mapeamento de {len(mapping)} campos para unidade_familiar_pessoa")
            return mapping
            
        except Exception as e:
            print(f"❌ Erro ao carregar mapeamento: {e}")
            return {}
    
    def get_postgres_fields_for_table(self, table_name: str) -> List[str]:
        """Retorna lista de campos PostgreSQL para uma tabela específica"""
        fields = []
        for mongo_field, mapping in self.mapping.items():
            if mapping['postgres_table'] == table_name and not pd.isna(mapping['postgres_field']):
                fields.append(mapping['postgres_field'])
        return list(set(fields))

def get_caf_schemas() -> List[str]:
    """Obtém lista de schemas CAF ordenados por data"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT schemaname 
        FROM pg_tables 
        WHERE schemaname LIKE 'caf_2025%' 
        GROUP BY schemaname 
        ORDER BY schemaname
    """)
    
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return schemas

def get_active_pessoa_changes(schema1: str, schema2: str, mapper: CAFPessoaFieldMapper, limit: int = None) -> List[Dict]:
    """
    Detecta alterações em registros de pessoa ATIVOS
    Apenas st_excluido = false
    """
    
    print(f"   🔍 Analisando pessoas ATIVAS entre {schema1} e {schema2}...")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Query para detectar IDs das pessoas que mudaram
    query = f"""
    SELECT 
        COALESCE(t1."id_unidade_familiar_pessoa", t2."id_unidade_familiar_pessoa") as id_pessoa,
        CASE 
            WHEN t1."id_unidade_familiar_pessoa" IS NULL THEN 'INSERT'
            WHEN t2."id_unidade_familiar_pessoa" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type
    FROM "{schema1}"."S_UNIDADE_FAMILIAR_PESSOA" t1
    FULL OUTER JOIN "{schema2}"."S_UNIDADE_FAMILIAR_PESSOA" t2 
        ON t1."id_unidade_familiar_pessoa" = t2."id_unidade_familiar_pessoa"
    WHERE 
        -- Apenas registros ATIVOS no schema2 (mais recente)
        (t2."st_excluido" = false OR t2."st_excluido" IS NULL)
        AND (
            t1."id_unidade_familiar_pessoa" IS NULL OR 
            t2."id_unidade_familiar_pessoa" IS NULL OR
            t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao"
        )
    ORDER BY COALESCE(t1."id_unidade_familiar_pessoa", t2."id_unidade_familiar_pessoa")
    """
    
    # Adicionar limite se especificado
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        changes = []
        for row in results:
            pessoa_id, change_type = row
            
            # Para cada ID, detectar que campos mudaram
            changed_fields = detect_pessoa_field_changes(str(pessoa_id), schema1, schema2, mapper, cursor)
            
            if changed_fields:
                change_record = {
                    'pessoa_id': str(pessoa_id),
                    'change_type': change_type,
                    'schema_from': schema1,
                    'schema_to': schema2,
                    'changed_fields': changed_fields,
                    'timestamp': datetime.utcnow()
                }
                changes.append(change_record)
        
        print(f"      ✅ {len(changes)} pessoas ativas com alterações detectadas")
        
    except Exception as e:
        print(f"      ❌ Erro na análise: {e}")
        changes = []
    
    cursor.close()
    conn.close()
    return changes

def detect_pessoa_field_changes(pessoa_id: str, schema1: str, schema2: str, mapper: CAFPessoaFieldMapper, cursor) -> List[str]:
    """Detecta alterações específicas em campos para uma pessoa"""
    
    try:
        # Buscar dados da pessoa em ambos os schemas
        cursor.execute(f"""
            SELECT * FROM "{schema1}"."S_UNIDADE_FAMILIAR_PESSOA" 
            WHERE "id_unidade_familiar_pessoa" = %s
        """, (pessoa_id,))
        
        old_data = cursor.fetchone()
        old_columns = [desc[0] for desc in cursor.description] if old_data else []
        
        cursor.execute(f"""
            SELECT * FROM "{schema2}"."S_UNIDADE_FAMILIAR_PESSOA" 
            WHERE "id_unidade_familiar_pessoa" = %s
        """, (pessoa_id,))
        
        new_data = cursor.fetchone()
        new_columns = [desc[0] for desc in cursor.description] if new_data else []
        
        changed_fields = []
        
        if old_data and new_data:
            # Ambos existem - verificar diferenças
            old_dict = dict(zip(old_columns, old_data))
            new_dict = dict(zip(new_columns, new_data))
            
            # Verificar cada campo mapeado
            pessoa_fields = mapper.get_postgres_fields_for_table('S_UNIDADE_FAMILIAR_PESSOA')
            
            for field in pessoa_fields:
                if field in ['dt_criacao', 'dt_atualizacao']:  # Ignorar campos de auditoria
                    continue
                
                old_value = old_dict.get(field)
                new_value = new_dict.get(field)
                
                if old_value != new_value:
                    mongo_field = get_mongo_field_name(field, mapper)
                    if mongo_field:
                        changed_fields.append({
                            'mongo_field': mongo_field,
                            'postgres_field': field,
                            'old_value': str(old_value) if old_value is not None else None,
                            'new_value': str(new_value) if new_value is not None else None
                        })
        
        elif new_data and not old_data:
            # Inserção - todos os campos
            changed_fields.append({
                'mongo_field': 'ALL',
                'postgres_field': 'ALL',
                'old_value': None,
                'new_value': 'NEW_RECORD'
            })
        
        return changed_fields
        
    except Exception as e:
        print(f"        ⚠️  Erro ao detectar mudanças para {pessoa_id}: {e}")
        return []

def get_mongo_field_name(postgres_field: str, mapper: CAFPessoaFieldMapper) -> Optional[str]:
    """Encontra o nome do campo MongoDB correspondente ao campo PostgreSQL"""
    
    for mongo_field, mapping in mapper.mapping.items():
        if mapping['postgres_field'] == postgres_field:
            return mongo_field
    
    return None

def build_complete_pessoa_document(pessoa_id: str, schema: str, mapper: CAFPessoaFieldMapper) -> Optional[Dict]:
    """Constrói documento completo da pessoa usando o mapeamento"""
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Query principal para pessoa com JOINs
        cursor.execute(f"""
            SELECT 
                ufp.*,
                uf.id_unidade_familiar,
                pf.id_pessoa_fisica, pf.sg_uf as pf_uf, pf.nm_pessoa_fisica, pf.nm_social,
                pf.nr_cpf, pf.dt_nascimento, pf.nm_mae, pf.nm_pai,
                pf.dt_criacao as pf_dt_criacao, pf.dt_atualizacao as pf_dt_atualizacao
            FROM "{schema}"."S_UNIDADE_FAMILIAR_PESSOA" ufp
            LEFT JOIN "{schema}"."S_UNIDADE_FAMILIAR" uf 
                ON ufp.id_unidade_familiar = uf.id_unidade_familiar
            LEFT JOIN "{schema}"."S_PESSOA_FISICA" pf 
                ON ufp.id_pessoa_fisica = pf.id_pessoa_fisica
            WHERE ufp.id_unidade_familiar_pessoa = %s
                AND ufp.st_excluido = false
        """, (pessoa_id,))
        
        result = cursor.fetchone()
        if not result:
            return None
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, result))
        
        # Construir documento usando mapeamento
        document = build_mapped_pessoa_document(data, mapper)
        
        return document
        
    except Exception as e:
        print(f"      ❌ Erro ao buscar dados completos da pessoa: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def build_mapped_pessoa_document(data: Dict, mapper: CAFPessoaFieldMapper) -> Dict:
    """Constrói documento MongoDB baseado no mapeamento"""
    
    document = {
        '_versao': 1,  # Será sobrescrito na função save_changes_to_mongodb
        'idMembroFamiliar': str(data.get('id_unidade_familiar_pessoa')),
        'idUnidadeFamiliar': str(data.get('id_unidade_familiar')),
    }
    
    # Mapear campos diretos da pessoa
    direct_mappings = {
        'codigoSipra': data.get('cd_sipra'),
        'excluido': bool(data.get('st_excluido', False)),
        'trabalhaUfpr': bool(data.get('st_trabalha_ufpr', False)),
        'codigoCaf': data.get('cd_caf'),
        'dataCriacao': convert_date(data.get('dt_criacao'), with_time=True),
        'dataAtualizacao': convert_date(data.get('dt_atualizacao'), with_time=True),
        'dataInicioMaoDeObra': convert_date(data.get('dt_inicio_mao_de_obra')),
        'dataFimMaoDeObra': convert_date(data.get('dt_fim_mao_de_obra')),
        'cadastroCadunico': bool(data.get('st_cadastro_cadunico', False)),
        'baixaRenda': bool(data.get('st_baixa_renda', False)),
        'cadunicoAtualizado': bool(data.get('st_cadunico_atualizado', False)),
    }
    
    document.update(direct_mappings)
    
    # Pessoa física
    if data.get('id_pessoa_fisica'):
        document['pessoaFisica'] = {
            'id': str(data.get('id_pessoa_fisica')),
            'uf': data.get('pf_uf'),
            'nome': data.get('nm_pessoa_fisica'),
            'nomeSocial': data.get('nm_social'),
            'cpf': data.get('nr_cpf'),
            'dataNascimento': convert_date(data.get('dt_nascimento')),
            'nomeMae': data.get('nm_mae'),
            'nomePai': data.get('nm_pai'),
            'dataCriacao': convert_date(data.get('pf_dt_criacao'), with_time=True),
            'dataAtualizacao': convert_date(data.get('pf_dt_atualizacao'), with_time=True),
        }
    
    return document

def convert_date(value, with_time=False):
    """Converte datas para formato MongoDB"""
    if value is None:
        return None
    if hasattr(value, 'isoformat'):
        if with_time:
            return value
        else:
            return value.strftime('%Y-%m-%d')
    if isinstance(value, str):
        return value
    return str(value)

def save_pessoa_changes_to_mongodb(changes: List[Dict], mapper: CAFPessoaFieldMapper) -> Tuple[bool, int]:
    """Salva alterações no MongoDB criando novas versões para mudanças"""
    if not changes:
        return True, 0
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        inserted_count = 0
        updated_count = 0
        ignored_count = 0
        
        for change in changes:
            pessoa_id = change['pessoa_id']
            schema = change['schema_to']
            
            # Verificar se já foi processado para este schema
            existing_doc = collection.find_one({
                'idMembroFamiliar': pessoa_id,
                '_schema_origem': schema
            })
            
            if existing_doc:
                ignored_count += 1
                continue
            
            # Construir documento completo
            new_document = build_complete_pessoa_document(pessoa_id, schema, mapper)
            
            if new_document:
                # Buscar a versão mais recente existente
                latest_doc = collection.find_one(
                    {'idMembroFamiliar': pessoa_id},
                    sort=[('_versao', -1)]
                )
                
                if latest_doc:
                    # Verificar se houve mudança real
                    if pessoa_documents_are_different(latest_doc, new_document):
                        # Criar nova versão
                        new_version = latest_doc.get('_versao', 1) + 1
                        new_document['_versao'] = new_version
                        new_document['_versao_anterior'] = latest_doc.get('_versao', 1)
                        new_document['_schema_origem'] = schema
                        new_document['_timestamp_versao'] = datetime.utcnow()
                        
                        result = collection.insert_one(new_document)
                        if result.inserted_id:
                            updated_count += 1
                        else:
                            ignored_count += 1
                    else:
                        ignored_count += 1
                else:
                    # Primeira versão do documento
                    new_document['_versao'] = 1
                    new_document['_versao_anterior'] = None
                    new_document['_schema_origem'] = schema
                    new_document['_timestamp_versao'] = datetime.utcnow()
                    
                    result = collection.insert_one(new_document)
                    if result.inserted_id:
                        inserted_count += 1
                    else:
                        ignored_count += 1
        
        # Relatório
        total_processed = inserted_count + updated_count + ignored_count
        actual_changes = inserted_count + updated_count  # Apenas inserções e novas versões
        
        if total_processed > 0:
            print(f"   📊 Processamento concluído:")
            if inserted_count > 0:
                print(f"      ✨ {inserted_count} pessoas INSERIDAS (primeira versão)")
            if updated_count > 0:
                print(f"      🔄 {updated_count} pessoas VERSIONADAS (nova versão criada)")
            if ignored_count > 0:
                print(f"      ⏭️  {ignored_count} pessoas IGNORADAS (sem alteração ou já processadas)")
        
        client.close()
        return True, actual_changes
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar no MongoDB: {e}")
        import traceback
        print(traceback.format_exc())
        return False, 0

def pessoa_documents_are_different(doc1: Dict, doc2: Dict) -> bool:
    """Compara documentos ignorando campos de auditoria e controle"""
    
    # Campos a ignorar na comparação
    ignore_fields = {
        '_id', 
        'dataCriacao', 
        'dataAtualizacao',
        '_versao',
        '_versao_anterior', 
        '_schema_origem', 
        '_timestamp_versao'
    }
    
    def clean_doc(doc):
        if isinstance(doc, dict):
            return {k: clean_doc(v) for k, v in doc.items() if k not in ignore_fields}
        elif isinstance(doc, list):
            return [clean_doc(item) for item in doc]
        else:
            return doc
    
    clean_doc1 = clean_doc(doc1)
    clean_doc2 = clean_doc(doc2)
    
    return clean_doc1 != clean_doc2

def run_incremental_pessoa_analysis(limit: int = None):
    """Executa análise incremental para unidade_familiar_pessoa"""
    
    print("🎯 ANÁLISE INCREMENTAL CAF - UNIDADE FAMILIAR PESSOA")
    print("=" * 70)
    print("📋 REGRAS:")
    print("   ✅ Apenas registros ATIVOS (st_excluido = false)")
    print("   🔍 Apenas campos que realmente mudaram")
    print("   📊 Mapeamento baseado no arquivo de_para_unidade_familiar_pessoa.csv")
    
    if limit:
        print(f"   🔢 LIMITE: {limit} alterações por schema")
    
    # Carregar mapeamento
    mapper = CAFPessoaFieldMapper()
    if not mapper.mapping:
        print("❌ Falha ao carregar mapeamento. Verifique o arquivo CSV.")
        return
    
    # Obter schemas
    schemas = get_caf_schemas()
    if len(schemas) < 2:
        print(f"❌ Schemas insuficientes. Encontrados: {schemas}")
        return
    
    print(f"📊 Schemas disponíveis: {schemas}")
    
    total_changes = 0
    
    for i in range(len(schemas) - 1):
        schema1 = schemas[i]
        schema2 = schemas[i + 1]
        
        print(f"\n🔄 Comparando {schema1} → {schema2}")
        
        # Analisar alterações com limite
        changes = get_active_pessoa_changes(schema1, schema2, mapper, limit)
        
        if changes:
            # Salvar no MongoDB e obter número real de documentos processados
            success, actual_changes = save_pessoa_changes_to_mongodb(changes, mapper)
            if success:
                total_changes += actual_changes
    
    print(f"\n🎉 Análise de pessoas concluída!")
    print(f"📈 Total de pessoas EFETIVAMENTE SALVAS: {total_changes:,}")
    print(f"💾 Dados salvos na coleção: {MONGODB_CONFIG['collection']}")
    print(f"🔗 Acesse Mongo Express: http://localhost:8080")

if __name__ == "__main__":
    import sys
    
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print("⚠️  Uso: python script.py [limite]")
    
    run_incremental_pessoa_analysis(limit)

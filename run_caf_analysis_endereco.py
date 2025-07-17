#!/usr/bin/env python3
"""
Análise incremental CAF - Endereços
Implementa as mesmas regras da análise unidade_familiar:
1. Todos os endereços (sem filtro de status - não existe campo indicativo)
2. Apenas campos que realmente mudaram entre dumps
3. Mapeamento baseado no arquivo de_para_endereco.csv
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
    'collection': 'caf_endereco'
}

class CAFEnderecoFieldMapper:
    """Mapeia campos entre PostgreSQL e MongoDB para endereços"""
    
    def __init__(self):
        self.mapping = self.load_field_mapping()
    
    def load_field_mapping(self) -> Dict[str, Dict]:
        """Carrega mapeamento de campos do arquivo CSV"""
        try:
            df = pd.read_csv('de_para_endereco.csv')
            
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
            
            print(f"✅ Carregado mapeamento de {len(mapping)} campos para endereços")
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

def get_endereco_changes(schema1: str, schema2: str, mapper: CAFEnderecoFieldMapper, limit: int = None) -> List[Dict]:
    """
    Detecta alterações em endereços
    Não há filtro de status (não existe campo indicativo)
    """
    
    print(f"   🔍 Analisando endereços entre {schema1} e {schema2}...")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Query para detectar IDs dos endereços que mudaram
    query = f"""
    SELECT 
        COALESCE(t1."id_endereco", t2."id_endereco") as id_endereco,
        CASE 
            WHEN t1."id_endereco" IS NULL THEN 'INSERT'
            WHEN t2."id_endereco" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type
    FROM "{schema1}"."S_ENDERECO" t1
    FULL OUTER JOIN "{schema2}"."S_ENDERECO" t2 
        ON t1."id_endereco" = t2."id_endereco"
    WHERE 
        t1."id_endereco" IS NULL OR 
        t2."id_endereco" IS NULL OR
        t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao"
    ORDER BY COALESCE(t1."id_endereco", t2."id_endereco")
    """
    
    # Adicionar limite se especificado
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        changes = []
        for row in results:
            endereco_id, change_type = row
            
            # Para cada ID, detectar que campos mudaram
            changed_fields = detect_endereco_field_changes(str(endereco_id), schema1, schema2, mapper, cursor)
            
            if changed_fields:
                change_record = {
                    'endereco_id': str(endereco_id),
                    'change_type': change_type,
                    'schema_from': schema1,
                    'schema_to': schema2,
                    'changed_fields': changed_fields,
                    'timestamp': datetime.utcnow()
                }
                changes.append(change_record)
        
        print(f"      ✅ {len(changes)} endereços com alterações detectadas")
        
    except Exception as e:
        print(f"      ❌ Erro na análise: {e}")
        changes = []
    
    cursor.close()
    conn.close()
    return changes

def detect_endereco_field_changes(endereco_id: str, schema1: str, schema2: str, mapper: CAFEnderecoFieldMapper, cursor) -> List[str]:
    """Detecta alterações específicas em campos para um endereço"""
    
    try:
        # Buscar dados do endereço em ambos os schemas
        cursor.execute(f"""
            SELECT * FROM "{schema1}"."S_ENDERECO" 
            WHERE "id_endereco" = %s
        """, (endereco_id,))
        
        old_data = cursor.fetchone()
        old_columns = [desc[0] for desc in cursor.description] if old_data else []
        
        cursor.execute(f"""
            SELECT * FROM "{schema2}"."S_ENDERECO" 
            WHERE "id_endereco" = %s
        """, (endereco_id,))
        
        new_data = cursor.fetchone()
        new_columns = [desc[0] for desc in cursor.description] if new_data else []
        
        changed_fields = []
        
        if old_data and new_data:
            # Ambos existem - verificar diferenças
            old_dict = dict(zip(old_columns, old_data))
            new_dict = dict(zip(new_columns, new_data))
            
            # Verificar cada campo mapeado
            endereco_fields = mapper.get_postgres_fields_for_table('S_ENDERECO')
            
            for field in endereco_fields:
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
        print(f"        ⚠️  Erro ao detectar mudanças para {endereco_id}: {e}")
        return []

def get_mongo_field_name(postgres_field: str, mapper: CAFEnderecoFieldMapper) -> Optional[str]:
    """Encontra o nome do campo MongoDB correspondente ao campo PostgreSQL"""
    
    for mongo_field, mapping in mapper.mapping.items():
        if mapping['postgres_field'] == postgres_field:
            return mongo_field
    
    return None

def build_complete_endereco_document(endereco_id: str, schema: str, mapper: CAFEnderecoFieldMapper) -> Optional[Dict]:
    """Constrói documento completo do endereço usando o mapeamento"""
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Query principal para endereço com JOINs
        cursor.execute(f"""
            SELECT 
                e.*,
                eup.id_unidade_familiar,
                m.cd_municipio, m.nm_municipio, m.cd_uf, m.sg_uf as municipio_uf
            FROM "{schema}"."S_ENDERECO" e
            LEFT JOIN "{schema}"."S_ENDERECO_UFPR_PESSOA" eup 
                ON e.id_endereco = eup.id_endereco
            LEFT JOIN "{schema}"."S_MUNICIPIO" m 
                ON e.cd_municipio = m.cd_municipio
            WHERE e.id_endereco = %s
        """, (endereco_id,))
        
        result = cursor.fetchone()
        if not result:
            return None
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, result))
        
        # Construir documento usando mapeamento
        document = build_mapped_endereco_document(data, mapper)
        
        return document
        
    except Exception as e:
        print(f"      ❌ Erro ao buscar dados completos do endereço: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def build_mapped_endereco_document(data: Dict, mapper: CAFEnderecoFieldMapper) -> Dict:
    """Constrói documento MongoDB baseado no mapeamento"""
    
    document = {
        '_versao': 1,  # Será sobrescrito na função save_changes_to_mongodb
        'idEndereco': str(data.get('id_endereco')),
        'idUnidadeFamiliar': str(data.get('id_unidade_familiar')) if data.get('id_unidade_familiar') else None,
    }
    
    # Mapear campos diretos do endereço
    direct_mappings = {
        'uf': data.get('sg_uf'),
        'cep': data.get('nr_cep'),
        'logradouro': data.get('ds_logradouro'),
        'complemento': data.get('ds_complemento'),
        'numero': data.get('nr_logradouro'),
        'referencia': data.get('ds_referencia'),
        'dataAtualizacao': convert_date(data.get('dt_atualizacao'), with_time=True),
    }
    
    document.update(direct_mappings)
    
    # Município
    if data.get('cd_municipio'):
        document['codigoMunicipio'] = data.get('cd_municipio')
        document['municipio'] = {
            'codigo': data.get('cd_municipio'),
            'nome': data.get('nm_municipio'),
            'codigoUf': data.get('cd_uf'),
            'siglaUf': data.get('municipio_uf')
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

def save_endereco_changes_to_mongodb(changes: List[Dict], mapper: CAFEnderecoFieldMapper) -> Tuple[bool, int]:
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
            endereco_id = change['endereco_id']
            schema = change['schema_to']
            
            # Verificar se já foi processado para este schema
            existing_doc = collection.find_one({
                'idEndereco': endereco_id,
                '_schema_origem': schema
            })
            
            if existing_doc:
                ignored_count += 1
                continue
            
            # Construir documento completo
            new_document = build_complete_endereco_document(endereco_id, schema, mapper)
            
            if new_document:
                # Buscar a versão mais recente existente
                latest_doc = collection.find_one(
                    {'idEndereco': endereco_id},
                    sort=[('_versao', -1)]
                )
                
                if latest_doc:
                    # Verificar se houve mudança real
                    if endereco_documents_are_different(latest_doc, new_document):
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
                print(f"      ✨ {inserted_count} endereços INSERIDOS (primeira versão)")
            if updated_count > 0:
                print(f"      🔄 {updated_count} endereços VERSIONADOS (nova versão criada)")
            if ignored_count > 0:
                print(f"      ⏭️  {ignored_count} endereços IGNORADOS (sem alteração ou já processados)")
        
        client.close()
        return True, actual_changes
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar no MongoDB: {e}")
        import traceback
        print(traceback.format_exc())
        return False, 0

def endereco_documents_are_different(doc1: Dict, doc2: Dict) -> bool:
    """Compara documentos ignorando campos de auditoria e controle"""
    
    # Campos a ignorar na comparação
    ignore_fields = {
        '_id', 
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

def run_incremental_endereco_analysis(limit: int = None):
    """Executa análise incremental para endereços"""
    
    print("🎯 ANÁLISE INCREMENTAL CAF - ENDEREÇOS")
    print("=" * 70)
    print("📋 REGRAS:")
    print("   📍 Todos os endereços (sem filtro de status)")
    print("   🔍 Apenas campos que realmente mudaram")
    print("   📊 Mapeamento baseado no arquivo de_para_endereco.csv")
    
    if limit:
        print(f"   🔢 LIMITE: {limit} alterações por schema")
    
    # Carregar mapeamento
    mapper = CAFEnderecoFieldMapper()
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
        changes = get_endereco_changes(schema1, schema2, mapper, limit)
        
        if changes:
            # Salvar no MongoDB e obter número real de documentos processados
            success, actual_changes = save_endereco_changes_to_mongodb(changes, mapper)
            if success:
                total_changes += actual_changes
    
    print(f"\n🎉 Análise de endereços concluída!")
    print(f"📈 Total de endereços EFETIVAMENTE SALVOS: {total_changes:,}")
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
    
    run_incremental_endereco_analysis(limit)

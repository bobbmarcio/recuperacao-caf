#!/usr/bin/env python3
"""
Análise incremental CAF com mapeamento baseado no arquivo ODS
Implementa regras:
1. Apenas unidades familiares ATIVAS (id_tipo_situacao_unidade_familiar = 1)
2. Apenas campos que realmente mudaram entre dumps
3. Mapeamento baseado no arquivo de_para_mongo_postgres_caf.ods
"""

import psycopg2
import pymongo
import yaml
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
    'collection': 'caf_unidade_familiar'
}

class CAFFieldMapper:
    """Mapeia campos entre PostgreSQL e MongoDB baseado no arquivo ODS"""
    
    def __init__(self):
        self.mapping = self.load_field_mapping()
    
    def load_field_mapping(self) -> Dict[str, Dict]:
        """Carrega mapeamento de campos do arquivo CSV"""
        try:
            df = pd.read_csv('de_para_unidade_familiar.csv')
            
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
                if mongo_field in ['_id', '_versao']:
                    continue
                
                # Pular campos que não se aplicam
                if pd.isna(postgres_table) or postgres_table == 'Não se aplica':
                    continue
                
                mapping[mongo_field] = {
                    'postgres_table': postgres_table,
                    'postgres_field': postgres_field,
                    'is_object': '.' in mongo_field,
                    'parent_field': mongo_field.split('.')[0] if '.' in mongo_field else None
                }
            
            print(f"✅ Carregado mapeamento de {len(mapping)} campos")
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

def get_active_unidade_familiar_changes(schema1: str, schema2: str, mapper: CAFFieldMapper, limit: int = None) -> List[Dict]:
    """
    Detecta alterações em unidades familiares ATIVAS
    Apenas id_tipo_situacao_unidade_familiar = 1
    """
    
    print(f"   🔍 Analisando unidades familiares ATIVAS entre {schema1} e {schema2}...")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Query simplificada para detectar IDs das unidades ativas que mudaram
    query = f"""
    SELECT 
        COALESCE(t1."id_unidade_familiar", t2."id_unidade_familiar") as id_unidade_familiar,
        CASE 
            WHEN t1."id_unidade_familiar" IS NULL THEN 'INSERT'
            WHEN t2."id_unidade_familiar" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type
    FROM "{schema1}"."S_UNIDADE_FAMILIAR" t1
    FULL OUTER JOIN "{schema2}"."S_UNIDADE_FAMILIAR" t2 
        ON t1."id_unidade_familiar" = t2."id_unidade_familiar"
    WHERE 
        -- Apenas unidades familiares ATIVAS no schema2 (mais recente)
        (t2."id_tipo_situacao_unidade_familiar" = 1 OR t2."id_tipo_situacao_unidade_familiar" IS NULL)
        AND (
            t1."id_unidade_familiar" IS NULL OR 
            t2."id_unidade_familiar" IS NULL OR
            t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao"
        )
    ORDER BY COALESCE(t1."id_unidade_familiar", t2."id_unidade_familiar")
    """
    
    # Adicionar limite se especificado
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        changes = []
        for row in results:
            unidade_id, change_type = row
            
            # Para cada ID, detectar que campos mudaram
            changed_fields = detect_specific_field_changes(str(unidade_id), schema1, schema2, mapper, cursor)
            
            if changed_fields:
                change_record = {
                    'unidade_familiar_id': str(unidade_id),
                    'change_type': change_type,
                    'schema_from': schema1,
                    'schema_to': schema2,
                    'changed_fields': changed_fields,
                    'timestamp': datetime.utcnow()
                }
                changes.append(change_record)
        
        print(f"      ✅ {len(changes)} unidades familiares ativas com alterações detectadas")
        
    except Exception as e:
        print(f"      ❌ Erro na análise: {e}")
        changes = []
    
    cursor.close()
    conn.close()
    return changes

def detect_specific_field_changes(unidade_id: str, schema1: str, schema2: str, mapper: CAFFieldMapper, cursor) -> List[str]:
    """Detecta alterações específicas em campos para uma unidade familiar"""
    
    try:
        # Buscar dados da unidade familiar em ambos os schemas
        cursor.execute(f"""
            SELECT * FROM "{schema1}"."S_UNIDADE_FAMILIAR" 
            WHERE "id_unidade_familiar" = %s
        """, (unidade_id,))
        
        old_data = cursor.fetchone()
        old_columns = [desc[0] for desc in cursor.description] if old_data else []
        
        cursor.execute(f"""
            SELECT * FROM "{schema2}"."S_UNIDADE_FAMILIAR" 
            WHERE "id_unidade_familiar" = %s
        """, (unidade_id,))
        
        new_data = cursor.fetchone()
        new_columns = [desc[0] for desc in cursor.description] if new_data else []
        
        changed_fields = []
        
        if old_data and new_data:
            # Ambos existem - verificar diferenças
            old_dict = dict(zip(old_columns, old_data))
            new_dict = dict(zip(new_columns, new_data))
            
            # Verificar cada campo mapeado
            uf_fields = mapper.get_postgres_fields_for_table('S_UNIDADE_FAMILIAR')
            
            for field in uf_fields:
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
        print(f"        ⚠️  Erro ao detectar mudanças para {unidade_id}: {e}")
        return []

def detect_field_changes(row_dict: Dict, postgres_fields: List[str], mapper: CAFFieldMapper) -> List[str]:
    """Detecta quais campos específicos mudaram"""
    
    changed_fields = []
    
    for field in postgres_fields:
        old_value = row_dict.get(field)
        new_value = row_dict.get(f't2_{field}')
        
        # Ignorar campos de auditoria para comparação
        if field in ['dt_criacao', 'dt_atualizacao']:
            continue
        
        # Verificar se houve mudança real
        if old_value != new_value:
            # Mapear para nome do campo MongoDB
            mongo_field = get_mongo_field_name(field, mapper)
            if mongo_field:
                changed_fields.append({
                    'mongo_field': mongo_field,
                    'postgres_field': field,
                    'old_value': old_value,
                    'new_value': new_value
                })
    
    return changed_fields

def get_mongo_field_name(postgres_field: str, mapper: CAFFieldMapper) -> Optional[str]:
    """Encontra o nome do campo MongoDB correspondente ao campo PostgreSQL"""
    
    for mongo_field, mapping in mapper.mapping.items():
        if mapping['postgres_field'] == postgres_field:
            return mongo_field
    
    return None

def build_complete_document(unidade_id: str, schema: str, mapper: CAFFieldMapper) -> Optional[Dict]:
    """Constrói documento completo da unidade familiar usando o mapeamento"""
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Query principal para unidade familiar com JOINs
        cursor.execute(f"""
            SELECT 
                uf.*,
                ts.ds_situacao_unidade_familiar,
                tt.nm_tipo_terreno_ufpr,
                ca.nm_caracterizacao_area,
                c.id_caf, c.nr_caf, c.uf as caf_uf, c.dt_criacao as caf_dt_criacao,
                tc.ds_tipo_caf,
                ee.id_entidade_emissora, ee.nr_cnpj, ee.nm_razao_social,
                ee.dt_criacao as ee_dt_criacao, ee.dt_inativacao as ee_dt_inativacao,
                ee.ds_motivo_inativacao
            FROM "{schema}"."S_UNIDADE_FAMILIAR" uf
            LEFT JOIN "{schema}"."S_TIPO_SITUACAO_UNIDADE_FAMILIAR" ts 
                ON uf.id_tipo_situacao_unidade_familiar = ts.id_tipo_situacao_unidade_familiar
            LEFT JOIN "{schema}"."S_TIPO_TERRENO_UFPR" tt 
                ON uf.id_tipo_terreno_ufpr = tt.id_tipo_terreno_ufpr
            LEFT JOIN "{schema}"."S_CARACTERIZACAO_AREA" ca 
                ON uf.id_caracterizacao_area = ca.id_caracterizacao_area
            LEFT JOIN "{schema}"."S_CAF" c 
                ON uf.id_unidade_familiar = c.id_unidade_familiar
            LEFT JOIN "{schema}"."S_TIPO_CAF" tc 
                ON c.id_tipo_caf = tc.id_tipo_caf
            LEFT JOIN "{schema}"."S_ENTIDADE_EMISSORA" ee 
                ON c.id_entidade = ee.id_entidade_emissora
            WHERE uf.id_unidade_familiar = %s
                AND uf.id_tipo_situacao_unidade_familiar = 1
        """, (unidade_id,))
        
        result = cursor.fetchone()
        if not result:
            return None
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, result))
        
        # Buscar enquadramentos
        cursor.execute(f"""
            SELECT uer.*, ter.ds_tipo_enquadramento_renda
            FROM "{schema}"."S_UNIDADE_FAMILIAR_ENQUADRAMENTO_RENDA" uer
            LEFT JOIN "{schema}"."S_TIPO_ENQUADRAMENTO_RENDA" ter 
                ON uer.id_tipo_enquadramento_renda = ter.id_tipo_enquadramento_renda
            WHERE uer.id_unidade_familiar = %s
        """, (unidade_id,))
        
        enquadramentos = cursor.fetchall()
        enq_columns = [desc[0] for desc in cursor.description]
        
        # Construir documento usando mapeamento
        document = build_mapped_document(data, enquadramentos, enq_columns, mapper)
        
        return document
        
    except Exception as e:
        print(f"      ❌ Erro ao buscar dados completos: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def build_mapped_document(data: Dict, enquadramentos: List, enq_columns: List, mapper: CAFFieldMapper) -> Dict:
    """Constrói documento MongoDB baseado no mapeamento"""
    
    document = {
        '_versao': 1,  # Será sobrescrito na função save_changes_to_mongodb
        'idUnidadeFamiliar': str(data.get('id_unidade_familiar')),
    }
    
    # Mapear campos diretos da unidade familiar
    direct_mappings = {
        'possuiMaoObraContratada': data.get('st_possui_mao_obra'),
        'dataValidade': convert_date(data.get('dt_validade')),
        'descricaoInativacao': data.get('ds_inativacao'),
        'dataCriacao': convert_date(data.get('dt_criacao')),
        'dataAtualizacao': convert_date(data.get('dt_atualizacao'), with_time=True),
        'dataAtivacao': convert_date(data.get('dt_ativacao'), with_time=True),
        'dataPrimeiraAtivacao': convert_date(data.get('dt_primeira_ativacao')),
        'dataBloqueio': convert_date(data.get('dt_bloqueio')),
        'dataInativacao': convert_date(data.get('dt_inativacao')),
        'migradaCaf2': bool(data.get('st_migrada_caf_2', False)),
        'possuiVersaoCaf3': bool(data.get('st_possui_versao_caf3', False)),
        'migradaIncra': bool(data.get('st_migrada_incra', False)),
    }
    
    document.update(direct_mappings)
    
    # Objetos complexos
    if data.get('id_tipo_terreno_ufpr'):
        document['tipoTerreno'] = {
            'id': data.get('id_tipo_terreno_ufpr'),
            'descricao': data.get('nm_tipo_terreno_ufpr') or 'Tipo não informado'
        }
    
    if data.get('id_caracterizacao_area'):
        document['caracterizacaoArea'] = {
            'id': data.get('id_caracterizacao_area'),
            'descricao': data.get('nm_caracterizacao_area') or 'Caracterização não informada'
        }
    
    if data.get('id_tipo_situacao_unidade_familiar'):
        document['tipoSituacao'] = {
            'id': data.get('id_tipo_situacao_unidade_familiar'),
            'descricao': data.get('ds_situacao_unidade_familiar') or 'Situação não informada'
        }
    
    # CAF
    if data.get('id_caf'):
        document['caf'] = {
            'id': str(data.get('id_caf')),
            'numeroCaf': data.get('nr_caf'),
            'uf': data.get('caf_uf'),
            'dataCriacao': convert_date(data.get('caf_dt_criacao')),
            'tipoCaf': {
                'id': data.get('id_tipo_caf', 1),
                'descricao': data.get('ds_tipo_caf') or 'Unidade Familiar'
            }
        }
        
        # Entidade emissora dentro do CAF
        if data.get('nr_cnpj'):
            document['caf']['entidadeEmissora'] = {
                'id': str(data.get('id_entidade_emissora')),
                'cnpj': data.get('nr_cnpj'),
                'razaoSocial': data.get('nm_razao_social'),
                'dataCriacao': convert_date(data.get('ee_dt_criacao'), with_time=True),
                'dataInativacao': convert_date(data.get('ee_dt_inativacao'), with_time=True),
                'motivoInativacao': data.get('ds_motivo_inativacao')
            }
    
    # Entidade emissora (nível raiz)
    if data.get('nr_cnpj'):
        document['entidadeEmissora'] = {
            'id': str(data.get('id_entidade_emissora')),
            'cnpj': data.get('nr_cnpj'),
            'razaoSocial': data.get('nm_razao_social'),
            'dataCriacao': convert_date(data.get('ee_dt_criacao'), with_time=True),
            'dataInativacao': convert_date(data.get('ee_dt_inativacao'), with_time=True),
            'motivoInativacao': data.get('ds_motivo_inativacao')
        }
    
    # Enquadramentos de renda
    if enquadramentos:
        document['enquadramentoRendas'] = []
        for enq_data in enquadramentos:
            enq_dict = dict(zip(enq_columns, enq_data))
            document['enquadramentoRendas'].append({
                'id': str(enq_dict.get('id_unidade_familiar_enquadramento_renda')),
                'tipoEnquadramentoRenda': {
                    'id': enq_dict.get('id_tipo_enquadramento_renda'),
                    'descricao': enq_dict.get('ds_tipo_enquadramento_renda') or 'Tipo não informado'
                }
            })
    
    # Número CAF formatado
    if data.get('nr_caf') and data.get('caf_uf'):
        year = str(data.get('caf_dt_criacao', '2025-01-01'))[:4]
        document['numeroCaf'] = f"{data.get('caf_uf')}062025.01.{data.get('nr_caf'):09d}CAF"
    
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

def save_changes_to_mongodb(changes: List[Dict], mapper: CAFFieldMapper) -> Tuple[bool, int]:
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
            unidade_id = change['unidade_familiar_id']
            schema = change['schema_to']
            
            # Construir documento completo
            new_document = build_complete_document(unidade_id, schema, mapper)
            
            if new_document:
                # Buscar a versão mais recente existente
                latest_doc = collection.find_one(
                    {'idUnidadeFamiliar': unidade_id},
                    sort=[('_versao', -1)]
                )
                
                if latest_doc:
                    # Verificar se houve mudança real
                    if documents_are_different(latest_doc, new_document):
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
                print(f"      ✨ {inserted_count} unidades familiares INSERIDAS (primeira versão)")
            if updated_count > 0:
                print(f"      🔄 {updated_count} unidades familiares VERSIONADAS (nova versão criada)")
            if ignored_count > 0:
                print(f"      ⏭️  {ignored_count} unidades familiares IGNORADAS (sem alteração)")
        
        client.close()
        return True, actual_changes
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar no MongoDB: {e}")
        import traceback
        print(traceback.format_exc())
        return False, 0

def documents_are_different(doc1: Dict, doc2: Dict) -> bool:
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

def run_incremental_analysis_with_mapping(limit: int = None):
    """Executa análise incremental com mapeamento baseado no ODS"""
    
    print("🎯 ANÁLISE INCREMENTAL CAF - BASEADA EM MAPEAMENTO ODS")
    print("=" * 70)
    print("📋 REGRAS:")
    print("   ✅ Apenas unidades familiares ATIVAS (id_tipo_situacao_unidade_familiar = 1)")
    print("   🔍 Apenas campos que realmente mudaram")
    print("   📊 Mapeamento baseado no arquivo de_para_mongo_postgres_caf.ods")
    
    if limit:
        print(f"   🔢 LIMITE: {limit} alterações por schema")
    
    # Carregar mapeamento
    mapper = CAFFieldMapper()
    if not mapper.mapping:
        print("❌ Falha ao carregar mapeamento. Verifique o arquivo ODS.")
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
        changes = get_active_unidade_familiar_changes(schema1, schema2, mapper, limit)
        
        if changes:
            # Salvar no MongoDB e obter número real de documentos processados
            success, actual_changes = save_changes_to_mongodb(changes, mapper)
            if success:
                total_changes += actual_changes
    
    print(f"\n🎉 Análise concluída!")
    print(f"📈 Total de unidades familiares EFETIVAMENTE SALVAS: {total_changes:,}")
    print(f"💾 Dados salvos na coleção: {MONGODB_CONFIG['collection']}")
    print(f"🔗 Acesse Mongo Express: http://localhost:8080")

def get_version_history(unidade_id: str) -> List[Dict]:
    """Obtém histórico de versões de uma unidade familiar"""
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        versions = list(collection.find(
            {'idUnidadeFamiliar': unidade_id}
        ).sort('_versao', 1))
        
        client.close()
        return versions
        
    except Exception as e:
        print(f"❌ Erro ao buscar histórico: {e}")
        return []

if __name__ == "__main__":
    import sys
    
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            print("⚠️  Uso: python script.py [limite]")
    
    run_incremental_analysis_with_mapping(limit)

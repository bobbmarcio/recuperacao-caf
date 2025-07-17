#!/usr/bin/env python3
"""
Análise incremental CAF - Renda
Implementa as mesmas regras da análise unidade_familiar:
1. Apenas rendas de unidades familiares ATIVAS (através de JOIN)
2. Apenas campos que realmente mudaram entre dumps
3. Mapeamento baseado no arquivo de_para_renda.csv
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
    'collection': 'caf_renda'
}

class CAFRendaFieldMapper:
    """Mapeia campos entre PostgreSQL e MongoDB para renda"""
    
    def __init__(self):
        self.mapping = self.load_field_mapping()
    
    def load_field_mapping(self) -> Dict[str, Dict]:
        """Carrega mapeamento de campos do arquivo CSV"""
        try:
            df = pd.read_csv('de_para_renda.csv')
            
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
            
            print(f"✅ Carregado mapeamento de {len(mapping)} campos para renda")
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

def get_active_renda_changes(schema1: str, schema2: str, mapper: CAFRendaFieldMapper, limit: int = None) -> List[Dict]:
    """
    Detecta alterações em registros de renda de unidades familiares ATIVAS
    Filtra através de JOIN com S_UNIDADE_FAMILIAR
    """
    
    print(f"   🔍 Analisando rendas de unidades familiares ATIVAS entre {schema1} e {schema2}...")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Query para detectar IDs das rendas que mudaram
    query = f"""
    SELECT 
        COALESCE(t1."id_renda", t2."id_renda") as id_renda,
        CASE 
            WHEN t1."id_renda" IS NULL THEN 'INSERT'
            WHEN t2."id_renda" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type
    FROM "{schema1}"."S_RENDA" t1
    FULL OUTER JOIN "{schema2}"."S_RENDA" t2 
        ON t1."id_renda" = t2."id_renda"
    WHERE 
        -- Apenas rendas de unidades familiares ATIVAS no schema2 (mais recente)
        EXISTS (
            SELECT 1 FROM "{schema2}"."S_UNIDADE_FAMILIAR" uf 
            WHERE uf."id_unidade_familiar" = t2."id_unidade_familiar"
            AND uf."id_tipo_situacao_unidade_familiar" = 1
        )
        AND (
            t1."id_renda" IS NULL OR 
            t2."id_renda" IS NULL OR
            t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao"
        )
    ORDER BY COALESCE(t1."id_renda", t2."id_renda")
    """
    
    # Adicionar limite se especificado
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        changes = []
        for row in results:
            renda_id, change_type = row
            
            # Para cada ID, detectar que campos mudaram
            changed_fields = detect_renda_field_changes(str(renda_id), schema1, schema2, mapper, cursor)
            
            if changed_fields:
                change_record = {
                    'renda_id': str(renda_id),
                    'change_type': change_type,
                    'schema_from': schema1,
                    'schema_to': schema2,
                    'changed_fields': changed_fields,
                    'timestamp': datetime.utcnow()
                }
                changes.append(change_record)
        
        print(f"      ✅ {len(changes)} rendas de unidades ativas com alterações detectadas")
        
    except Exception as e:
        print(f"      ❌ Erro na análise: {e}")
        changes = []
    
    cursor.close()
    conn.close()
    return changes

def detect_renda_field_changes(renda_id: str, schema1: str, schema2: str, mapper: CAFRendaFieldMapper, cursor) -> List[str]:
    """Detecta alterações específicas em campos para uma renda"""
    
    try:
        # Buscar dados da renda em ambos os schemas
        cursor.execute(f"""
            SELECT * FROM "{schema1}"."S_RENDA" 
            WHERE "id_renda" = %s
        """, (renda_id,))
        
        old_data = cursor.fetchone()
        old_columns = [desc[0] for desc in cursor.description] if old_data else []
        
        cursor.execute(f"""
            SELECT * FROM "{schema2}"."S_RENDA" 
            WHERE "id_renda" = %s
        """, (renda_id,))
        
        new_data = cursor.fetchone()
        new_columns = [desc[0] for desc in cursor.description] if new_data else []
        
        changed_fields = []
        
        if old_data and new_data:
            # Comparar apenas campos mapeados
            for mongo_field, field_info in mapper.mapping.items():
                postgres_field = field_info['postgres_field']
                
                # Encontrar índice do campo PostgreSQL
                old_index = old_columns.index(postgres_field) if postgres_field in old_columns else None
                new_index = new_columns.index(postgres_field) if postgres_field in new_columns else None
                
                if old_index is not None and new_index is not None:
                    old_value = old_data[old_index]
                    new_value = new_data[new_index]
                    
                    # Comparar valores (considerando NULL)
                    if old_value != new_value:
                        changed_fields.append(mongo_field)
        
        return changed_fields
        
    except Exception as e:
        print(f"      ⚠️  Erro ao detectar mudanças para renda {renda_id}: {e}")
        return []

def create_renda_document(renda_id: str, schema: str, mapper: CAFRendaFieldMapper) -> Optional[Dict]:
    """Cria documento MongoDB para uma renda"""
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Query principal da renda com JOIN para verificar se unidade familiar é ativa
        query = f"""
        SELECT r.* 
        FROM "{schema}"."S_RENDA" r
        INNER JOIN "{schema}"."S_UNIDADE_FAMILIAR" uf 
            ON r.id_unidade_familiar = uf.id_unidade_familiar
        WHERE r.id_renda = %s
            AND uf.id_tipo_situacao_unidade_familiar = 1
        """
        
        cursor.execute(query, (renda_id,))
        result = cursor.fetchone()
        
        if not result:
            return None
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, result))
        
        # Criar documento base
        document = {
            '_id': str(renda_id),
            '_schema_origem': schema,
            '_timestamp_criacao': datetime.utcnow(),
            '_versao': 1
        }
        
        # Mapear campos básicos
        for mongo_field, field_info in mapper.mapping.items():
            if field_info['postgres_table'] == 'S_RENDA':
                postgres_field = field_info['postgres_field']
                
                if postgres_field in data and data[postgres_field] is not None:
                    value = data[postgres_field]
                    
                    # Converter tipos se necessário
                    if mongo_field in ['rendaEstimada', 'rendaAuferida']:
                        value = float(value) if value else 0.0
                    elif mongo_field in ['producaoAgroecologica']:
                        value = bool(value)
                    elif mongo_field in ['dataCriacao', 'dataAtualizacao']:
                        value = value.isoformat() if hasattr(value, 'isoformat') else str(value)
                    else:
                        value = str(value)
                    
                    document[mongo_field] = value
        
        # Adicionar informações relacionadas de unidade familiar
        document['idUnidadeFamiliar'] = str(data.get('id_unidade_familiar', ''))
        
        return document
        
    except Exception as e:
        print(f"      ❌ Erro ao criar documento para renda {renda_id}: {e}")
        return None
    
    finally:
        cursor.close()
        conn.close()

def save_renda_to_mongodb(documents: List[Dict], changes: List[Dict]):
    """Salva documentos de renda no MongoDB"""
    
    if not documents:
        return
    
    try:
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        saved_count = 0
        updated_count = 0
        
        for document in documents:
            renda_id = document['_id']
            
            # Verificar se já existe documento para esta renda
            existing = collection.find_one({'_id': renda_id})
            
            if existing:
                # Verificar se os campos mapeados realmente mudaram
                relevant_change = False
                change_info = next((c for c in changes if c['renda_id'] == renda_id), None)
                
                if change_info:
                    # Comparar apenas campos mapeados (ignorar campos de controle)
                    for field in change_info.get('changed_fields', []):
                        if field in document and field in existing:
                            if document[field] != existing[field]:
                                relevant_change = True
                                break
                        elif field in document or field in existing:
                            relevant_change = True
                            break
                
                if relevant_change:
                    # Criar nova versão
                    new_version = existing.get('_versao', 1) + 1
                    document['_versao'] = new_version
                    
                    # Manter schema de origem original, mas atualizar timestamp
                    if '_schema_origem' in existing:
                        document['_schema_origem_inicial'] = existing['_schema_origem']
                    document['_schema_origem'] = change_info['schema_to'] if change_info else document['_schema_origem']
                    document['_timestamp_atualizacao'] = datetime.utcnow()
                    
                    collection.insert_one(document)
                    updated_count += 1
            else:
                # Novo documento
                collection.insert_one(document)
                saved_count += 1
        
        print(f"      ✅ MongoDB: {saved_count} novos, {updated_count} atualizados")
        client.close()
        
    except Exception as e:
        print(f"      ❌ Erro ao salvar no MongoDB: {e}")

def analyze_renda_incremental(limit: int = None):
    """Executa análise incremental para renda"""
    
    print("💰 INICIANDO ANÁLISE INCREMENTAL - RENDA")
    print("=" * 60)
    
    # Carregar mapeamento
    mapper = CAFRendaFieldMapper()
    if not mapper.mapping:
        print("❌ Falha ao carregar mapeamento. Abortando análise.")
        return
    
    # Obter schemas disponíveis
    schemas = get_caf_schemas()
    if len(schemas) < 2:
        print("❌ Necessário pelo menos 2 schemas para análise incremental")
        return
    
    print(f"📋 Schemas encontrados: {schemas}")
    print(f"📊 Configurações:")
    print(f"   ✅ Apenas rendas de unidades familiares ATIVAS (JOIN)")
    print(f"   ✅ Apenas campos que realmente mudaram")
    print(f"   ✅ Mapeamento: {len(mapper.mapping)} campos")
    if limit:
        print(f"   ⚠️  LIMITE: {limit} registros por análise")
    print()
    
    total_changes = 0
    total_documents = 0
    
    # Analisar cada par de schemas consecutivos
    for i in range(len(schemas) - 1):
        schema1 = schemas[i]
        schema2 = schemas[i + 1]
        
        print(f"🔄 Analisando: {schema1} → {schema2}")
        
        # Detectar alterações
        changes = get_active_renda_changes(schema1, schema2, mapper, limit)
        
        if changes:
            print(f"   📝 Processando {len(changes)} alterações...")
            
            # Criar documentos para rendas que mudaram
            documents = []
            for change in changes:
                renda_id = change['renda_id']
                schema = change['schema_to']  # Usar schema mais recente
                
                document = create_renda_document(renda_id, schema, mapper)
                if document:
                    documents.append(document)
            
            # Salvar no MongoDB
            if documents:
                save_renda_to_mongodb(documents, changes)
                total_documents += len(documents)
            
            total_changes += len(changes)
        else:
            print("   ℹ️  Nenhuma alteração detectada")
        
        print()
    
    print("🎉 ANÁLISE INCREMENTAL CONCLUÍDA!")
    print(f"📊 Total de alterações processadas: {total_changes}")
    print(f"📄 Total de documentos gerados: {total_documents}")
    print(f"🔗 MongoDB: {MONGODB_CONFIG['database']}.{MONGODB_CONFIG['collection']}")

if __name__ == "__main__":
    import sys
    
    # Permitir limite opcional via argumento
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"⚠️  Executando com limite de {limit} registros por análise")
        except ValueError:
            print("❌ Limite deve ser um número inteiro")
            sys.exit(1)
    
    analyze_renda_incremental(limit)

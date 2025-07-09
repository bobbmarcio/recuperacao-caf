#!/usr/bin/env python3
"""
Análise incremental completa dos schemas CAF com integração MongoDB
"""

import psycopg2
import pymongo
import yaml
from datetime import datetime
import json
from typing import Dict, List, Any
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
    'connection_string': 'mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db',
    'database': 'audit_db',
    'collection': 'caf_changes'
}

def load_monitoring_config() -> Dict:
    """Carrega configuração de monitoramento"""
    config_path = Path("config/monitoring_config.yaml")
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

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

def analyze_table_changes(schema1: str, schema2: str, table_name: str, config: Dict) -> List[Dict]:
    """Analisa alterações em uma tabela específica"""
    
    if table_name not in config.get('tables', {}):
        return []
    
    # Focar apenas na S_UNIDADE_FAMILIAR para estrutura completa
    if table_name != 'S_UNIDADE_FAMILIAR':
        return []
    
    table_config = config['tables'][table_name]
    primary_key = table_config['primary_key']
    columns = table_config['columns']
    
    print(f"   🔍 Analisando {table_name}...")
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Verificar se tabela existe em ambos os schemas
    cursor.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    """, (schema1, table_name))
    
    if cursor.fetchone()[0] == 0:
        print(f"      ⚠️  Tabela {table_name} não encontrada em {schema1}")
        cursor.close()
        conn.close()
        return []
    
    cursor.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    """, (schema2, table_name))
    
    if cursor.fetchone()[0] == 0:
        print(f"      ⚠️  Tabela {table_name} não encontrada em {schema2}")
        cursor.close()
        conn.close()
        return []
    
    # Query simplificada para pegar apenas IDs das unidades que foram alteradas
    query = f"""
    SELECT 
        COALESCE(t1."{primary_key}", t2."{primary_key}") as id,
        CASE 
            WHEN t1."{primary_key}" IS NULL THEN 'INSERT'
            WHEN t2."{primary_key}" IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END as change_type
    FROM "{schema1}"."{table_name}" t1
    FULL OUTER JOIN "{schema2}"."{table_name}" t2 
        ON t1."{primary_key}" = t2."{primary_key}"
    WHERE 
        t1."{primary_key}" IS NULL OR 
        t2."{primary_key}" IS NULL OR
        (t1."dt_atualizacao" IS DISTINCT FROM t2."dt_atualizacao")
    ORDER BY COALESCE(t1."{primary_key}", t2."{primary_key}")
    """
    
    # Adicionar limite se especificado
    if hasattr(run_incremental_analysis, '_limit') and run_incremental_analysis._limit:
        query += f" LIMIT {run_incremental_analysis._limit}"
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        changes = []
        for row in results:
            unidade_id, change_type = row
            
            # Estruturar dados básicos para processamento posterior
            change_record = {
                'table_name': table_name,
                'record_id': str(unidade_id),
                'change_type': change_type,
                'schema_from': schema1,
                'schema_to': schema2,
                'timestamp': datetime.utcnow()
            }
            
            changes.append(change_record)
        
        print(f"      ✅ {len(changes)} unidades familiares com alterações detectadas")
        
    except Exception as e:
        print(f"      ❌ Erro na análise: {e}")
        changes = []
    
    cursor.close()
    conn.close()
    return changes

def get_unidade_familiar_complete_data(unidade_id: str, schema: str) -> Dict:
    """Obtém dados completos da unidade familiar de várias tabelas relacionadas"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()
    
    # Dados principais da unidade familiar
    cursor.execute(f"""
        SELECT uf.*, ts.ds_situacao_unidade_familiar, tt.nm_tipo_terreno_ufpr, ca.nm_caracterizacao_area
        FROM "{schema}"."S_UNIDADE_FAMILIAR" uf
        LEFT JOIN "{schema}"."S_TIPO_SITUACAO_UNIDADE_FAMILIAR" ts ON uf.id_tipo_situacao_unidade_familiar = ts.id_tipo_situacao_unidade_familiar
        LEFT JOIN "{schema}"."S_TIPO_TERRENO_UFPR" tt ON uf.id_tipo_terreno_ufpr = tt.id_tipo_terreno_ufpr  
        LEFT JOIN "{schema}"."S_CARACTERIZACAO_AREA" ca ON uf.id_caracterizacao_area = ca.id_caracterizacao_area
        WHERE uf.id_unidade_familiar = %s
    """, (unidade_id,))
    
    uf_data = cursor.fetchone()
    if not uf_data:
        cursor.close()
        conn.close()
        return None
    
    uf_columns = [desc[0] for desc in cursor.description]
    uf_dict = dict(zip(uf_columns, uf_data))
    
    # Dados do CAF
    cursor.execute(f"""
        SELECT c.*, tc.ds_tipo_caf, ee.nr_cnpj, ee.nm_razao_social, ee.dt_criacao as ee_dt_criacao,
               ee.dt_inativacao as ee_dt_inativacao, ee.ds_motivo_inativacao as ee_motivo_inativacao
        FROM "{schema}"."S_CAF" c
        LEFT JOIN "{schema}"."S_TIPO_CAF" tc ON c.id_tipo_caf = tc.id_tipo_caf
        LEFT JOIN "{schema}"."S_ENTIDADE_EMISSORA" ee ON c.id_entidade = ee.id_entidade_emissora
        WHERE c.id_unidade_familiar = %s
    """, (unidade_id,))
    
    caf_data = cursor.fetchone()
    caf_dict = None
    if caf_data:
        caf_columns = [desc[0] for desc in cursor.description]
        caf_dict = dict(zip(caf_columns, caf_data))
    
    # Dados de enquadramento de renda
    cursor.execute(f"""
        SELECT uer.*, ter.ds_tipo_enquadramento_renda
        FROM "{schema}"."S_UNIDADE_FAMILIAR_ENQUADRAMENTO_RENDA" uer
        LEFT JOIN "{schema}"."S_TIPO_ENQUADRAMENTO_RENDA" ter ON uer.id_tipo_enquadramento_renda = ter.id_tipo_enquadramento_renda
        WHERE uer.id_unidade_familiar = %s
    """, (unidade_id,))
    
    enquadramento_data = cursor.fetchall()
    enquadramento_columns = [desc[0] for desc in cursor.description] if enquadramento_data else []
    
    # Buscar dados de endereço (simplificado - usar padrão para evitar erros)
    endereco_data = None
    
    cursor.close()
    conn.close()
    
    return {
        'unidade_familiar': uf_dict,
        'caf': caf_dict,
        'enquadramentos': [dict(zip(enquadramento_columns, row)) for row in enquadramento_data],
        'endereco': endereco_data
    }

def convert_to_mongodb_document(complete_data: Dict) -> Dict:
    """Converte dados completos para estrutura MongoDB conforme especificação"""
    if not complete_data or not complete_data.get('unidade_familiar'):
        return None
    
    uf = complete_data['unidade_familiar']
    caf = complete_data.get('caf')
    enquadramentos = complete_data.get('enquadramentos', [])
    endereco = complete_data.get('endereco')
    
    # Função auxiliar para converter datas
    def convert_date(value, with_time=False):
        if value is None:
            return None
        if hasattr(value, 'isoformat'):
            if with_time:
                return value  # ISODate para MongoDB
            else:
                return value.strftime('%Y-%m-%d')  # String para datas simples
        if isinstance(value, str):
            return value
        return str(value)
    
    # Função para gerar UUID padrão quando não disponível
    def get_uuid_or_default(value):
        if value and str(value) != 'None':
            return str(value)
        return '00000000-0000-0000-0000-000000000000'
    
    # Construir documento MongoDB
    document = {
        '_versao': 1,
        'idUnidadeFamiliar': get_uuid_or_default(uf.get('id_unidade_familiar')),
        'possuiMaoObraContratada': bool(uf.get('st_possui_mao_obra', False)),
        'dataValidade': convert_date(uf.get('dt_validade')),
        'descricaoInativacao': uf.get('ds_inativacao'),
        'dataCriacao': convert_date(uf.get('dt_criacao')),
        'dataAtualizacao': convert_date(uf.get('dt_atualizacao'), with_time=True),
        'dataAtivacao': convert_date(uf.get('dt_ativacao'), with_time=True),
        'dataPrimeiraAtivacao': convert_date(uf.get('dt_primeira_ativacao')),
        'dataBloqueio': convert_date(uf.get('dt_bloqueio')),
        'dataInativacao': convert_date(uf.get('dt_inativacao')),
        'migradaCaf2': bool(uf.get('st_migrada_caf_2', False)),
        'possuiVersaoCaf3': bool(uf.get('st_possui_versao_caf3', False)),
        'migradaIncra': bool(uf.get('st_migrada_incra', False)),
        'dataEdicao': None,  # Campo adicional conforme exemplo
    }
    
    # Tipo de terreno
    if uf.get('id_tipo_terreno_ufpr'):
        document['tipoTerreno'] = {
            'id': uf.get('id_tipo_terreno_ufpr'),
            'descricao': uf.get('nm_tipo_terreno_ufpr') or 'Agricultura, Pecuária e Outras atividades'
        }
    
    # Caracterização da área
    if uf.get('id_caracterizacao_area'):
        document['caracterizacaoArea'] = {
            'id': uf.get('id_caracterizacao_area'),
            'descricao': uf.get('nm_caracterizacao_area') or 'Nenhuma das opções'
        }
    
    # Tipo de situação
    if uf.get('id_tipo_situacao_unidade_familiar'):
        document['tipoSituacao'] = {
            'id': uf.get('id_tipo_situacao_unidade_familiar'),
            'descricao': uf.get('ds_situacao_unidade_familiar') or 'ATIVA'
        }
    
    # Dados do CAF
    if caf:
        document['caf'] = {
            'id': get_uuid_or_default(caf.get('id_caf')),
            'numeroCaf': caf.get('nr_caf'),
            'uf': caf.get('uf'),
            'dataCriacao': convert_date(caf.get('dt_criacao')),
            'tipoCaf': {
                'id': caf.get('id_tipo_caf', 1),
                'descricao': caf.get('ds_tipo_caf') or 'Unidade Familiar'
            }
        }
        
        # Entidade emissora dentro do CAF
        if caf.get('nr_cnpj'):
            document['caf']['entidadeEmissora'] = {
                'id': get_uuid_or_default(caf.get('id_entidade')),
                'cnpj': caf.get('nr_cnpj'),
                'razaoSocial': caf.get('nm_razao_social'),
                'dataCriacao': convert_date(caf.get('ee_dt_criacao'), with_time=True),
                'dataInativacao': convert_date(caf.get('ee_dt_inativacao'), with_time=True),
                'motivoInativacao': caf.get('ee_motivo_inativacao')
            }
        
        # Número CAF formatado
        if caf.get('uf') and caf.get('nr_caf'):
            year = str(caf.get('dt_criacao', '2025-01-01'))[:4] if caf.get('dt_criacao') else '2025'
            document['numeroCaf'] = f"{caf.get('uf')}062025.01.{caf.get('nr_caf'):09d}CAF"
    
    # Entidade emissora (duplicada conforme exemplo)
    if caf and caf.get('nr_cnpj'):
        document['entidadeEmissora'] = {
            'id': get_uuid_or_default(caf.get('id_entidade')),
            'cnpj': caf.get('nr_cnpj'),
            'razaoSocial': caf.get('nm_razao_social'),
            'dataCriacao': convert_date(caf.get('ee_dt_criacao'), with_time=True),
            'dataInativacao': convert_date(caf.get('ee_dt_inativacao'), with_time=True),
            'motivoInativacao': caf.get('ee_motivo_inativacao')
        }
    
    # Enquadramentos de renda
    if enquadramentos:
        document['enquadramentoRendas'] = []
        for enq in enquadramentos:
            document['enquadramentoRendas'].append({
                'id': get_uuid_or_default(enq.get('id_unidade_familiar_enquadramento_renda')),
                'tipoEnquadramentoRenda': {
                    'id': enq.get('id_tipo_enquadramento_renda'),
                    'descricao': enq.get('ds_tipo_enquadramento_renda') or 'V'
                }
            })
    
    # Endereço da pessoa (usar dados reais se disponível)
    if endereco:
        document['enderecoPessoa'] = {
            'id': get_uuid_or_default(endereco.get('id_endereco')),
            'endereco': {
                'id': get_uuid_or_default(endereco.get('id_endereco')),
                'uf': 'PE',  # Fixo para simplificar
                'cep': endereco.get('nr_cep') or '00000000',
                'logradouro': endereco.get('ds_logradouro') or 'Endereço não disponível',
                'complemento': endereco.get('ds_complemento') or '',
                'numero': endereco.get('nr_endereco') or '',
                'referencia': endereco.get('ds_referencia') or '',
                'dataCriacao': convert_date(endereco.get('dt_criacao'), with_time=True) or datetime.now(),
                'dataAtualizacao': convert_date(endereco.get('dt_atualizacao'), with_time=True) or datetime.now(),
                'codigoMunicipio': {
                    'codigoMunicipio': endereco.get('cd_municipio') or '0000000',
                    'codigoUf': '26',  # PE
                    'siglaUf': 'PE',
                    'nome': endereco.get('nm_municipio') or 'Município'
                }
            }
        }
    else:
        # Endereço padrão quando não há dados
        document['enderecoPessoa'] = {
            'id': '00000000-0000-0000-0000-000000000000',
            'endereco': {
                'id': '00000000-0000-0000-0000-000000000000',
                'uf': 'PE',
                'cep': '00000000',
                'logradouro': 'Endereço não disponível',
                'complemento': '',
                'numero': '',
                'referencia': '',
                'dataCriacao': datetime.now(),
                'dataAtualizacao': datetime.now(),
                'codigoMunicipio': {
                    'codigoMunicipio': '0000000',
                    'codigoUf': '26',
                    'siglaUf': 'PE',
                    'nome': 'Município'
                }
            }
        }
    
    return document

def documents_are_different(doc1: Dict, doc2: Dict) -> bool:
    """Compara dois documentos MongoDB ignorando campos de auditoria e timestamps"""
    
    # Campos que devem ser completamente ignorados na comparação
    ignore_fields = {
        '_id', '_versao', 'dataCriacao', 'dataAtualizacao', 'dataEdicao',
        'dataAtivacao', 'dataInativacao'  # Adicionar mais campos de data
    }
    
    def should_ignore_field(key, parent_path=""):
        """Determina se um campo deve ser ignorado"""
        # Ignorar campos específicos
        if key in ignore_fields:
            return True
        # Ignorar campos que terminam com padrões de data
        if key.endswith(('dataCriacao', 'dataAtualizacao', 'dataEdicao', 'dataAtivacao', 'dataInativacao')):
            return True
        # Ignorar campos de data em qualquer nível
        if 'data' in key.lower() and ('criacao' in key.lower() or 'atualizacao' in key.lower() or 'ativacao' in key.lower() or 'inativacao' in key.lower()):
            return True
        return False
    
    def deep_clean_doc(obj, parent_path=""):
        """Remove campos de auditoria recursivamente"""
        if isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                current_path = f"{parent_path}.{key}" if parent_path else key
                
                # Ignorar campos de auditoria
                if should_ignore_field(key, parent_path):
                    continue
                    
                cleaned[key] = deep_clean_doc(value, current_path)
            return cleaned
        elif isinstance(obj, list):
            # Para listas, limpar cada item
            return [deep_clean_doc(item, parent_path) for item in obj]
        else:
            # Para valores primitivos, retornar como está
            return obj
    
    def normalize_for_comparison(obj):
        """Normaliza objeto para comparação consistente"""
        if isinstance(obj, dict):
            normalized = {}
            for key, value in obj.items():
                normalized[key] = normalize_for_comparison(value)
            return normalized
        elif isinstance(obj, list):
            # Para listas, normalizar cada item
            return [normalize_for_comparison(item) for item in obj]
        elif obj is None:
            return None
        elif isinstance(obj, (str, int, float, bool)):
            return obj
        elif hasattr(obj, 'isoformat'):  # datetime
            # Para datas, ignorar diferenças de microssegundos
            return obj.isoformat()[:19]  # Manter apenas até segundos
        else:
            return str(obj)
    
    try:
        # Limpar campos de auditoria
        clean_doc1 = deep_clean_doc(doc1)
        clean_doc2 = deep_clean_doc(doc2)
        
        # Normalizar para comparação
        norm_doc1 = normalize_for_comparison(clean_doc1)
        norm_doc2 = normalize_for_comparison(clean_doc2)
        
        # Comparar
        are_different = norm_doc1 != norm_doc2
        
        # Debug opcional - descomente para ver diferenças
        # if are_different:
        #     print(f"      🔍 Documentos diferentes após limpeza e normalização")
        
        return are_different
        
    except Exception as e:
        print(f"      ⚠️  Erro na comparação: {e}")
        # Em caso de erro, assumir que são diferentes para ser conservador
        return True

def save_changes_to_mongodb(changes: List[Dict]) -> bool:
    """Salva alterações no MongoDB apenas se os dados realmente mudaram"""
    if not changes:
        return True
    
    try:
        # Conectar ao MongoDB
        client = pymongo.MongoClient(MONGODB_CONFIG['connection_string'])
        db = client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        new_count = 0
        updated_count = 0
        unchanged_count = 0
        
        for change in changes:
            # Para cada alteração, buscar dados completos da unidade familiar
            unidade_id = change.get('record_id')
            schema = change.get('schema_to')  # Usar schema mais recente
            
            if unidade_id and schema:
                complete_data = get_unidade_familiar_complete_data(unidade_id, schema)
                if complete_data:
                    new_document = convert_to_mongodb_document(complete_data)
                    if new_document:
                        # Verificar se documento já existe
                        existing_doc = collection.find_one({'idUnidadeFamiliar': new_document['idUnidadeFamiliar']})
                        
                        if existing_doc:
                            # Documento existe - verificar se mudou
                            if documents_are_different(new_document, existing_doc):
                                # Documento mudou - atualizar
                                collection.replace_one(
                                    {'idUnidadeFamiliar': new_document['idUnidadeFamiliar']},
                                    new_document
                                )
                                updated_count += 1
                            else:
                                # Documento não mudou - ignorar
                                unchanged_count += 1
                        else:
                            # Documento novo - inserir
                            collection.insert_one(new_document)
                            new_count += 1
        
        # Relatório de resultados
        total_processed = new_count + updated_count + unchanged_count
        if total_processed > 0:
            print(f"   📊 Processamento concluído:")
            if new_count > 0:
                print(f"      ✨ {new_count} novas unidades familiares salvas")
            if updated_count > 0:
                print(f"      🔄 {updated_count} unidades familiares atualizadas")
            if unchanged_count > 0:
                print(f"      ⏭️  {unchanged_count} unidades familiares sem alterações (ignoradas)")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Erro ao salvar no MongoDB: {e}")
        import traceback
        print(traceback.format_exc())
        return False

def run_incremental_analysis(limit: int = None):
    """Executa análise incremental completa
    
    Args:
        limit: Limitar número de alterações por tabela (None = sem limite)
    """
    
    print("🔍 Análise Incremental CAF - Completa")
    print("=" * 60)
    
    # Configurar limite
    run_incremental_analysis._limit = limit
    if limit:
        print(f"⚠️  Limite configurado: {limit:,} alterações por tabela")
    else:
        print("🚀 Sem limite - processando TODAS as alterações")
    
    # Carregar configuração
    try:
        config = load_monitoring_config()
        print(f"✅ Configuração carregada: {len(config.get('tables', {}))} tabelas monitoradas")
    except Exception as e:
        print(f"❌ Erro ao carregar configuração: {e}")
        return
    
    # Obter schemas CAF
    schemas = get_caf_schemas()
    if len(schemas) < 2:
        print(f"❌ Schemas insuficientes para comparação. Encontrados: {schemas}")
        return
    
    print(f"📊 Schemas disponíveis: {schemas}")
    
    # Comparar schemas consecutivos
    total_changes = 0
    
    for i in range(len(schemas) - 1):
        schema1 = schemas[i]
        schema2 = schemas[i + 1]
        
        print(f"\n🔄 Comparando {schema1} → {schema2}")
        
        # Analisar cada tabela configurada
        for table_name in config.get('tables', {}).keys():
            changes = analyze_table_changes(schema1, schema2, table_name, config)
            
            if changes:
                # Salvar no MongoDB
                if save_changes_to_mongodb(changes):
                    total_changes += len(changes)
    
    print(f"\n🎉 Análise concluída!")
    print(f"📈 Total de alterações detectadas: {total_changes:,}")
    print(f"💾 Dados salvos na coleção: {MONGODB_CONFIG['collection']}")
    print(f"🔗 Acesse Mongo Express: http://localhost:8080")

if __name__ == "__main__":
    import sys
    
    # Verificar se foi passado um argumento de limite
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"🔧 Limite definido via argumento: {limit:,}")
        except ValueError:
            print("⚠️  Argumento inválido. Use: python run_caf_analysis.py [limite]")
            print("     Exemplo: python run_caf_analysis.py 1000")
    
    run_incremental_analysis(limit)

"""
Script de validação da configuração CAF
Verifica se as tabelas e colunas configuradas existem no banco de dados
"""

import sys
import yaml
from typing import Dict, List, Set
import psycopg2
from loguru import logger

def load_config(config_path: str) -> Dict:
    """Carrega configuração do arquivo YAML"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar configuração: {e}")
        return {}

def get_db_schema(schema_name: str) -> Dict[str, Set[str]]:
    """Obtém estrutura das tabelas do banco de dados"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="caf_analysis",
            user="caf_user",
            password="caf_password"
        )
        
        cursor = conn.cursor()
        
        # Buscar todas as tabelas e colunas do schema
        query = """
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
        """
        
        cursor.execute(query, (schema_name,))
        results = cursor.fetchall()
        
        # Organizar por tabela
        schema_info = {}
        for table_name, column_name in results:
            if table_name not in schema_info:
                schema_info[table_name] = set()
            schema_info[table_name].add(column_name)
        
        conn.close()
        return schema_info
        
    except Exception as e:
        logger.error(f"Erro ao conectar com banco: {e}")
        return {}

def validate_configuration(config_path: str, schema_name: str) -> bool:
    """Valida se a configuração está consistente com o banco"""
    
    logger.info(f"Validando configuração: {config_path}")
    logger.info(f"Schema do banco: {schema_name}")
    
    # Carregar configuração
    config = load_config(config_path)
    if not config or 'tables' not in config:
        logger.error("Configuração inválida ou sem seção 'tables'")
        return False
    
    # Obter estrutura do banco
    db_schema = get_db_schema(schema_name)
    if not db_schema:
        logger.error("Não foi possível obter estrutura do banco")
        return False
    
    logger.info(f"Encontradas {len(db_schema)} tabelas no banco")
    
    # Validar cada tabela configurada
    all_valid = True
    
    for table_name, table_config in config['tables'].items():
        logger.info(f"\n--- Validando tabela: {table_name} ---")
        
        # Verificar se tabela existe no banco
        if table_name not in db_schema:
            logger.error(f"❌ Tabela '{table_name}' não encontrada no banco")
            all_valid = False
            continue
        
        logger.info(f"✅ Tabela '{table_name}' encontrada")
        
        # Verificar primary key
        primary_key = table_config.get('primary_key')
        if not primary_key:
            logger.error(f"❌ Primary key não definida para tabela '{table_name}'")
            all_valid = False
        elif primary_key not in db_schema[table_name]:
            logger.error(f"❌ Primary key '{primary_key}' não encontrada na tabela '{table_name}'")
            all_valid = False
        else:
            logger.info(f"✅ Primary key '{primary_key}' válida")
        
        # Verificar colunas monitoradas
        columns = table_config.get('columns', [])
        if not columns:
            logger.warning(f"⚠️ Nenhuma coluna configurada para monitoramento na tabela '{table_name}'")
        
        for column in columns:
            if column not in db_schema[table_name]:
                logger.error(f"❌ Coluna '{column}' não encontrada na tabela '{table_name}'")
                all_valid = False
            else:
                logger.info(f"✅ Coluna '{column}' válida")
        
        # Mostrar colunas disponíveis mas não monitoradas
        available_columns = db_schema[table_name] - set(columns) - {primary_key}
        if available_columns:
            logger.info(f"📋 Colunas disponíveis não monitoradas: {', '.join(sorted(available_columns))}")
    
    # Mostrar tabelas disponíveis mas não configuradas
    configured_tables = set(config['tables'].keys())
    available_tables = set(db_schema.keys())
    unconfigured_tables = available_tables - configured_tables
    
    if unconfigured_tables:
        logger.info(f"\n📋 Tabelas disponíveis não configuradas ({len(unconfigured_tables)}):")
        for table in sorted(unconfigured_tables):
            logger.info(f"  - {table}")
    
    # Resultado final
    if all_valid:
        logger.info(f"\n✅ Configuração válida! Todas as tabelas e colunas estão corretas.")
    else:
        logger.error(f"\n❌ Configuração inválida! Corrija os erros acima.")
    
    return all_valid

def main():
    """Função principal"""
    
    if len(sys.argv) < 2:
        print("Uso: python validate_config.py <schema_name> [config_path]")
        print("Exemplo: python validate_config.py caf_20250301")
        print("Exemplo: python validate_config.py caf_20250301 config/monitoring_config.yaml")
        sys.exit(1)
    
    schema_name = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else "config/monitoring_config.yaml"
    
    logger.add("logs/config_validation.log", rotation="1 MB")
    
    success = validate_configuration(config_path, schema_name)
    
    if success:
        print(f"\n✅ Configuração válida para schema '{schema_name}'")
        sys.exit(0)
    else:
        print(f"\n❌ Configuração inválida para schema '{schema_name}'")
        sys.exit(1)

if __name__ == "__main__":
    main()

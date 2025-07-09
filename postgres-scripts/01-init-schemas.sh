#!/bin/bash
# Script de inicialização do PostgreSQL para dumps CAF

echo "🚀 Iniciando configuração do banco para análise de dumps CAF..."

# Conectar como superuser
export PGPASSWORD="$POSTGRES_PASSWORD"

# Criar schemas baseados nos dumps disponíveis
echo "📁 Criando schemas para os dumps..."

# Schema para dump inicial (se houver)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Schema para dados de referência
    CREATE SCHEMA IF NOT EXISTS caf_reference;
    COMMENT ON SCHEMA caf_reference IS 'Schema de referência para comparações';
    
    -- Schema para dumps históricos organizados por data
    CREATE SCHEMA IF NOT EXISTS caf_20250301;
    COMMENT ON SCHEMA caf_20250301 IS 'Dump CAF de 2025-03-01';
    
    -- Schema para análise e comparação
    CREATE SCHEMA IF NOT EXISTS caf_analysis;
    COMMENT ON SCHEMA caf_analysis IS 'Tabelas de análise e comparação';
    
    -- Tabela para metadados dos dumps
    CREATE TABLE caf_analysis.dump_metadata (
        id SERIAL PRIMARY KEY,
        dump_file VARCHAR(255) NOT NULL,
        schema_name VARCHAR(100) NOT NULL,
        dump_date DATE,
        imported_at TIMESTAMP DEFAULT NOW(),
        file_size_mb NUMERIC(10,2),
        tables_count INTEGER,
        records_count BIGINT,
        notes TEXT
    );
    
    -- Tabela para tracking de comparações
    CREATE TABLE caf_analysis.comparison_log (
        id SERIAL PRIMARY KEY,
        source_schema VARCHAR(100) NOT NULL,
        target_schema VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        changes_detected INTEGER DEFAULT 0,
        comparison_date TIMESTAMP DEFAULT NOW(),
        status VARCHAR(50) DEFAULT 'pending'
    );
    
    -- Índices para performance
    CREATE INDEX idx_dump_metadata_schema ON caf_analysis.dump_metadata(schema_name);
    CREATE INDEX idx_dump_metadata_date ON caf_analysis.dump_metadata(dump_date);
    CREATE INDEX idx_comparison_log_schemas ON caf_analysis.comparison_log(source_schema, target_schema);
    
    -- Função para extrair data do nome do arquivo
    CREATE OR REPLACE FUNCTION caf_analysis.extract_date_from_filename(filename TEXT)
    RETURNS DATE AS \$\$
    BEGIN
        -- Extrair data do formato: dump-caf_mapa-YYYYMMDD-YYYYMMDDHHMI.sql
        -- Exemplo: dump-caf_mapa-20250301-202506151151.sql
        RETURN TO_DATE(
            SUBSTRING(filename FROM 'dump-caf_mapa-(\d{8})-\d+\.sql'),
            'YYYYMMDD'
        );
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END;
    \$\$ LANGUAGE plpgsql;
    
    -- Função para gerar nome do schema baseado na data
    CREATE OR REPLACE FUNCTION caf_analysis.generate_schema_name(dump_date DATE)
    RETURNS TEXT AS \$\$
    BEGIN
        IF dump_date IS NULL THEN
            RETURN 'caf_' || TO_CHAR(NOW(), 'YYYYMMDD');
        ELSE
            RETURN 'caf_' || TO_CHAR(dump_date, 'YYYYMMDD');
        END IF;
    END;
    \$\$ LANGUAGE plpgsql;
    
    -- View para listar dumps importados
    CREATE OR REPLACE VIEW caf_analysis.v_dumps_summary AS
    SELECT 
        schema_name,
        dump_file,
        dump_date,
        imported_at,
        file_size_mb,
        tables_count,
        records_count,
        CASE 
            WHEN dump_date = (SELECT MAX(dump_date) FROM caf_analysis.dump_metadata) 
            THEN 'MAIS_RECENTE'
            ELSE 'HISTORICO'
        END as status
    FROM caf_analysis.dump_metadata
    ORDER BY dump_date DESC;
    
    -- Conceder permissões
    GRANT USAGE ON SCHEMA caf_reference TO "$POSTGRES_USER";
    GRANT USAGE ON SCHEMA caf_20250301 TO "$POSTGRES_USER";
    GRANT USAGE ON SCHEMA caf_analysis TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA caf_analysis TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA caf_analysis TO "$POSTGRES_USER";
    
EOSQL

echo "✅ Configuração inicial do banco concluída!"
echo "📊 Schemas criados:"
echo "   - caf_reference: Dados de referência"
echo "   - caf_20250301: Dump de 2025-03-01"
echo "   - caf_analysis: Análise e metadados"
echo ""
echo "🔧 Funções utilitárias criadas:"
echo "   - extract_date_from_filename(): Extrai data do nome do arquivo"
echo "   - generate_schema_name(): Gera nome do schema baseado na data"
echo ""
echo "📋 Tabelas de controle criadas em caf_analysis:"
echo "   - dump_metadata: Metadados dos dumps importados"
echo "   - comparison_log: Log de comparações realizadas"

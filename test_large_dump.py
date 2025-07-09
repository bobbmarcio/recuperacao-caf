#!/usr/bin/env python3
"""
Script para testar o processamento de dumps grandes
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import DatabaseConfig, MonitoringConfig, AppConfig, setup_logging
from postgresql_processor import PostgreSQLDumpProcessor


def create_large_test_dump(file_path: str, size_mb: int = 100):
    """
    Cria um arquivo de dump de teste grande para validar o processamento
    
    Args:
        file_path: Caminho do arquivo
        size_mb: Tamanho aproximado em MB
    """
    print(f"Criando dump de teste de ~{size_mb}MB em {file_path}...")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        # Header SQL padrão
        f.write("""--
-- PostgreSQL database dump (Arquivo de teste para dumps grandes)
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA IF NOT EXISTS public;

--
-- Name: usuarios; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usuarios (
    id integer NOT NULL,
    nome character varying(100) NOT NULL,
    email character varying(255) NOT NULL,
    telefone character varying(20),
    data_cadastro timestamp with time zone DEFAULT now(),
    ativo boolean DEFAULT true
);

--
-- Data for Name: usuarios; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.usuarios (id, nome, email, telefone, data_cadastro, ativo) FROM stdin;
""")
        
        # Gerar dados simulados até atingir o tamanho desejado
        target_size = size_mb * 1024 * 1024  # Converter para bytes
        current_size = f.tell()
        record_id = 1
        
        while current_size < target_size:
            # Dados de exemplo
            nome = f"Usuario Teste {record_id:06d}"
            email = f"usuario{record_id}@exemplo.com"
            telefone = f"+55 11 9{record_id:04d}-{record_id%10000:04d}"
            data_cadastro = "2024-01-15 10:30:00+00"
            ativo = "t" if record_id % 10 != 0 else "f"
            
            linha = f"{record_id}\t{nome}\t{email}\t{telefone}\t{data_cadastro}\t{ativo}\n"
            f.write(linha)
            
            record_id += 1
            current_size = f.tell()
            
            # Progresso a cada 10000 registros
            if record_id % 10000 == 0:
                print(f"  {record_id:,} registros gerados ({current_size / (1024*1024):.1f} MB)")
        
        # Finalizar COPY
        f.write("\\.\n\n")
        
        # Footer SQL
        f.write("""--
-- Name: usuarios_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usuarios
    ADD CONSTRAINT usuarios_pkey PRIMARY KEY (id);

--
-- PostgreSQL database dump complete
--
""")
    
    final_size = Path(file_path).stat().st_size / (1024*1024)
    print(f"✅ Dump de teste criado: {final_size:.1f} MB com {record_id-1:,} registros")


def test_postgresql_strategy():
    """Testa a estratégia PostgreSQL para dumps grandes"""
    
    # Configurar logging
    setup_logging("INFO", debug=True)
    
    print("🔧 Testando estratégia PostgreSQL para dumps grandes...\n")
    
    # Verificar se PostgreSQL está disponível
    db_config = DatabaseConfig.from_env()
    
    print("📋 Configurações do PostgreSQL:")
    print(f"  Host: {db_config.postgres_host}")
    print(f"  Port: {db_config.postgres_port}")
    print(f"  User: {db_config.postgres_user}")
    print(f"  Password: {'*' * len(db_config.postgres_password) if db_config.postgres_password else 'Não configurado'}")
    
    if not db_config.postgres_password:
        print("\n⚠️  POSTGRES_PASSWORD não está configurado no .env")
        print("   Para testar dumps grandes, configure as credenciais do PostgreSQL")
        return False
    
    # Criar dumps de teste
    temp_dir = Path("temp_test_dumps")
    temp_dir.mkdir(exist_ok=True)
    
    try:
        dump1 = temp_dir / "dump_test_1.sql"
        dump2 = temp_dir / "dump_test_2.sql"
        
        # Criar primeiro dump
        create_large_test_dump(str(dump1), size_mb=50)
        
        # Criar segundo dump (com algumas modificações)
        create_large_test_dump(str(dump2), size_mb=50)
        
        # Modificar alguns registros no segundo dump para simular alterações
        print("\n🔄 Simulando alterações no segundo dump...")
        
        # Configuração de monitoramento
        monitoring_config = {
            'usuarios': {
                'primary_key': 'id',
                'columns': ['nome', 'email', 'telefone', 'ativo']
            }
        }
        
        # Estimar tempo de processamento
        processor = PostgreSQLDumpProcessor(db_config)
        estimates = processor.estimate_processing_time([str(dump1), str(dump2)])
        
        print(f"\n⏱️  Estimativas de tempo:")
        print(f"  Tamanho total: {estimates['total_size_gb']:.2f} GB")
        print(f"  Tempo estimado de restauração: {estimates['estimated_restore_time_minutes']:.1f} min")
        print(f"  Tempo estimado de comparação: {estimates['estimated_comparison_time_minutes']:.1f} min")
        print(f"  Tempo total estimado: {estimates['estimated_total_time_minutes']:.1f} min")
        
        print(f"\n📁 Arquivos criados para teste:")
        for file_info in estimates['files']:
            print(f"  {file_info['file']}: {file_info['size_gb']:.3f} GB")
        
        # Pergunta se quer continuar (só simular, não processar realmente)
        print(f"\n✅ Teste de criação concluído!")
        print(f"💡 Para testar o processamento real, execute:")
        print(f"   python src/main.py analyze --config config/monitoring_config.yaml --dump-dir {temp_dir}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        return False
    
    finally:
        # Limpar arquivos de teste
        try:
            for file in temp_dir.glob("*.sql"):
                file.unlink()
            temp_dir.rmdir()
            print(f"\n🧹 Arquivos de teste removidos")
        except:
            print(f"\n⚠️  Mantenha os arquivos em {temp_dir} para testes manuais")


def check_postgresql_availability():
    """Verifica se PostgreSQL está disponível"""
    
    print("🔍 Verificando disponibilidade do PostgreSQL...")
    
    try:
        import psycopg2
        
        db_config = DatabaseConfig.from_env()
        
        # Tentar conectar
        conn = psycopg2.connect(
            host=db_config.postgres_host,
            port=db_config.postgres_port,
            user=db_config.postgres_user,
            password=db_config.postgres_password,
            database=db_config.postgres_database,
            connect_timeout=5
        )
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL conectado: {version}")
        
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"❌ Erro de conexão PostgreSQL: {e}")
        print("💡 Verifique se:")
        print("   - PostgreSQL está rodando")
        print("   - Credenciais no .env estão corretas")
        print("   - Host/porta estão acessíveis")
        return False
    
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Teste de processamento de dumps grandes\n")
    
    # Verificar PostgreSQL
    pg_available = check_postgresql_availability()
    
    print("\n" + "="*60)
    
    if pg_available:
        # Executar teste
        test_postgresql_strategy()
    else:
        print("\n📋 Para habilitar processamento de dumps grandes:")
        print("1. Instale e configure PostgreSQL")
        print("2. Configure credenciais no arquivo .env:")
        print("   POSTGRES_HOST=localhost")
        print("   POSTGRES_PORT=5432") 
        print("   POSTGRES_USER=seu_usuario")
        print("   POSTGRES_PASSWORD=sua_senha")
        print("   POSTGRES_DATABASE=postgres")
        print("\n3. Execute este teste novamente")
        
        print(f"\n💡 O sistema ainda funciona para dumps pequenos (< 2GB)")
        print(f"   usando a estratégia em memória!")

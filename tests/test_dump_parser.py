"""
Testes para o módulo de parsing de dumps PostgreSQL
"""

import unittest
import tempfile
import os
from pathlib import Path
import sys

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from dump_parser import PostgreSQLDumpParser, list_dump_files


class TestPostgreSQLDumpParser(unittest.TestCase):
    
    def setUp(self):
        self.parser = PostgreSQLDumpParser()
        
        # Criar dump de exemplo
        self.sample_dump = """
-- PostgreSQL database dump

-- Database: test_db
-- PostgreSQL 13.0 on x86_64

CREATE TABLE public.usuarios (
    id integer NOT NULL,
    email character varying(255),
    status character varying(50),
    created_at timestamp without time zone,
    PRIMARY KEY (id)
);

COPY public.usuarios (id, email, status, created_at) FROM stdin;
1	user1@test.com	active	2023-01-01 10:00:00
2	user2@test.com	inactive	2023-01-02 11:00:00
3	user3@test.com	active	2023-01-03 12:00:00
\\.

CREATE TABLE public.produtos (
    id integer NOT NULL,
    nome character varying(255),
    preco numeric(10,2),
    PRIMARY KEY (id)
);

COPY public.produtos (id, nome, preco) FROM stdin;
1	Produto A	29.99
2	Produto B	49.99
\\.
"""
    
    def test_list_dump_files(self):
        """Testa listagem de arquivos de dump"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Criar alguns arquivos de teste
            (Path(temp_dir) / "dump1.sql").write_text(self.sample_dump)
            (Path(temp_dir) / "dump2.sql").write_text(self.sample_dump)
            (Path(temp_dir) / "other.txt").write_text("not a dump")
            
            dump_files = list_dump_files(temp_dir)
            
            self.assertEqual(len(dump_files), 2)
            self.assertTrue(all(f.endswith('.sql') for f in dump_files))
    
    def test_parse_dump_file(self):
        """Testa parsing de arquivo de dump"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(self.sample_dump)
            temp_file = f.name
        
        try:
            tables_data = self.parser.parse_dump_file(temp_file, ['usuarios', 'produtos'])
            
            # Verificar se as tabelas foram encontradas
            self.assertIn('usuarios', tables_data)
            self.assertIn('produtos', tables_data)
            
            # Verificar dados da tabela usuarios
            usuarios_data = tables_data['usuarios']
            self.assertEqual(usuarios_data.name, 'usuarios')
            self.assertEqual(len(usuarios_data.data), 3)
            self.assertIn('id', usuarios_data.columns)
            self.assertIn('email', usuarios_data.columns)
            
            # Verificar dados da tabela produtos
            produtos_data = tables_data['produtos']
            self.assertEqual(produtos_data.name, 'produtos')
            self.assertEqual(len(produtos_data.data), 2)
            
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()

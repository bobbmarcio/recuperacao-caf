<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# Instruções para o GitHub Copilot

Este é um projeto Python para análise incremental de dumps PostgreSQL com integração ao MongoDB.

## Contexto do Projeto
- **Objetivo**: Comparar dumps PostgreSQL sequenciais e detectar alterações em colunas específicas
- **Funcionalidade**: Quando alterações são encontradas, inserir os dados antigos em um MongoDB para auditoria
- **Arquitetura**: Projeto modular com separação clara de responsabilidades

## Padrões de Código
- Use **type hints** em todas as funções
- Implemente **logging** detalhado com loguru
- Use **dataclasses** para estruturas de dados
- Implemente **tratamento de erro** robusto
- Siga os padrões **PEP 8**

## Estrutura Preferida
- **dump_parser.py**: Classes para parsing de dumps PostgreSQL
- **data_comparator.py**: Lógica de comparação incremental
- **mongo_inserter.py**: Integração com MongoDB
- **config.py**: Configurações e validação
- **main.py**: CLI e orquestração

## Tecnologias Utilizadas
- **PostgreSQL**: psycopg2-binary para conexão e parsing
- **MongoDB**: pymongo para inserção de dados de auditoria
- **Pandas**: Para manipulação eficiente de dados
- **Click**: Para interface de linha de comando
- **Loguru**: Para logging estruturado
- **YAML**: Para arquivos de configuração

## Casos de Uso
- Monitoramento de mudanças em tabelas críticas
- Auditoria de alterações de dados
- Recuperação de estados anteriores de dados
- Análise forense de modificações no banco

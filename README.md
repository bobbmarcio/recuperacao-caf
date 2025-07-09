# Análise Incremental de Dumps PostgreSQL

Este projeto permite analisar dumps PostgreSQL de forma incremental, comparando dados entre dumps consecutivos e inserindo alterações em um banco MongoDB para auditoria.

## ✨ Funcionalidades

- **🔄 Análise incremental**: Compara dumps PostgreSQL sequencialmente
- **🔍 Detecção de alterações**: Identifica mudanças em colunas específicas de tabelas configuradas
- **📊 Auditoria no MongoDB**: Insere dados antigos quando alterações são detectadas
- **⚙️ Configuração flexível**: Define quais tabelas e colunas monitorar
- **📝 Logging detalhado**: Acompanha todo o processo de análise
- **🐳 Ambiente Docker**: MongoDB + PostgreSQL + Interfaces Web unificadas
- **🚀 Gerenciamento automático**: Scripts para controle completo do ambiente

## 🚀 Início Rápido

### 1. Iniciar Ambiente
```powershell
# Iniciar ambiente completo (MongoDB + PostgreSQL + Interfaces Web)
python manage-environment.py start

# Verificar se tudo está funcionando
python manage-environment.py status
```

### 2. Acessar Interfaces Web
- **Mongo Express**: http://localhost:8080 (MongoDB)
- **PgAdmin**: http://localhost:8082 (PostgreSQL)
  - Email: `admin@caf.local` / Senha: `admin123`

### 3. Importar Dumps e Analisar
```powershell
# Importar dumps automaticamente
python import-dumps-caf.py

# Executar análise incremental
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/
```

### 4. Parar Ambiente
```powershell
python manage-environment.py stop
```

## 📁 Estrutura do Projeto

```
recuperacao-caf/
├── src/                        # Código da aplicação principal
│   ├── dump_parser.py         # Parser de dumps PostgreSQL
│   ├── data_comparator.py     # Comparador de dados incrementais
│   ├── mongo_inserter.py      # Integração com MongoDB
│   ├── config.py              # Configurações do projeto
│   └── main.py                # CLI e orquestração
├── config/
│   └── monitoring_config.yaml # Configuração de tabelas/colunas
├── dumps/                      # Diretório para dumps PostgreSQL
├── logs/                       # Logs da aplicação
├── tests/                      # Testes unitários
├── docker-compose.yml          # Ambiente unificado
├── manage-environment.py       # Gerenciamento do ambiente
├── import-dumps-caf.py        # Importação automática de dumps
├── requirements.txt
└── README.md
```

## 🔧 Pré-requisitos

- **Python 3.11+**
- **Docker Desktop** (para ambientes containerizados)
- **Git** (para clone do repositório)

## 📦 Instalação

1. **Clone o repositório**:
```powershell
git clone <repo-url>
cd recuperacao-caf
```

2. **Instale as dependências Python**:
```powershell
pip install -r requirements.txt
```

3. **Inicie o ambiente Docker**:
```powershell
python manage-environment.py start
```

## 🎯 Uso Detalhado

### Gerenciamento do Ambiente

```powershell
# Iniciar ambiente completo
python manage-environment.py start

# Verificar status
python manage-environment.py status

# Ver logs
python manage-environment.py logs

# Parar ambiente
python manage-environment.py stop

# Resetar ambiente (cuidado: apaga dados!)
python manage-environment.py reset
```

### Importação de Dumps

```powershell
# Importação automática (detecta data do nome do arquivo)
python import-dumps-caf.py

# Importação de dump específico
python import-dumps-caf.py --dump-file "dumps/dump-caf_mapa-20250301-202506151151.sql"
```

### Análise de Dumps

```powershell
# Análise incremental básica
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/

# Análise com logs detalhados
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/ --verbose
```

## ⚙️ Configuração

### Configuração de Monitoramento

Edite o arquivo `config/monitoring_config.yaml` para definir quais tabelas e colunas monitorar:

```yaml
tables:
  usuarios:
    primary_key: id
    monitored_columns:
      - email
      - status
      - updated_at
  
  contratos:
    primary_key: id
    monitored_columns:
      - valor
      - status
      - data_vencimento
```

### Configurações do Ambiente

O ambiente está pré-configurado, mas você pode ajustar as configurações editando o `docker-compose.yml`:

**PostgreSQL**: 
- Porta: `5433`
- Database: `caf_analysis`
- User: `caf_user`
- Password: `caf_password123`

**MongoDB**:
- Porta: `27017`
- Database: `audit_db`
- Admin User: `admin`
- Admin Password: `admin123`

## 🔗 Acesso aos Serviços

- **Mongo Express**: http://localhost:8080
- **PgAdmin**: http://localhost:8082
  - Email: `admin@caf.local`
  - Senha: `admin123`

## 📊 Exemplos de Uso

### Fluxo Completo

1. **Preparar ambiente**:
```powershell
python manage-environment.py start
python manage-environment.py status
```

2. **Importar dumps**:
```powershell
# Coloque os dumps na pasta dumps/
python import-dumps-caf.py
```

3. **Executar análise**:
```powershell
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/
```

4. **Verificar resultados** nas interfaces web ou logs

5. **Finalizar**:
```powershell
python manage-environment.py stop
```

## 🐛 Troubleshooting

### Problemas Comuns

1. **Docker não encontrado**:
   - Certifique-se de que o Docker Desktop está instalado e rodando

2. **Portas ocupadas**:
   - Verifique se as portas 27017, 5433, 8080, 8082 estão livres

3. **Erro na importação de dumps**:
   - Use sempre `docker exec` para importar dumps grandes
   - Exemplo: `docker exec -i postgres-caf-dumps psql -U caf_user -d caf_analysis < dump.sql`

### Comandos de Diagnóstico

```powershell
# Ver containers rodando
docker ps

# Ver logs específicos
docker logs postgres-caf-dumps
docker logs recuperacao-caf-mongo

# Acessar shell do PostgreSQL
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis
```

## 📚 Documentação Adicional

- **[COMANDOS_RAPIDOS.md](COMANDOS_RAPIDOS.md)**: Referência rápida de comandos
- **[README_POSTGRESQL_CAF.md](README_POSTGRESQL_CAF.md)**: Guia detalhado do PostgreSQL
- **[GUIA_DUMPS_GRANDES.md](GUIA_DUMPS_GRANDES.md)**: Guia para dumps grandes

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.

## Como Funciona

1. **Parse dos Dumps**: Cada dump PostgreSQL é analisado e os dados das tabelas configuradas são extraídos
2. **Comparação Incremental**: Os dados são comparados com o dump anterior
3. **Detecção de Alterações**: Mudanças nas colunas monitoradas são identificadas
4. **Auditoria**: Dados antigos são inseridos no MongoDB com timestamp e metadados

## Desenvolvimento

Para executar em modo de desenvolvimento:

```bash
python src/main.py --debug analyze --dump-dir dumps/
```

## Testes

```bash
python -m pytest tests/
```

## 📊 Estrutura MongoDB

O sistema salva as alterações detectadas no MongoDB seguindo uma estrutura específica baseada no modelo do sistema CAF. Para detalhes completos, consulte [`ESTRUTURA_MONGODB.md`](./ESTRUTURA_MONGODB.md).

### Exemplo de Documento de Auditoria

```json
{
  "_versao": 1,
  "idUnidadeFamiliar": "8ab52a30-89c1-436b-b006-62a9d48153cb",
  "possuiMaoObraContratada": false,
  "dataValidade": "2028-01-28",
  "dataAtualizacao": {
    "$date": "2025-01-28T18:03:40.627Z"
  },
  "tipoTerreno": {
    "id": 1,
    "descricao": "Agricultura, Pecuária e Outras atividades"
  },
  "audit_metadata": {
    "change_type": "update",
    "changed_field": "possuiMaoObraContratada", 
    "old_value": true,
    "new_value": false,
    "change_timestamp": "2025-06-26T10:30:00Z",
    "dump_source": "caf_20250301",
    "dump_target": "caf_20250401"
  }
}
```

### Mapeamento de Campos SQL → MongoDB

| Campo SQL | Campo MongoDB | Tipo |
|-----------|---------------|------|
| `id_unidade_familiar` | `idUnidadeFamiliar` | string (UUID) |
| `st_possui_mao_obra` | `possuiMaoObraContratada` | boolean |
| `dt_atualizacao` | `dataAtualizacao` | {$date: ISO_STRING} |
| `id_tipo_terreno_ufpr` | `tipoTerreno` | {id, descricao} |

Ver [mapeamento completo](./ESTRUTURA_MONGODB.md#mapeamento-de-campos-sql--mongodb).

## 🔧 Configuração Avançada

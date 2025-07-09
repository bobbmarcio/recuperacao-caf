# 🗜️ Guia para Dumps Grandes e Comprimidos (GZIP)

## � PROBLEMA IDENTIFICADO

O arquivo `dump-caf_mapa-20250301-202506151151.sql` tem extensão `.sql` mas é **comprimido com gzip**!

### ❌ Erro Original:
```
psql: error: invalid command \�����l�c�������O��-k�`���`K�������omi
```

### ✅ Causa:
Tentativa de importar arquivo binário (gzip) como texto SQL.

## � Como Identificar Arquivo Comprimido

```powershell
# Verificar primeiros bytes
(Get-Content "dumps\dump-caf_mapa-20250301-202506151151.sql" -TotalCount 3 -Encoding Byte | ForEach-Object { [System.Convert]::ToString($_, 16).PadLeft(2, '0') }) -join ' '

# Resultado: "1f 8b 08" = GZIP comprimido ✅
# Se fosse texto SQL normal veria caracteres legíveis
```

## ✅ SOLUÇÃO: Comando Correto

### Método Recomendado - Importação Direta:
```powershell
# 1. Criar schema
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# 2. Importar descomprimindo na hora
docker exec postgres-caf-dumps sh -c "
export PGPASSWORD='caf_password123'
gunzip -c /dumps/dump-caf_mapa-20250301-202506151151.sql | \
grep -v 'SET transaction_timeout' | \
sed 's/public\./caf_20250301\./g; s/SCHEMA public/SCHEMA caf_20250301/g' | \
psql -U caf_user -d caf_analysis -v ON_ERROR_STOP=1 --single-transaction
"
```

### Método Alternativo - Descomprimir Primeiro:
```powershell
# Se preferir descomprimir primeiro (usa mais espaço)
docker exec postgres-caf-dumps sh -c "gunzip -c /dumps/dump-caf_mapa-20250301-202506151151.sql > /tmp/dump_descomprimido.sql"

# Depois importar
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f /tmp/dump_descomprimido.sql

# Limpar temporário
docker exec postgres-caf-dumps rm /tmp/dump_descomprimido.sql
```

#### Windows:
```bash
# Opção 1: Download oficial
https://www.postgresql.org/download/windows/

# Opção 2: Via Chocolatey
choco install postgresql

# Opção 3: Via Scoop
scoop install postgresql
```

#### Via Docker (Recomendado):
```bash
# Subir PostgreSQL temporário
docker run -d --name postgres-temp \
  -e POSTGRES_PASSWORD=temp123 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgres:15

# Verificar se está rodando
docker ps
```

### 2. Configurar Credenciais

Edite o arquivo `.env`:

```env
# PostgreSQL para processamento de dumps grandes
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=temp123
POSTGRES_DATABASE=postgres

# MongoDB (já configurado)
MONGODB_CONNECTION_STRING=mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db
MONGODB_DATABASE=audit_db
MONGODB_COLLECTION=data_changes
```

### 3. Testar Configuração

```bash
# Testar PostgreSQL
python test_large_dump.py

# Se conectar corretamente, testar processamento
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/
```

## 🚀 Como Funciona o Processamento de Dumps Grandes

### Fluxo Automático:
1. **Detecção**: Sistema verifica tamanho total dos dumps
2. **Decisão**: Se > 2GB → usa PostgreSQL, senão → usa memória
3. **Processamento**:
   - Cria bancos temporários para cada dump
   - Restaura dumps usando `psql`/`pg_restore`
   - Compara dados via SQL otimizado
   - Remove bancos temporários
4. **Auditoria**: Insere alterações no MongoDB

### Estimativas de Performance:
- **Restauração**: ~120 segundos por GB
- **Comparação**: ~30 segundos por GB
- **Total para 5GB**: ~12.5 minutos

### Exemplo de Execução:
```bash
# Dump de 5.2GB
📁 Encontrados 2 dumps para análise
📊 Tamanho total: 5.20 GB
🔄 Usando estratégia PostgreSQL para arquivos grandes...
⏱️  Tempo estimado: 12.8 minutos
📝 Detectadas 1,245 alterações
✅ Alterações inseridas no MongoDB
🎉 Análise concluída!
```

## 🎯 Vantagens da Estratégia PostgreSQL

### Performance:
- ✅ Processa 5-6GB em ~15 minutos
- ✅ Usa comparação SQL nativa (muito rápida)
- ✅ Não consome RAM excessiva

### Escalabilidade:
- ✅ Suporta dumps de qualquer tamanho
- ✅ Processamento em paralelo de tabelas
- ✅ Limpeza automática de recursos

### Precisão:
- ✅ Parsing nativo do PostgreSQL
- ✅ Comparação exata com `IS DISTINCT FROM`
- ✅ Suporte a todos os tipos de dados

## 🔧 Comandos Úteis

### Análise Completa:
```bash
# Processar todos os dumps
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/

# Com logs detalhados
python src/main.py --debug analyze --config config/monitoring_config.yaml --dump-dir dumps/
```

### Consultas:
```bash
# Ver todas as alterações
python src/main.py query --limit 50

# Filtrar por tabela
python src/main.py query --table usuarios --limit 20

# Filtrar por coluna
python src/main.py query --table usuarios --column email --limit 10
```

### Docker PostgreSQL:
```bash
# Subir PostgreSQL temporário
docker run -d --name postgres-temp -e POSTGRES_PASSWORD=temp123 -p 5432:5432 postgres:15

# Parar e remover
docker stop postgres-temp && docker rm postgres-temp
```

## 📊 Monitoramento de Progresso

Durante o processamento de dumps grandes, você verá:

```
🔧 Criando banco temporário: temp_dump_0_1704123456
📁 Dump restaurado com sucesso no banco temp_dump_0_1704123456
🔍 Comparando dump_inicial.sql → dump_alteracoes.sql
📊 Tabela usuarios: 834 alterações detectadas
📊 Tabela produtos: 411 alterações detectadas
🧹 Limpando bancos temporários...
✅ Processamento concluído: 1,245 alterações detectadas
```

## ⚠️ Considerações Importantes

### Recursos:
- **Disco**: Bancos temporários ocupam ~2x o tamanho do dump
- **CPU**: Processo intensivo durante restauração
- **Rede**: Se PostgreSQL não for local

### Segurança:
- Bancos temporários são automaticamente removidos
- Credenciais ficam apenas no `.env` local
- Dados não ficam persistidos no PostgreSQL

### Fallback:
- Se PostgreSQL não estiver disponível, sistema usa estratégia em memória
- Aviso automático quando dumps são muito grandes para memória

---

🎉 **Pronto!** Seu sistema agora pode processar dumps de qualquer tamanho eficientemente!

## 🤖 Scripts Automatizados para Importação em Lote

Criamos scripts que automatizam a importação de **todos** os dumps CAF encontrados em `./dumps`:

### Script Python (Recomendado):
```bash
# Importar todos os dumps CAF automaticamente
python import-all-caf-dumps.py

# Ver progresso detalhado
python import-all-caf-dumps.py --verbose
```

### Script PowerShell:
```powershell
# Importar todos os dumps CAF
.\import-all-dumps.ps1

# Importar pulando já existentes
.\import-all-dumps.ps1 -SkipExisting

# Forçar reimportação de todos
.\import-all-dumps.ps1 -Force
```

### ✅ O que os Scripts Fazem Automaticamente:

1. **Detectam tipo de arquivo**: Gzip ou SQL texto
2. **Extraem data do nome**: `dump-caf_mapa-20250301-...` → `2025-03-01`
3. **Criam schema por data**: `caf_20250301`, `caf_20250401`, etc.
4. **Filtram comandos problemáticos**: `transaction_timeout`, etc.
5. **Ajustam schemas**: `public.` → `"caf_20250301".`
6. **Registram metadados**: Tabela `dump_metadata` para auditoria
7. **Pulam já importados**: Evita reimportações desnecessárias

### 📊 Exemplo de Execução:
```
=== Iniciando importação automática de dumps CAF ===
Encontrados 2 dumps CAF para processamento

--- Processando dump: dump-caf_mapa-20250301-202506151151.sql ---
Data: 2025-03-01, Schema: caf_20250301
Arquivo é comprimido (gzip)
✅ Schema 'caf_20250301' criado com sucesso
✅ Dump importado com sucesso para schema 'caf_20250301'

--- Processando dump: dump-caf_mapa-20250401-202506161955.sql ---
Data: 2025-04-01, Schema: caf_20250401
Arquivo é comprimido (gzip)
✅ Schema 'caf_20250401' criado com sucesso
✅ Dump importado com sucesso para schema 'caf_20250401'

=== Resumo da Importação ===
Dumps processados: 2
Importações bem-sucedidas: 2
Importações puladas (já existentes): 0
Importações falharam: 0
```

### 🔍 Verificar Schemas Importados:
```sql
-- Listar todos os schemas CAF
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name LIKE 'caf_%' ORDER BY schema_name;

-- Verificar metadados das importações
SELECT * FROM dump_metadata ORDER BY import_timestamp DESC;

-- Contar tabelas por schema
SELECT 
    schemaname, 
    COUNT(*) as table_count 
FROM pg_tables 
WHERE schemaname LIKE 'caf_%' 
GROUP BY schemaname 
ORDER BY schemaname;
```

---

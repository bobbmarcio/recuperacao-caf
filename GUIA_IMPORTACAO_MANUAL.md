# 📥 Guia de Importação Manual de Dumps PostgreSQL

## � IMPORTANTE: Detecção de Tipo de Arquivo

Primeiro, identifique se seu dump é **texto** ou **comprimido**:

```powershell
# Verificar primeiros bytes
(Get-Content "dumps\seu-arquivo.sql" -TotalCount 3 -Encoding Byte | ForEach-Object { [System.Convert]::ToString($_, 16).PadLeft(2, '0') }) -join ' '

# Se retornar "1f 8b 08" = Arquivo GZIP comprimido
# Se retornar texto legível = Arquivo SQL texto
```

## 🗜️ Para Arquivos COMPRIMIDOS (gzip)

### Método Recomendado - Docker com gunzip:
```powershell
# 1. Criar schema
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# 2. Importar descomprimindo na hora (RECOMENDADO)
docker exec postgres-caf-dumps sh -c "gunzip -c /dumps/dump-caf_mapa-20250301-202506151151.sql | psql -U caf_user -d caf_analysis -v ON_ERROR_STOP=1 --single-transaction"

# 3. OU descomprimir primeiro e depois importar
docker exec postgres-caf-dumps sh -c "gunzip -c /dumps/dump-caf_mapa-20250301-202506151151.sql > /tmp/dump_descomprimido.sql"
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f /tmp/dump_descomprimido.sql
```

## 📄 Para Arquivos SQL TEXTO

### Script PowerShell Melhorado:
```powershell
# Detecta automaticamente se é comprimido ou texto
.\import-dump-improved.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql"
```

### 2. Script Batch (Windows Simples)
```cmd
import-dump.bat "dumps\dump-caf_mapa-20250301-202506151151.sql" "caf_20250301"
```

### 3. Comandos Docker (Mais Controle)
```powershell
# 1. Criar schema
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# 2. Copiar dump para container
docker cp "dumps\dump-caf_mapa-20250301-202506151151.sql" postgres-caf-dumps:/tmp/

# 3. Importar dump
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f /tmp/dump-caf_mapa-20250301-202506151151.sql

# 4. Limpar arquivo temporário
docker exec postgres-caf-dumps rm /tmp/dump-caf_mapa-20250301-202506151151.sql
```

### 4. psql Local (Se PostgreSQL Instalado)
```powershell
# Definir senha
$env:PGPASSWORD="caf_password123"

# Criar schema
psql -h localhost -p 5433 -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# Importar
psql -h localhost -p 5433 -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f "dumps\dump-caf_mapa-20250301-202506151151.sql"
```

## 🎯 Recomendações por Tamanho

### Dumps Pequenos (< 100MB)
- **Use o Script PowerShell**: `.\import-dump.ps1`
- Mais rápido e com feedback visual

### Dumps Grandes (> 1GB)
- **Use comandos Docker diretos** para melhor controle
- Permite monitorar progresso no terminal
- Evita timeouts do PowerShell

### Dumps Binários (.dump)
```powershell
# Para dumps em formato binário
docker exec postgres-caf-dumps pg_restore -U caf_user -d caf_analysis -v --schema=caf_20250301 /dumps/arquivo.dump
```

## ⚡ Comandos de Verificação

### Verificar se container está rodando:
```powershell
docker ps --filter "name=postgres-caf-dumps"
```

### Verificar schemas existentes:
```powershell
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'caf_%';"
```

### Verificar tabelas em um schema:
```powershell
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SELECT tablename FROM pg_tables WHERE schemaname = 'caf_20250301' ORDER BY tablename;"
```

### Verificar tamanho dos schemas:
```powershell
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SELECT schema_name, ROUND(SUM(pg_total_relation_size(c.oid))/1024/1024) AS size_mb FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname LIKE 'caf_%' GROUP BY schema_name ORDER BY size_mb DESC;"
```

## 🚨 Troubleshooting

### Erro "arquivo não encontrado":
- Verifique o caminho do arquivo
- Use aspas duplas no caminho se houver espaços

### Erro "container não encontrado":
```powershell
# Iniciar ambiente
python manage-environment.py start
```

### Erro de memória/timeout:
- Para dumps muito grandes (>5GB), use comandos Docker diretos
- Considere importar em horários de menor uso

### Erro de encoding:
- Dumps com caracteres especiais podem precisar de `--encoding=UTF8`
- Use: `docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis --encoding=UTF8 -f /tmp/dump.sql`

## 💡 Dicas de Performance

1. **Para dumps grandes**: Execute durante a madrugada
2. **Para múltiplos dumps**: Importe um de cada vez
3. **Para análise**: Use sempre schemas separados por data
4. **Para produção**: Sempre faça backup antes de importar

## 📊 Exemplo Completo

```powershell
# 1. Verificar ambiente
python manage-environment.py status

# 2. Importar dump
.\import-dump.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql"

# 3. Verificar importação
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'caf_20250301';"

# 4. Acessar via PgAdmin: http://localhost:8082
```

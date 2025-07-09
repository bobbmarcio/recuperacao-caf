# 🚀 Comandos Rápidos - Ambiente Unificado

## Gerenciamento do Ambiente Completo

### Comandos Principais
```powershell
# Iniciar ambiente completo (MongoDB + PostgreSQL + Interfaces Web)
python manage-environment.py start

# Verificar status
python manage-environment.py status

# Parar ambiente
python manage-environment.py stop

# Reiniciar ambiente
python manage-environment.py restart

# Ver logs
python manage-environment.py logs

# Ver logs em tempo real
python manage-environment.py logs --follow

# Ver logs de um serviço específico
python manage-environment.py logs --service mongodb
python manage-environment.py logs --service postgres-caf

# Resetar ambiente (CUIDADO: apaga dados!)
python manage-environment.py reset

# Mostrar informações de conexão
python manage-environment.py info
```

## 🔗 Acessos Rápidos

### Interfaces Web
- **Mongo Express**: http://localhost:8080
- **PgAdmin**: http://localhost:8082
  - Email: `admin@caf.local`
  - Senha: `admin123`

### Conexões de Banco

#### PostgreSQL (Dumps CAF)
```
Host: localhost
Port: 5433
Database: caf_analysis
User: caf_user
Password: caf_password123
```

#### MongoDB (Auditoria)
```
Host: localhost
Port: 27017
Database: audit_db
Admin User: admin
Admin Password: admin123
```

## 📊 Comandos de Análise

### Importar Dumps CAF
```powershell
# Importar dump automaticamente (cria schema por data)
python import-dumps-caf.py

# Importar dump específico
python import-dumps-caf.py --dump-file "dumps/dump-caf_mapa-20250301-202506151151.sql"
```

### Executar Análise Incremental
```powershell
# Análise com configuração padrão
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/

# Análise com logs detalhados
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/ --verbose
```

## 📥 Importação Manual de Dumps

### Opção 1: Script PowerShell (Recomendado)
```powershell
# Importar dump específico
.\import-dump.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql" -SchemaName "caf_20250301"

# Importar deixando o script detectar a data do nome
.\import-dump.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql"
```

### Opção 2: Script Batch (Simples)
```cmd
# Importar dump com nome do schema específico
import-dump.bat "dumps\dump-caf_mapa-20250301-202506151151.sql" "caf_20250301"

# Importar com schema padrão
import-dump.bat "dumps\dump-caf_mapa-20250301-202506151151.sql"
```

### Opção 3: Comandos Docker Diretos
```powershell
# Criar schema
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# Copiar dump para container
docker cp "dumps\dump-caf_mapa-20250301-202506151151.sql" postgres-caf-dumps:/tmp/

# Importar
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f /tmp/dump-caf_mapa-20250301-202506151151.sql

# Limpar arquivo temporário
docker exec postgres-caf-dumps rm /tmp/dump-caf_mapa-20250301-202506151151.sql
```

### Opção 4: Via psql Local (se PostgreSQL instalado)
```powershell
# Definir senha
$env:PGPASSWORD="caf_password123"

# Criar schema
psql -h localhost -p 5433 -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"

# Importar dump
psql -h localhost -p 5433 -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301, public;" -f "dumps\dump-caf_mapa-20250301-202506151151.sql"
```

## 🚀 Scripts de Importação em Lote

### Opção 1: Script Python (Recomendado)
```powershell
# Importar todos os dumps CAF automaticamente
python import-all-caf-dumps.py

# Funcionalidades:
# ✅ Detecta automaticamente arquivos gzip vs SQL
# ✅ Extrai data do nome do arquivo
# ✅ Cria schemas separados por data
# ✅ Pula dumps já importados
# ✅ Registra metadados no banco
# ✅ Mostra progresso detalhado
```

### Opção 2: Script PowerShell
```powershell
# Importar todos os dumps com confirmação
.\import-all-dumps.ps1

# Importar sem confirmação (automático)
.\import-all-dumps.ps1 -Force

# Funcionalidades:
# ✅ Interface mais visual
# ✅ Confirma antes de iniciar
# ✅ Mostra estimativa de tempo
# ✅ Resumo final dos schemas
```

### Opção 3: Script Individual Melhorado
```powershell
# Para um dump específico
.\import-dump-improved.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql"

# Detecta automaticamente se é gzip ou SQL
```

## 🐳 Comandos Docker Diretos

```powershell
# Ver containers rodando
docker ps

# Ver logs de um container específico
docker logs recuperacao-caf-mongo
docker logs postgres-caf-dumps
docker logs recuperacao-caf-mongo-express
docker logs pgadmin-caf

# Acessar shell do PostgreSQL
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis

# Acessar shell do MongoDB
docker exec -it recuperacao-caf-mongo mongosh -u admin -p admin123
```

## 🔧 Troubleshooting

### Se containers não iniciarem:
```powershell
# Verificar se Docker está rodando
docker --version

# Limpar containers órfãos
docker system prune

# Recriar ambiente do zero
python manage-environment.py reset
```

### Se portas estiverem ocupadas:
- MongoDB: 27017
- Mongo Express: 8080  
- PostgreSQL: 5433
- PgAdmin: 8082

```powershell
# Ver processos usando portas
netstat -ano | findstr :27017
netstat -ano | findstr :5433
netstat -ano | findstr :8080
netstat -ano | findstr :8082
```

## 📁 Estrutura de Arquivos

```
recuperacao-caf/
├── docker-compose.yml          # Ambiente unificado
├── manage-environment.py       # Script de gerenciamento
├── import-dumps-caf.py        # Importação automática de dumps
├── config/
│   └── monitoring_config.yaml # Configuração de monitoramento
├── dumps/                     # Dumps PostgreSQL
├── src/                       # Código da aplicação
└── logs/                      # Logs da aplicação
```

## 🎯 Fluxo de Trabalho Recomendado

1. **Iniciar ambiente**: `python manage-environment.py start`
2. **Verificar status**: `python manage-environment.py status`
3. **Importar dumps**: `python import-dumps-caf.py`
4. **Executar análise**: `python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/`
5. **Verificar resultados** nas interfaces web ou logs
6. **Parar ambiente**: `python manage-environment.py stop` (quando terminar)

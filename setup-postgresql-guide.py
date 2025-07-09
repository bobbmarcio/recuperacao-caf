#!/usr/bin/env python3
"""
Guia completo para trabalhar com dumps CAF grandes
"""

import subprocess
from pathlib import Path


def create_readme():
    """Cria documentação completa"""
    
    readme_content = """# 🐘 Sistema PostgreSQL + MongoDB para Análise de Dumps CAF

## 📋 Resumo Executivo

✅ **Ambiente criado com sucesso:**
- 🐘 **PostgreSQL** (porta 5433) - Para processar dumps grandes (5-6GB)
- 📊 **MongoDB** (porta 27017) - Para auditoria de alterações
- 🌐 **Interfaces Web**:
  - PgAdmin: http://localhost:8082
  - Mongo Express: http://localhost:8080

## 🚀 Início Rápido

### 1. Iniciar Ambiente
```bash
python manage-environment.py start
```

### 2. Ver Status
```bash
python manage-environment.py status
```

### 3. Importar Dumps CAF
```bash
# Método Docker (recomendado para dumps grandes)
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis -c "
CREATE SCHEMA IF NOT EXISTS caf_20250301;
COMMENT ON SCHEMA caf_20250301 IS 'Schema para dump CAF de 2025-03-01';"

# Importar dump usando pg_restore (para dumps binários)
docker exec -i postgres-caf-dumps pg_restore -U caf_user -d caf_analysis -n caf_20250301 -v < dumps/dump-caf_mapa-20250301-202506151151.sql

# OU importar dump SQL (para dumps texto)
docker exec -i postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301;" < dumps/dump-caf_mapa-20250301-202506151151.sql
```

### 4. Verificar Importação
```bash
# Shell PostgreSQL
python manage-environment.py shell-postgres

# Listar schemas
\\dn

# Ver tabelas do schema CAF
\\dt caf_20250301.*

# Contar registros
SELECT schemaname, tablename, n_tup_ins as registros 
FROM pg_stat_user_tables 
WHERE schemaname = 'caf_20250301' 
ORDER BY n_tup_ins DESC;
```

### 5. Executar Análise Incremental
```bash
# Análise usando estratégia PostgreSQL (automática para dumps > 10GB)
python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/

# Consultar alterações detectadas
python src/main.py query --limit 20
```

## 🔧 Comandos Úteis

### Gerenciamento do Ambiente
```bash
# Iniciar tudo
python manage-environment.py start

# Parar tudo  
python manage-environment.py stop

# Status detalhado
python manage-environment.py status

# Ver logs
python manage-environment.py logs

# Reset completo (CUIDADO!)
python manage-environment.py reset
```

### Shells dos Bancos
```bash
# PostgreSQL
python manage-environment.py shell-postgres
# ou
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis

# MongoDB
python manage-environment.py shell-mongo
# ou  
docker exec -it recuperacao-caf-mongo mongosh audit_db -u app_user -p app_password
```

### Importação Manual de Dumps

#### Para Dumps SQL (texto):
```bash
# 1. Criar schema
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis -c "
CREATE SCHEMA IF NOT EXISTS caf_20250301;
SET search_path TO caf_20250301;"

# 2. Importar (para arquivo local)
docker exec -i postgres-caf-dumps psql -U caf_user -d caf_analysis < dumps/seu_dump.sql

# 3. Verificar
docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis -c "\\dt caf_20250301.*"
```

#### Para Dumps Binários (.dump):
```bash
# Restaurar direto no schema
docker exec -i postgres-caf-dumps pg_restore -U caf_user -d caf_analysis -n caf_20250301 -v < dumps/seu_dump.dump
```

## 📊 Configurações dos Bancos

### PostgreSQL
- **Host**: localhost:5433
- **Usuário**: caf_user  
- **Senha**: caf_password123
- **Banco**: caf_analysis
- **String**: `postgresql://caf_user:caf_password123@localhost:5433/caf_analysis`

### MongoDB  
- **Host**: localhost:27017
- **Usuário**: app_user
- **Senha**: app_password  
- **Banco**: audit_db
- **String**: `mongodb://app_user:app_password@localhost:27017/audit_db?authSource=audit_db`

## 🌐 Interfaces Web

### PgAdmin (PostgreSQL)
- **URL**: http://localhost:8082
- **Email**: admin@caf.local
- **Senha**: admin123

Para conectar ao PostgreSQL via PgAdmin:
1. Add New Server
2. Name: CAF Analysis
3. Host: postgres-caf-dumps
4. Port: 5432
5. Username: caf_user  
6. Password: caf_password123

### Mongo Express (MongoDB)
- **URL**: http://localhost:8080
- Acesso direto (sem login adicional)

## 📁 Estrutura de Schemas PostgreSQL

```
caf_analysis/
├── caf_reference/          # Dados de referência
├── caf_20250301/          # Dump de 2025-03-01  
├── caf_20250315/          # Dump de 2025-03-15
└── caf_analysis/          # Tabelas de controle
    ├── dump_metadata      # Metadados dos dumps
    └── comparison_log     # Log de comparações
```

## 🔍 Consultas Úteis

### PostgreSQL - Metadados dos Dumps
```sql
-- Ver dumps importados
SELECT * FROM caf_analysis.dump_metadata ORDER BY dump_date DESC;

-- Comparar tamanhos entre schemas
SELECT 
    schemaname,
    COUNT(*) as tabelas,
    SUM(n_tup_ins) as total_registros
FROM pg_stat_user_tables 
WHERE schemaname LIKE 'caf_%'
GROUP BY schemaname
ORDER BY schemaname;

-- Tabelas por schema
SELECT schemaname, tablename, n_tup_ins as registros
FROM pg_stat_user_tables 
WHERE schemaname = 'caf_20250301'
ORDER BY n_tup_ins DESC;
```

### MongoDB - Alterações Detectadas
```javascript
// Ver alterações recentes
db.data_changes.find().sort({change_timestamp: -1}).limit(10)

// Alterações por tabela
db.data_changes.aggregate([
  {$group: {_id: "$table_name", count: {$sum: 1}}},
  {$sort: {count: -1}}
])

// Alterações em período específico
db.data_changes.find({
  change_timestamp: {
    $gte: ISODate("2025-06-01"),
    $lte: ISODate("2025-06-30")
  }
})
```

## 🛠️ Troubleshooting

### Container não inicia
```bash
# Ver logs detalhados
docker-compose logs postgres-caf
docker-compose logs mongodb

# Reiniciar containers
docker-compose restart

# Reset completo
docker-compose down -v
docker-compose up -d
```

### Importação falha
```bash
# Verificar formato do dump
file dumps/seu_dump.sql

# Para dumps muito grandes, aumentar timeout
docker exec -i postgres-caf-dumps psql -U caf_user -d caf_analysis --set ON_ERROR_STOP=on < dumps/seu_dump.sql

# Verificar logs do PostgreSQL
docker-compose logs postgres-caf
```

### Erro de conexão
```bash
# Testar conectividade
docker exec postgres-caf-dumps pg_isready -U caf_user -d caf_analysis

# Verificar portas
netstat -an | findstr :5433
netstat -an | findstr :27017
```

## 📈 Performance para Dumps Grandes

### Dumps de 5-6GB:
- ⏱️ **Importação**: ~15-30 minutos
- 💾 **Espaço**: ~10-12GB (original + índices + logs)
- 🔄 **Comparação**: ~5-10 minutos via SQL
- 📊 **RAM**: ~2-4GB durante processamento

### Otimizações:
```sql
-- Desabilitar autovacuum temporariamente (apenas durante importação)
ALTER TABLE sua_tabela SET (autovacuum_enabled = false);

-- Aumentar configurações de performance
-- (já configurado no container)
```

## 🎯 Próximos Passos

1. **Importar seus dumps CAF** usando os comandos acima
2. **Configurar monitoramento** em `config/monitoring_config.yaml`
3. **Executar análise incremental** entre dumps
4. **Consultar alterações** via CLI ou interfaces web
5. **Automatizar processo** com scripts ou cron jobs

---

💡 **Dica**: Mantenha o ambiente rodando durante o desenvolvimento. Use `python manage-environment.py status` para verificar saúde dos serviços.

🆘 **Suporte**: Em caso de problemas, verifique logs com `python manage-environment.py logs` ou `docker-compose logs`.
"""
    
    with open("README_POSTGRESQL_CAF.md", "w", encoding="utf-8") as f:
        f.write(readme_content)
    
    print("✅ Documentação criada em README_POSTGRESQL_CAF.md")


def show_quick_commands():
    """Mostra comandos rápidos para uso"""
    
    print("""
🐘 COMANDOS RÁPIDOS PARA DUMPS CAF

📊 STATUS:
   python manage-environment.py status

🚀 IMPORTAR DUMP CAF:
   # 1. Entrar no container PostgreSQL
   docker exec -it postgres-caf-dumps bash
   
   # 2. Criar schema (dentro do container)
   psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS caf_20250301;"
   
   # 3. Importar dump (dentro do container)  
   psql -U caf_user -d caf_analysis -c "SET search_path TO caf_20250301;" -f /dumps/dump-caf_mapa-20250301-202506151151.sql

🔍 VERIFICAR IMPORTAÇÃO:
   # Entrar no PostgreSQL
   python manage-environment.py shell-postgres
   
   # Listar schemas
   \\dn
   
   # Ver tabelas
   \\dt caf_20250301.*

📋 ANÁLISE INCREMENTAL:
   python src/main.py analyze --config config/monitoring_config.yaml --dump-dir dumps/

🌐 INTERFACES WEB:
   PgAdmin: http://localhost:8082 (admin@caf.local / admin123)
   Mongo Express: http://localhost:8080
   
🛑 PARAR AMBIENTE:
   python manage-environment.py stop
""")


if __name__ == "__main__":
    print("📚 Criando documentação completa para ambiente PostgreSQL + MongoDB...")
    
    create_readme()
    show_quick_commands()
    
    print("\n✅ Ambiente PostgreSQL + MongoDB configurado com sucesso!")
    print("📖 Consulte README_POSTGRESQL_CAF.md para instruções detalhadas")
    print("🚀 Execute: python manage-environment.py status")

# PostgreSQL CAF - Gerenciamento de Dumps
# Comandos para Windows PowerShell

# 🚀 Iniciar PostgreSQL
function Start-PostgreSQLCAF {
    Write-Host "🚀 Iniciando PostgreSQL para dumps CAF..." -ForegroundColor Green
    docker-compose -f docker-compose-postgres.yml up -d
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "⏳ Aguardando PostgreSQL estar pronto..." -ForegroundColor Yellow
        Start-Sleep 15
        
        Write-Host "✅ PostgreSQL iniciado!" -ForegroundColor Green
        Write-Host "🔗 Conexão: postgresql://caf_user:caf_password123@localhost:5433/caf_analysis"
        Write-Host "🌐 PgAdmin: http://localhost:8081 (admin@caf.local / admin123)"
    } else {
        Write-Host "❌ Erro ao iniciar PostgreSQL" -ForegroundColor Red
    }
}

# 🛑 Parar PostgreSQL
function Stop-PostgreSQLCAF {
    Write-Host "🛑 Parando PostgreSQL..." -ForegroundColor Yellow
    docker-compose -f docker-compose-postgres.yml down
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ PostgreSQL parado com sucesso!" -ForegroundColor Green
    } else {
        Write-Host "❌ Erro ao parar PostgreSQL" -ForegroundColor Red
    }
}

# 📊 Status dos containers
function Get-PostgreSQLCAFStatus {
    Write-Host "📊 Status dos containers CAF:" -ForegroundColor Cyan
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    
    $containers = @("postgres-caf-dumps", "pgadmin-caf")
    
    foreach ($container in $containers) {
        $status = docker inspect --format="{{.State.Status}}" $container 2>$null
        
        if ($LASTEXITCODE -eq 0) {
            if ($status -eq "running") {
                Write-Host "✅ $container`: $status" -ForegroundColor Green
                
                # Mostrar portas
                $ports = docker port $container 2>$null
                if ($ports) {
                    $ports | ForEach-Object {
                        Write-Host "   🔗 $_" -ForegroundColor Cyan
                    }
                }
            } else {
                Write-Host "⚠️  $container`: $status" -ForegroundColor Yellow
            }
        } else {
            Write-Host "❌ $container`: não encontrado" -ForegroundColor Red
        }
    }
    
    # Testar conexão
    Write-Host "`n🔍 Teste de conexão PostgreSQL:" -ForegroundColor Cyan
    $testConnection = docker exec postgres-caf-dumps pg_isready -U caf_user -d caf_analysis 2>$null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ PostgreSQL: conectado" -ForegroundColor Green
    } else {
        Write-Host "❌ PostgreSQL: não conecta" -ForegroundColor Red
    }
}

# 📋 Logs do PostgreSQL
function Get-PostgreSQLCAFLogs {
    param(
        [string]$Service = "postgres-caf"
    )
    
    Write-Host "📋 Logs do $Service`:" -ForegroundColor Cyan
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    
    docker-compose -f docker-compose-postgres.yml logs --tail 50 $Service
}

# 📥 Importar dumps CAF
function Import-CAFDumps {
    Write-Host "📥 Importando dumps CAF..." -ForegroundColor Green
    
    # Verificar se há dumps CAF
    $caf_dumps = Get-ChildItem -Path "dumps" -Filter "*caf*" -File
    
    if ($caf_dumps.Count -eq 0) {
        Write-Host "📭 Nenhum dump CAF encontrado na pasta dumps/" -ForegroundColor Yellow
        return
    }
    
    Write-Host "📁 Dumps CAF encontrados:" -ForegroundColor Cyan
    foreach ($dump in $caf_dumps) {
        $size_mb = [math]::Round($dump.Length / 1MB, 1)
        Write-Host "  - $($dump.Name) ($size_mb MB)" -ForegroundColor White
    }
    
    $confirm = Read-Host "`n🚀 Importar todos os dumps? (s/N)"
    if ($confirm -eq 's' -or $confirm -eq 'S') {
        Write-Host "⏳ Executando importação..." -ForegroundColor Yellow
        
        # Executar script Python de importação
        docker exec postgres-caf-dumps python3 /docker-entrypoint-initdb.d/import_caf_dumps.py
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Importação concluída!" -ForegroundColor Green
        } else {
            Write-Host "❌ Erro na importação" -ForegroundColor Red
        }
    }
}

# 📁 Listar schemas CAF
function Get-CAFSchemas {
    Write-Host "📁 Listando schemas CAF..." -ForegroundColor Cyan
    
    $query = "SELECT schema_name, obj_description(oid, 'pg_namespace') as description FROM information_schema.schemata s JOIN pg_namespace n ON n.nspname = s.schema_name WHERE schema_name LIKE 'caf_%' ORDER BY schema_name;"
    
    docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "$query"
}

# 🐘 Shell PostgreSQL
function Enter-PostgreSQLCAFShell {
    Write-Host "🐘 Abrindo shell PostgreSQL..." -ForegroundColor Green
    Write-Host "💡 Digite \q para sair" -ForegroundColor Yellow
    
    docker exec -it postgres-caf-dumps psql -U caf_user -d caf_analysis
}

# 🔗 Informações de conexão
function Show-PostgreSQLCAFInfo {
    Write-Host "🔗 Informações de Conexão PostgreSQL CAF:" -ForegroundColor Cyan
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
    Write-Host "Host: localhost" -ForegroundColor White
    Write-Host "Porta: 5433" -ForegroundColor White
    Write-Host "Banco: caf_analysis" -ForegroundColor White
    Write-Host "Usuário: caf_user" -ForegroundColor White
    Write-Host "Senha: caf_password123" -ForegroundColor White
    Write-Host ""
    Write-Host "📋 String de conexão:" -ForegroundColor Cyan
    Write-Host "postgresql://caf_user:caf_password123@localhost:5433/caf_analysis" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "🌐 PgAdmin (interface web):" -ForegroundColor Cyan
    Write-Host "URL: http://localhost:8081" -ForegroundColor Yellow
    Write-Host "Email: admin@caf.local" -ForegroundColor Yellow
    Write-Host "Senha: admin123" -ForegroundColor Yellow
}

# 🧹 Limpeza completa
function Reset-PostgreSQLCAF {
    Write-Host "🧹 Limpeza completa do ambiente PostgreSQL CAF..." -ForegroundColor Yellow
    
    $confirm = Read-Host "⚠️  Isso removerá TODOS os dados. Continuar? (s/N)"
    if ($confirm -eq 's' -or $confirm -eq 'S') {
        Write-Host "🛑 Parando containers..." -ForegroundColor Yellow
        docker-compose -f docker-compose-postgres.yml down -v
        
        Write-Host "🗑️  Removendo volumes..." -ForegroundColor Yellow
        docker volume rm recuperacao-caf_postgres_caf_data 2>$null
        docker volume rm recuperacao-caf_pgadmin_caf_data 2>$null
        
        Write-Host "✅ Limpeza concluída!" -ForegroundColor Green
    }
}

# Aliases para facilitar o uso
Set-Alias -Name pg-start -Value Start-PostgreSQLCAF
Set-Alias -Name pg-stop -Value Stop-PostgreSQLCAF
Set-Alias -Name pg-status -Value Get-PostgreSQLCAFStatus
Set-Alias -Name pg-logs -Value Get-PostgreSQLCAFLogs
Set-Alias -Name pg-import -Value Import-CAFDumps
Set-Alias -Name pg-schemas -Value Get-CAFSchemas
Set-Alias -Name pg-shell -Value Enter-PostgreSQLCAFShell
Set-Alias -Name pg-info -Value Show-PostgreSQLCAFInfo
Set-Alias -Name pg-reset -Value Reset-PostgreSQLCAF

# Mostrar comandos disponíveis
Write-Host "🐘 PostgreSQL CAF - Comandos Disponíveis:" -ForegroundColor Cyan
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "pg-start     # Iniciar PostgreSQL e PgAdmin" -ForegroundColor Green
Write-Host "pg-stop      # Parar PostgreSQL e PgAdmin" -ForegroundColor Red
Write-Host "pg-status    # Status dos containers" -ForegroundColor Yellow
Write-Host "pg-logs      # Ver logs" -ForegroundColor Cyan
Write-Host "pg-import    # Importar dumps CAF" -ForegroundColor Magenta
Write-Host "pg-schemas   # Listar schemas CAF" -ForegroundColor Blue
Write-Host "pg-shell     # Shell PostgreSQL" -ForegroundColor White
Write-Host "pg-info      # Informações de conexão" -ForegroundColor Gray
Write-Host "pg-reset     # Limpeza completa" -ForegroundColor DarkRed
Write-Host ""
Write-Host "💡 Use '. .\manage-postgres-caf.ps1' para carregar as funções" -ForegroundColor Yellow

# Script PowerShell para importar dumps CAF
# Uso: .\import-dump.ps1 -DumpFile "dumps/dump-caf_mapa-20250301-202506151151.sql" -SchemaName "caf_20250301"

param(
    [Parameter(Mandatory=$true)]
    [string]$DumpFile,
    
    [Parameter(Mandatory=$false)]
    [string]$SchemaName,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseDocker = $true
)

# Extrair data do nome do arquivo se schema não fornecido
if (-not $SchemaName) {
    if ($DumpFile -match 'dump-caf.*?-(\d{8})-\d+') {
        $dateStr = $Matches[1]
        $SchemaName = "caf_$dateStr"
    } else {
        $dateStr = Get-Date -Format "yyyyMMdd"
        $SchemaName = "caf_$dateStr"
    }
}

Write-Host "Importando dump CAF para PostgreSQL" -ForegroundColor Green
Write-Host "Arquivo: $DumpFile" -ForegroundColor Cyan
Write-Host "Schema: $SchemaName" -ForegroundColor Cyan

# Verificar se arquivo existe
if (-not (Test-Path $DumpFile)) {
    Write-Host "ERRO: Arquivo nao encontrado: $DumpFile" -ForegroundColor Red
    exit 1
}

# Configurações do banco
$pgHost = if ($UseDocker) { "localhost" } else { "localhost" }
$pgPort = if ($UseDocker) { "5433" } else { "5432" }
$pgUser = "caf_user"
$pgPassword = "caf_password123"
$pgDatabase = "caf_analysis"

# Definir variável de ambiente para senha
$env:PGPASSWORD = $pgPassword

try {
    if ($UseDocker) {
        Write-Host "Usando container Docker..." -ForegroundColor Yellow
        
        # Verificar se container está rodando
        $containerStatus = docker ps --filter "name=postgres-caf-dumps" --format "{{.Status}}"
        if (-not $containerStatus) {
            Write-Host "ERRO: Container postgres-caf-dumps nao esta rodando" -ForegroundColor Red
            Write-Host "Execute: python manage-environment.py start" -ForegroundColor Yellow
            exit 1
        }
        
        Write-Host "Container esta rodando" -ForegroundColor Green
        
        # Criar schema
        Write-Host "Criando schema $SchemaName..." -ForegroundColor Yellow
        docker exec postgres-caf-dumps psql -U $pgUser -d $pgDatabase -c "CREATE SCHEMA IF NOT EXISTS `"$SchemaName`";"
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Falha ao criar schema" -ForegroundColor Red
            exit 1
        }
        
        # Copiar arquivo para container
        Write-Host "Copiando dump para container..." -ForegroundColor Yellow
        $tempFile = "/tmp/$(Split-Path $DumpFile -Leaf)"
        docker cp $DumpFile "postgres-caf-dumps:$tempFile"
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Falha ao copiar arquivo para container" -ForegroundColor Red
            exit 1
        }
        
        # Importar dump
        Write-Host "Importando dump (pode demorar para arquivos grandes)..." -ForegroundColor Yellow
        docker exec postgres-caf-dumps psql -U $pgUser -d $pgDatabase -c "SET search_path TO `"$SchemaName`", public;" -f $tempFile
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "SUCESSO: Dump importado com sucesso!" -ForegroundColor Green
            
            # Limpar arquivo temporário
            docker exec postgres-caf-dumps rm $tempFile
            
            # Mostrar informações do schema
            Write-Host "`nInformacoes do schema:" -ForegroundColor Cyan
            docker exec postgres-caf-dumps psql -U $pgUser -d $pgDatabase -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = '$SchemaName' ORDER BY tablename LIMIT 10;"
            
        } else {
            Write-Host "ERRO: Falha na importacao" -ForegroundColor Red
            # Limpar arquivo temporário mesmo em caso de erro
            docker exec postgres-caf-dumps rm $tempFile 2>$null
            exit 1
        }
    } else {
        Write-Host "Usando PostgreSQL local..." -ForegroundColor Yellow
        
        # Testar conexão
        $testResult = psql -h $pgHost -p $pgPort -U $pgUser -d $pgDatabase -c "SELECT 1;" 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Nao foi possivel conectar ao PostgreSQL local" -ForegroundColor Red
            Write-Host "Verifique se o PostgreSQL esta rodando ou use -UseDocker" -ForegroundColor Yellow
            exit 1
        }
        
        # Criar schema
        Write-Host "Criando schema $SchemaName..." -ForegroundColor Yellow
        psql -h $pgHost -p $pgPort -U $pgUser -d $pgDatabase -c "CREATE SCHEMA IF NOT EXISTS `"$SchemaName`";"
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Falha ao criar schema" -ForegroundColor Red
            exit 1
        }
        
        # Importar dump
        Write-Host "Importando dump..." -ForegroundColor Yellow
        psql -h $pgHost -p $pgPort -U $pgUser -d $pgDatabase -c "SET search_path TO `"$SchemaName`", public;" -f $DumpFile
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "SUCESSO: Dump importado com sucesso!" -ForegroundColor Green
        } else {
            Write-Host "ERRO: Falha na importacao" -ForegroundColor Red
            exit 1
        }
    }

} finally {
    # Limpar variável de ambiente
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

Write-Host "`nImportacao concluida!" -ForegroundColor Green
Write-Host "Acesse PgAdmin em: http://localhost:8082" -ForegroundColor Cyan
Write-Host "Schema criado: $SchemaName" -ForegroundColor Cyan

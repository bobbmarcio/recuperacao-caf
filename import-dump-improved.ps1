# Script PowerShell para importar dumps CAF (com suporte a gzip)
# Uso: .\import-dump-improved.ps1 -DumpFile "dumps\dump-caf_mapa-20250301-202506151151.sql" -SchemaName "caf_20250301"

param(
    [Parameter(Mandatory=$true)]
    [string]$DumpFile,
    
    [Parameter(Mandatory=$false)]
    [string]$SchemaName,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseDocker = $true
)

function Test-GzipFile {
    param([string]$FilePath)
    
    if (-not (Test-Path $FilePath)) {
        return $false
    }
    
    $bytes = Get-Content $FilePath -TotalCount 3 -Encoding Byte
    return ($bytes.Count -ge 3 -and $bytes[0] -eq 0x1f -and $bytes[1] -eq 0x8b -and $bytes[2] -eq 0x08)
}

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

# Verificar se é arquivo gzip
$isGzip = Test-GzipFile $DumpFile
if ($isGzip) {
    Write-Host "Detectado: Arquivo comprimido (gzip)" -ForegroundColor Yellow
} else {
    Write-Host "Detectado: Arquivo texto SQL" -ForegroundColor Yellow
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
        
        # Importar baseado no tipo de arquivo
        if ($isGzip) {
            Write-Host "Importando dump comprimido (pode demorar bastante)..." -ForegroundColor Yellow
            Write-Host "IMPORTANTE: Este processo pode levar 30-60 minutos para dumps grandes!" -ForegroundColor Magenta
            
            # Para arquivos gzip, usar gunzip + psql direto via pipe
            $importCmd = "gunzip -c /dumps/$(Split-Path $DumpFile -Leaf) | psql -U $pgUser -d $pgDatabase -v ON_ERROR_STOP=1 --single-transaction"
            docker exec postgres-caf-dumps sh -c "export PGPASSWORD='$pgPassword'; $importCmd"
            
        } else {
            Write-Host "Importando dump SQL..." -ForegroundColor Yellow
            
            # Copiar arquivo para container
            $tempFile = "/tmp/$(Split-Path $DumpFile -Leaf)"
            docker cp $DumpFile "postgres-caf-dumps:$tempFile"
            
            if ($LASTEXITCODE -ne 0) {
                Write-Host "ERRO: Falha ao copiar arquivo" -ForegroundColor Red
                exit 1
            }
            
            # Importar dump SQL normal
            docker exec postgres-caf-dumps psql -U $pgUser -d $pgDatabase -c "SET search_path TO `"$SchemaName`", public;" -f $tempFile
            
            # Limpar arquivo temporário
            docker exec postgres-caf-dumps rm $tempFile 2>$null
        }
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "SUCESSO: Dump importado com sucesso!" -ForegroundColor Green
            
            # Mostrar informações do schema
            Write-Host "`nInformacoes do schema:" -ForegroundColor Cyan
            docker exec postgres-caf-dumps psql -U $pgUser -d $pgDatabase -c "SELECT schemaname, tablename, n_tup_ins + n_tup_upd + n_tup_del as rows FROM pg_stat_user_tables WHERE schemaname = '$SchemaName' ORDER BY rows DESC LIMIT 10;"
            
        } else {
            Write-Host "ERRO: Falha na importacao" -ForegroundColor Red
            exit 1
        }
        
    } else {
        Write-Host "PostgreSQL local nao suporta gzip automatico" -ForegroundColor Red
        Write-Host "Use o metodo Docker ou descomprima o arquivo manualmente" -ForegroundColor Yellow
        exit 1
    }

} finally {
    # Limpar variável de ambiente
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

Write-Host "`nImportacao concluida!" -ForegroundColor Green
Write-Host "Acesse PgAdmin em: http://localhost:8082" -ForegroundColor Cyan
Write-Host "Schema criado: $SchemaName" -ForegroundColor Cyan

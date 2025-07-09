# Script PowerShell para importar todos os dumps CAF automaticamente
# Uso: .\import-all-dumps.ps1

param(
    [Parameter(Mandatory=$false)]
    [string]$DumpsDir = "dumps",
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipExisting = $true,
    
    [Parameter(Mandatory=$false)]
    [switch]$Force = $false
)

function Test-GzipFile {
    param([string]$FilePath)
    
    if (-not (Test-Path $FilePath)) {
        return $false
    }
    
    $bytes = Get-Content $FilePath -TotalCount 3 -Encoding Byte
    return ($bytes.Count -ge 3 -and $bytes[0] -eq 0x1f -and $bytes[1] -eq 0x8b -and $bytes[2] -eq 0x08)
}

function Get-DateFromFilename {
    param([string]$Filename)
    
    # Tentar vários padrões de data
    $patterns = @(
        'dump-caf.*?-(\d{8})-\d+',   # dump-caf_mapa-20250301-202506151151
        'caf.*?(\d{8})',              # caf20250301
        '(\d{4}-\d{2}-\d{2})',        # 2025-03-01
        '(\d{8})'                     # 20250301
    )
    
    foreach ($pattern in $patterns) {
        if ($Filename -match $pattern) {
            $dateStr = $Matches[1] -replace '-', ''
            if ($dateStr.Length -eq 8 -and $dateStr -match '^\d{8}$') {
                return $dateStr
            }
        }
    }
    
    return $null
}

function Get-SchemaName {
    param([string]$Filename)
    
    $dateStr = Get-DateFromFilename $Filename
    if ($dateStr) {
        return "caf_$dateStr"
    } else {
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        return "caf_$timestamp"
    }
}

function Test-SchemaExists {
    param([string]$SchemaName)
    
    try {
        $result = docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -t -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname = '$SchemaName';" 2>$null
        $count = [int]($result.Trim())
        return $count -gt 0
    } catch {
        return $false
    }
}

function Import-CAFDump {
    param(
        [string]$FilePath,
        [string]$SchemaName,
        [bool]$IsGzip
    )
    
    $fileName = Split-Path $FilePath -Leaf
    
    Write-Host "Criando schema $SchemaName..." -ForegroundColor Yellow
    $createResult = docker exec recuperacao-caf-postgres-1 psql -U postgres -d postgres -c "CREATE SCHEMA IF NOT EXISTS `"$SchemaName`";" 2>$null
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERRO: Falha ao criar schema $SchemaName" -ForegroundColor Red
        return $false
    }
    
    if ($IsGzip) {
        Write-Host "Importando dump comprimido (pode demorar bastante)..." -ForegroundColor Yellow
        
        $cmd = @"
export PGPASSWORD='postgres123'
gunzip -c /dumps/$fileName | \
grep -v 'SET transaction_timeout' | \
sed 's/public\./`"$SchemaName`"\./g; s/SCHEMA public/SCHEMA `"$SchemaName`"/g' | \
psql -U postgres -d postgres -v ON_ERROR_STOP=1 --single-transaction
"@
        
        $result = docker exec recuperacao-caf-postgres-1 sh -c $cmd
        
    } else {
        Write-Host "Importando dump SQL..." -ForegroundColor Yellow
        
        # Copiar para container
        $tempFile = "/tmp/$fileName"
        docker cp $FilePath "recuperacao-caf-postgres-1:$tempFile" | Out-Null
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "ERRO: Falha ao copiar arquivo" -ForegroundColor Red
            return $false
        }
        
        # Importar
        $cmd = @"
export PGPASSWORD='postgres123'
psql -U postgres -d postgres -c "SET search_path TO \`"$SchemaName\`", public;" -f $tempFile
"@
        
        $result = docker exec recuperacao-caf-postgres-1 sh -c $cmd
        
        # Limpar arquivo temporário
        docker exec recuperacao-caf-postgres-1 rm $tempFile 2>$null | Out-Null
    }
    
    return $LASTEXITCODE -eq 0
}

function Get-SchemaInfo {
    param([string]$SchemaName)
    
    try {
        $tableCount = docker exec recuperacao-caf-postgres-1 psql -U postgres -d postgres -t -c "SELECT COUNT(*) FROM pg_tables WHERE schemaname = '$SchemaName';" 2>$null
        $tableCount = [int]($tableCount.Trim())
        
        $sizeResult = docker exec recuperacao-caf-postgres-1 psql -U postgres -d postgres -t -c "SELECT COALESCE(ROUND(SUM(pg_total_relation_size(c.oid))/1024/1024), 0) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '$SchemaName';" 2>$null
        $sizeMB = [int]($sizeResult.Trim())
        
        return @{
            TableCount = $tableCount
            SizeMB = $sizeMB
        }
    } catch {
        return @{
            TableCount = 0
            SizeMB = 0
        }
    }
}

# Início do script principal
Write-Host "Importador em Lote de Dumps CAF" -ForegroundColor Green
Write-Host "=" * 50

# Verificar se container está rodando
$containerStatus = docker ps --filter "name=recuperacao-caf-postgres-1" --format "{{.Status}}" 2>$null
if (-not $containerStatus) {
    Write-Host "ERRO: Container recuperacao-caf-postgres-1 nao esta rodando" -ForegroundColor Red
    Write-Host "Execute: python manage-environment.py start" -ForegroundColor Yellow
    exit 1
}

Write-Host "Container PostgreSQL esta rodando" -ForegroundColor Green

# Encontrar dumps CAF
$dumpFiles = @()
$patterns = @("*caf*.sql", "*caf*.dump", "*CAF*.sql", "*CAF*.dump")

foreach ($pattern in $patterns) {
    $files = Get-ChildItem -Path $DumpsDir -Filter $pattern -File | Where-Object { -not $_.Name.StartsWith("temp_") }
    $dumpFiles += $files
}

# Remover duplicatas
$dumpFiles = $dumpFiles | Sort-Object Name -Unique

if ($dumpFiles.Count -eq 0) {
    Write-Host "Nenhum dump CAF encontrado na pasta $DumpsDir" -ForegroundColor Yellow
    exit 0
}

Write-Host "`nEncontrados $($dumpFiles.Count) dumps CAF:" -ForegroundColor Cyan

$totalSizeMB = 0
$dumpInfo = @()

foreach ($dump in $dumpFiles) {
    $sizeMB = [math]::Round($dump.Length / 1MB, 1)
    $isGzip = Test-GzipFile $dump.FullName
    $schemaName = Get-SchemaName $dump.Name
    $alreadyExists = Test-SchemaExists $schemaName
    
    $totalSizeMB += $sizeMB
    
    $dumpInfo += [PSCustomObject]@{
        File = $dump
        SizeMB = $sizeMB
        IsGzip = $isGzip
        SchemaName = $schemaName
        AlreadyExists = $alreadyExists
    }
    
    $status = if ($alreadyExists) { "IMPORTADO" } elseif ($isGzip) { "GZIP" } else { "SQL" }
    $statusColor = if ($alreadyExists) { "Green" } elseif ($isGzip) { "Yellow" } else { "Cyan" }
    
    Write-Host "  " -NoNewline
    Write-Host $status -ForegroundColor $statusColor -NoNewline
    Write-Host " $($dump.Name) -> $schemaName ($($sizeMB) MB)"
}

Write-Host "`nTamanho total: $($totalSizeMB) MB" -ForegroundColor Cyan

# Filtrar dumps pendentes
$pendingDumps = $dumpInfo | Where-Object { -not $_.AlreadyExists }

if ($pendingDumps.Count -eq 0) {
    Write-Host "Todos os dumps ja foram importados!" -ForegroundColor Green
    exit 0
}

if (-not $Force) {
    Write-Host "`n$($pendingDumps.Count) dumps pendentes para importacao" -ForegroundColor Yellow
    $estimatedMinutes = [math]::Round(($pendingDumps | Measure-Object SizeMB -Sum).Sum / 100, 1)
    Write-Host "Tempo estimado: $estimatedMinutes minutos" -ForegroundColor Yellow
    
    $response = Read-Host "`nContinuar com a importacao? (s/N)"
    if ($response -notmatch '^[sS].*') {
        Write-Host "Importacao cancelada pelo usuario" -ForegroundColor Yellow
        exit 0
    }
}

Write-Host "`n" + ("=" * 50)

# Processar dumps pendentes
$successful = 0
$failed = 0

for ($i = 0; $i -lt $pendingDumps.Count; $i++) {
    $dump = $pendingDumps[$i]
    $progress = $i + 1
    
    Write-Host "`n[$progress/$($pendingDumps.Count)] Processando: $($dump.File.Name)" -ForegroundColor White
    Write-Host "  Schema: $($dump.SchemaName)" -ForegroundColor Cyan
    Write-Host "  Tipo: $(if ($dump.IsGzip) { 'GZIP Comprimido' } else { 'SQL Texto' })" -ForegroundColor Cyan
    Write-Host "  Tamanho: $($dump.SizeMB) MB" -ForegroundColor Cyan
    
    $startTime = Get-Date
    
    $success = Import-CAFDump -FilePath $dump.File.FullName -SchemaName $dump.SchemaName -IsGzip $dump.IsGzip
    
    $elapsedSeconds = [math]::Round((Get-Date - $startTime).TotalSeconds, 1)
    
    if ($success) {
        $successful++
        Write-Host "  SUCESSO: Importado em $($elapsedSeconds)s" -ForegroundColor Green
        
        $info = Get-SchemaInfo $dump.SchemaName
        if ($info.TableCount -gt 0) {
            Write-Host "  Info: $($info.TableCount) tabelas, $($info.SizeMB) MB" -ForegroundColor Green
        }
    } else {
        $failed++
        Write-Host "  ERRO: Falha na importacao" -ForegroundColor Red
    }
}

# Resumo final
Write-Host "`n" + ("=" * 50)
Write-Host "Importacao em lote concluida!" -ForegroundColor Green
Write-Host "Sucessos: $successful" -ForegroundColor Green
Write-Host "Falhas: $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })

if ($successful -gt 0) {
    Write-Host "`nAcesse PgAdmin: http://localhost:8082" -ForegroundColor Cyan
    
    # Mostrar resumo dos schemas
    Write-Host "`nSchemas CAF importados:" -ForegroundColor Cyan
    $schemas = docker exec recuperacao-caf-postgres-1 psql -U postgres -d postgres -t -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'caf_%' ORDER BY schema_name;" 2>$null
    
    if ($schemas) {
        $schemas.Split("`n") | Where-Object { $_.Trim() } | ForEach-Object {
            $schema = $_.Trim()
            $info = Get-SchemaInfo $schema
            Write-Host "  $schema - $($info.TableCount) tabelas, $($info.SizeMB) MB" -ForegroundColor Gray
        }
    }
}

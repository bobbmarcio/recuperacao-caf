# Script PowerShell para gerenciar MongoDB via Docker Compose
# Projeto Recuperação CAF

param(
    [Parameter(Position=0)]
    [ValidateSet("up", "down", "restart", "logs", "status", "clean", "shell", "backup", "restore", "help")]
    [string]$Command = "help"
)

function Write-ColorText {
    param(
        [string]$Text,
        [string]$Color = "White"
    )
    Write-Host $Text -ForegroundColor $Color
}

function Show-Usage {
    Write-ColorText "🐳 Gerenciador MongoDB - Projeto Recuperação CAF" "Cyan"
    Write-Host ""
    Write-Host "Uso: .\manage-mongo.ps1 [comando]"
    Write-Host ""
    Write-Host "Comandos disponíveis:"
    Write-Host "  up        - Iniciar MongoDB e Mongo Express"
    Write-Host "  down      - Parar e remover containers"
    Write-Host "  restart   - Reiniciar serviços"
    Write-Host "  logs      - Visualizar logs do MongoDB"
    Write-Host "  status    - Verificar status dos containers"
    Write-Host "  clean     - Limpar dados persistentes (CUIDADO!)"
    Write-Host "  shell     - Abrir shell no MongoDB"
    Write-Host "  backup    - Fazer backup do banco"
    Write-Host "  restore   - Restaurar backup"
    Write-Host "  help      - Mostrar esta ajuda"
    Write-Host ""
    Write-Host "URLs após inicialização:"
    Write-Host "  MongoDB: mongodb://localhost:27017"
    Write-Host "  Mongo Express: http://localhost:8081"
}

function Test-DockerAvailable {
    try {
        $null = docker --version
        return $true
    }
    catch {
        Write-ColorText "❌ Docker não encontrado. Instale o Docker Desktop primeiro." "Red"
        exit 1
    }
}

function Get-DockerComposeCommand {
    try {
        $null = docker compose version
        return "docker compose"
    }
    catch {
        try {
            $null = docker-compose --version
            return "docker-compose"
        }
        catch {
            Write-ColorText "❌ Docker Compose não encontrado." "Red"
            exit 1
        }
    }
}

function Start-Services {
    Write-ColorText "🚀 Iniciando MongoDB e Mongo Express..." "Green"
    
    Test-DockerAvailable
    $dockerCompose = Get-DockerComposeCommand
    
    & $dockerCompose.Split() up -d
    
    if ($LASTEXITCODE -eq 0) {
        Write-ColorText "✅ Serviços iniciados!" "Green"
        Write-Host ""
        Write-ColorText "📊 Aguardando MongoDB inicializar..." "Yellow"
        Start-Sleep -Seconds 10
        
        Write-ColorText "🔗 URLs disponíveis:" "Cyan"
        Write-Host "  MongoDB: mongodb://localhost:27017"
        Write-Host "  Mongo Express: http://localhost:8081"
        Write-Host ""
        Write-ColorText "👤 Credenciais do MongoDB:" "Yellow"
        Write-Host "  Admin: admin / admin123"
        Write-Host "  App User: app_user / app_password"
        Write-Host ""
        Write-ColorText "🎯 Para testar a aplicação:" "Cyan"
        Write-Host "  python src/main.py analyze --config config/monitoring_config.yaml"
    }
    else {
        Write-ColorText "❌ Erro ao iniciar serviços." "Red"
    }
}

function Stop-Services {
    Write-ColorText "🛑 Parando serviços..." "Yellow"
    
    $dockerCompose = Get-DockerComposeCommand
    & $dockerCompose.Split() down
    
    if ($LASTEXITCODE -eq 0) {
        Write-ColorText "✅ Serviços parados!" "Green"
    }
}

function Restart-Services {
    Write-ColorText "🔄 Reiniciando serviços..." "Yellow"
    Stop-Services
    Start-Sleep -Seconds 2
    Start-Services
}

function Show-Logs {
    Write-ColorText "📋 Logs do MongoDB:" "Cyan"
    
    $dockerCompose = Get-DockerComposeCommand
    & $dockerCompose.Split() logs -f mongodb
}

function Show-Status {
    Write-ColorText "📊 Status dos containers:" "Cyan"
    
    $dockerCompose = Get-DockerComposeCommand
    & $dockerCompose.Split() ps
}

function Clean-Data {
    Write-ColorText "⚠️  ATENÇÃO: Isso irá remover TODOS os dados do MongoDB!" "Red"
    $confirmation = Read-Host "Tem certeza? Digite 'CONFIRMAR' para continuar"
    
    if ($confirmation -eq "CONFIRMAR") {
        Write-ColorText "🧹 Limpando dados..." "Yellow"
        Stop-Services
        docker volume rm recuperacao-caf_mongodb_data 2>$null
        Write-ColorText "✅ Dados limpos!" "Green"
    }
    else {
        Write-ColorText "❌ Operação cancelada." "Cyan"
    }
}

function Open-Shell {
    Write-ColorText "🐚 Abrindo shell do MongoDB..." "Cyan"
    Write-Host "Use 'exit' para sair do shell"
    Write-Host ""
    
    docker exec -it recuperacao-caf-mongo mongosh mongodb://admin:admin123@localhost:27017/audit_db
}

function Backup-Database {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupFile = "backup_audit_db_$timestamp.archive"
    
    Write-ColorText "💾 Fazendo backup do banco audit_db..." "Cyan"
    
    # Criar diretório de backups se não existir
    if (!(Test-Path "./backups")) {
        New-Item -ItemType Directory -Path "./backups"
    }
    
    docker exec recuperacao-caf-mongo mongodump --uri="mongodb://admin:admin123@localhost:27017/audit_db" --archive="/tmp/$backupFile" --gzip
    docker cp "recuperacao-caf-mongo:/tmp/$backupFile" "./backups/$backupFile"
    
    Write-ColorText "✅ Backup criado: ./backups/$backupFile" "Green"
}

function Restore-Database {
    Write-ColorText "📥 Arquivos de backup disponíveis:" "Cyan"
    
    if (Test-Path "./backups") {
        Get-ChildItem "./backups/*.archive" | Format-Table Name, Length, LastWriteTime
    }
    else {
        Write-ColorText "❌ Nenhum backup encontrado na pasta ./backups" "Red"
        return
    }
    
    $backupFile = Read-Host "Digite o nome do arquivo de backup"
    
    if (Test-Path "./backups/$backupFile") {
        Write-ColorText "📥 Restaurando backup: $backupFile" "Cyan"
        
        docker cp "./backups/$backupFile" "recuperacao-caf-mongo:/tmp/$backupFile"
        docker exec recuperacao-caf-mongo mongorestore --uri="mongodb://admin:admin123@localhost:27017/audit_db" --archive="/tmp/$backupFile" --gzip --drop
        
        Write-ColorText "✅ Backup restaurado com sucesso!" "Green"
    }
    else {
        Write-ColorText "❌ Arquivo de backup não encontrado." "Red"
    }
}

# Criar diretório de backups se não existir
if (!(Test-Path "./backups")) {
    New-Item -ItemType Directory -Path "./backups" -Force | Out-Null
}

# Processar comando
switch ($Command) {
    "up" { Start-Services }
    "down" { Stop-Services }
    "restart" { Restart-Services }
    "logs" { Show-Logs }
    "status" { Show-Status }
    "clean" { Clean-Data }
    "shell" { Open-Shell }
    "backup" { Backup-Database }
    "restore" { Restore-Database }
    default { Show-Usage }
}

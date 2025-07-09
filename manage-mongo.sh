#!/bin/bash
# Script para gerenciar o MongoDB via Docker Compose

set -e

# Cores para output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

function print_usage() {
    echo -e "${BLUE}🐳 Gerenciador MongoDB - Projeto Recuperação CAF${NC}"
    echo ""
    echo "Uso: $0 [comando]"
    echo ""
    echo "Comandos disponíveis:"
    echo "  up        - Iniciar MongoDB e Mongo Express"
    echo "  down      - Parar e remover containers"
    echo "  restart   - Reiniciar serviços"
    echo "  logs      - Visualizar logs do MongoDB"
    echo "  status    - Verificar status dos containers"
    echo "  clean     - Limpar dados persistentes (CUIDADO!)"
    echo "  shell     - Abrir shell no MongoDB"
    echo "  backup    - Fazer backup do banco"
    echo "  restore   - Restaurar backup"
    echo "  help      - Mostrar esta ajuda"
    echo ""
    echo "URLs após inicialização:"
    echo "  MongoDB: mongodb://localhost:27017"
    echo "  Mongo Express: http://localhost:8081"
}

function start_services() {
    echo -e "${GREEN}🚀 Iniciando MongoDB e Mongo Express...${NC}"
    
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker não encontrado. Instale o Docker primeiro.${NC}"
        exit 1
    fi
    
    # Usar docker compose ou docker-compose
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    $DOCKER_COMPOSE up -d
    
    echo -e "${GREEN}✅ Serviços iniciados!${NC}"
    echo ""
    echo -e "${YELLOW}📊 Aguardando MongoDB inicializar...${NC}"
    sleep 10
    
    echo -e "${BLUE}🔗 URLs disponíveis:${NC}"
    echo "  MongoDB: mongodb://localhost:27017"
    echo "  Mongo Express: http://localhost:8081"
    echo ""
    echo -e "${YELLOW}👤 Credenciais do MongoDB:${NC}"
    echo "  Admin: admin / admin123"
    echo "  App User: app_user / app_password"
}

function stop_services() {
    echo -e "${YELLOW}🛑 Parando serviços...${NC}"
    
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    $DOCKER_COMPOSE down
    echo -e "${GREEN}✅ Serviços parados!${NC}"
}

function restart_services() {
    echo -e "${YELLOW}🔄 Reiniciando serviços...${NC}"
    stop_services
    sleep 2
    start_services
}

function show_logs() {
    echo -e "${BLUE}📋 Logs do MongoDB:${NC}"
    
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    $DOCKER_COMPOSE logs -f mongodb
}

function show_status() {
    echo -e "${BLUE}📊 Status dos containers:${NC}"
    
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
    
    $DOCKER_COMPOSE ps
}

function clean_data() {
    echo -e "${RED}⚠️  ATENÇÃO: Isso irá remover TODOS os dados do MongoDB!${NC}"
    read -p "Tem certeza? Digite 'CONFIRMAR' para continuar: " confirmation
    
    if [ "$confirmation" = "CONFIRMAR" ]; then
        echo -e "${YELLOW}🧹 Limpando dados...${NC}"
        stop_services
        docker volume rm recuperacao-caf_mongodb_data 2>/dev/null || true
        echo -e "${GREEN}✅ Dados limpos!${NC}"
    else
        echo -e "${BLUE}❌ Operação cancelada.${NC}"
    fi
}

function open_shell() {
    echo -e "${BLUE}🐚 Abrindo shell do MongoDB...${NC}"
    echo "Use 'exit' para sair do shell"
    echo ""
    
    docker exec -it recuperacao-caf-mongo mongosh mongodb://admin:admin123@localhost:27017/audit_db
}

function backup_db() {
    timestamp=$(date +"%Y%m%d_%H%M%S")
    backup_file="backup_audit_db_${timestamp}.archive"
    
    echo -e "${BLUE}💾 Fazendo backup do banco audit_db...${NC}"
    
    docker exec recuperacao-caf-mongo mongodump \
        --uri="mongodb://admin:admin123@localhost:27017/audit_db" \
        --archive=/tmp/$backup_file \
        --gzip
    
    docker cp recuperacao-caf-mongo:/tmp/$backup_file ./backups/$backup_file
    
    echo -e "${GREEN}✅ Backup criado: ./backups/$backup_file${NC}"
}

function restore_db() {
    echo -e "${BLUE}📥 Arquivos de backup disponíveis:${NC}"
    ls -la ./backups/*.archive 2>/dev/null || {
        echo -e "${RED}❌ Nenhum backup encontrado na pasta ./backups/${NC}"
        exit 1
    }
    
    echo ""
    read -p "Digite o nome do arquivo de backup: " backup_file
    
    if [ -f "./backups/$backup_file" ]; then
        echo -e "${BLUE}📥 Restaurando backup: $backup_file${NC}"
        
        docker cp ./backups/$backup_file recuperacao-caf-mongo:/tmp/$backup_file
        
        docker exec recuperacao-caf-mongo mongorestore \
            --uri="mongodb://admin:admin123@localhost:27017/audit_db" \
            --archive=/tmp/$backup_file \
            --gzip \
            --drop
        
        echo -e "${GREEN}✅ Backup restaurado com sucesso!${NC}"
    else
        echo -e "${RED}❌ Arquivo de backup não encontrado.${NC}"
    fi
}

# Criar diretório de backups se não existir
mkdir -p ./backups

# Processar comando
case "${1:-help}" in
    "up"|"start")
        start_services
        ;;
    "down"|"stop")
        stop_services
        ;;
    "restart")
        restart_services
        ;;
    "logs")
        show_logs
        ;;
    "status")
        show_status
        ;;
    "clean")
        clean_data
        ;;
    "shell")
        open_shell
        ;;
    "backup")
        backup_db
        ;;
    "restore")
        restore_db
        ;;
    "help"|*)
        print_usage
        ;;
esac

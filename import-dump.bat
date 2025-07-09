@echo off
REM Script simples para importar dump CAF
REM Uso: import-dump.bat "dumps\dump-caf_mapa-20250301-202506151151.sql" "caf_20250301"

setlocal EnableDelayedExpansion

set DUMP_FILE=%1
set SCHEMA_NAME=%2

if "%DUMP_FILE%"=="" (
    echo ❌ Uso: import-dump.bat "caminho\para\dump.sql" "nome_schema"
    echo 💡 Exemplo: import-dump.bat "dumps\dump-caf_mapa-20250301-202506151151.sql" "caf_20250301"
    exit /b 1
)

if "%SCHEMA_NAME%"=="" (
    echo ⚠️  Schema não especificado, usando padrão...
    set SCHEMA_NAME=caf_import
)

echo 🐘 Importando dump CAF para PostgreSQL
echo 📁 Arquivo: %DUMP_FILE%
echo 📊 Schema: %SCHEMA_NAME%

REM Verificar se arquivo existe
if not exist "%DUMP_FILE%" (
    echo ❌ Arquivo não encontrado: %DUMP_FILE%
    exit /b 1
)

REM Verificar se container está rodando
docker ps --filter "name=postgres-caf-dumps" --format "{{.Status}}" > temp_status.txt 2>nul
set /p CONTAINER_STATUS=<temp_status.txt
del temp_status.txt 2>nul

if "%CONTAINER_STATUS%"=="" (
    echo ❌ Container postgres-caf-dumps não está rodando
    echo 💡 Execute: python manage-environment.py start
    exit /b 1
)

echo ✅ Container está rodando

REM Criar schema
echo 📋 Criando schema %SCHEMA_NAME%...
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "CREATE SCHEMA IF NOT EXISTS \"%SCHEMA_NAME%\";"

if !errorlevel! neq 0 (
    echo ❌ Erro ao criar schema
    exit /b 1
)

REM Copiar arquivo para container
echo 📤 Copiando dump para container...
for %%F in ("%DUMP_FILE%") do set FILENAME=%%~nxF
docker cp "%DUMP_FILE%" postgres-caf-dumps:/tmp/%FILENAME%

if !errorlevel! neq 0 (
    echo ❌ Erro ao copiar arquivo para container
    exit /b 1
)

REM Importar dump
echo 📥 Importando dump (pode demorar para arquivos grandes)...
docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SET search_path TO \"%SCHEMA_NAME%\", public;" -f /tmp/%FILENAME%

if !errorlevel! equ 0 (
    echo ✅ Dump importado com sucesso!
    
    REM Limpar arquivo temporário
    docker exec postgres-caf-dumps rm /tmp/%FILENAME% 2>nul
    
    echo.
    echo 📊 Verificando tabelas criadas...
    docker exec postgres-caf-dumps psql -U caf_user -d caf_analysis -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = '%SCHEMA_NAME%' ORDER BY tablename LIMIT 10;"
    
) else (
    echo ❌ Erro na importação
    REM Limpar arquivo temporário mesmo em caso de erro
    docker exec postgres-caf-dumps rm /tmp/%FILENAME% 2>nul
    exit /b 1
)

echo.
echo 🎉 Importação concluída!
echo 🔗 Acesse PgAdmin em: http://localhost:8082
echo 📊 Schema criado: %SCHEMA_NAME%

endlocal

@echo off
chcp 65001 >nul
title Neo Energia - Orquestrador Macro
cd /d "%~dp0"

echo =============================================
echo  NEO ENERGIA - ORQUESTRADOR AUTOMATICO
echo =============================================
echo  Ciclo: banco de dados -> SSH+tunel -> API
echo =============================================
echo.

:: Verifica se o venv existe
if not exist ".venv\Scripts\python.exe" (
    echo [ERRO] Ambiente virtual nao encontrado.
    echo        Execute primeiro: setup_venv.bat
    echo.
    pause
    exit /b 1
)

:: Verifica se o .env existe
if not exist ".env" (
    echo [ERRO] Arquivo .env nao encontrado.
    echo        Copie .env.example para .env e preencha as credenciais.
    echo.
    pause
    exit /b 1
)

echo Venv: OK
echo .env: OK
echo.

:: Repassa todos os argumentos para o python (ex: --tamanho 500 --dry-run)
:: Uso:
::   EXECUTAR.bat                  -> lote padrao (2000 registros)
::   EXECUTAR.bat --tamanho 500    -> lote de 500
::   EXECUTAR.bat --dry-run        -> so busca, nao grava
echo Iniciando ciclo... (%*)
echo.

.venv\Scripts\python.exe -u executar_automatico.py %*

echo.
echo =============================================
if %errorlevel% equ 0 (
    echo  CICLO CONCLUIDO COM SUCESSO
) else (
    echo  CICLO ENCERROU COM ERRO ^(codigo %errorlevel%^)
    echo  Verifique a saida acima para detalhes.
)
echo =============================================
echo.
pause
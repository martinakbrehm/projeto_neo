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

:: Uso:
::   EXECUTAR.bat                        -> loop continuo, lotes de 200 (modo padrao)
::   EXECUTAR.bat --tamanho 500          -> loop continuo, lotes de 500
::   EXECUTAR.bat --tamanho 200 --pausa 60  -> loop com pausa de 60s entre ciclos
::   EXECUTAR.bat --dry-run              -> so busca, nao grava
::   EXECUTAR.bat --tamanho 2000 (sem --continuar) -> ciclo unico de 2000
echo Iniciando modo continuo... (%*)
echo Ctrl+C para parar com segurança.
echo.

.venv\Scripts\python.exe -u executar_automatico.py --continuar %*

echo.
echo =============================================
if %errorlevel% equ 0 (
    echo  ORQUESTRADOR ENCERRADO
) else (
    echo  ENCERROU COM ERRO ^(codigo %errorlevel%^)
    echo  Verifique a saida acima para detalhes.
)
echo =============================================
echo.
pause
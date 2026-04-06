@echo off
chcp 65001 >nul
title Neo Energia - Launcher Externo
echo =============================================
echo 🚀 NEO ENERGIA - LAUNCHER EXTERNO
echo =============================================
echo.

REM ========================================
REM CONFIGURE AQUI O CAMINHO DO SEU PROJETO
REM ========================================
set PROJETO_NEO=C:\Users\gismi\OneDrive\Desktop\auto_api_neo\consulta_neo_reinan\consulta_neo_reinan

echo 📁 BAT executado de: %~dp0
echo 📁 Projeto NEO localizado em: %PROJETO_NEO%
echo.

REM Verifica se o diretório do projeto existe
if not exist "%PROJETO_NEO%" (
    echo ❌ ERRO: Diretório do projeto não encontrado!
    echo    Caminho: %PROJETO_NEO%
    echo.
    echo 💡 Edite este BAT e configure a variável PROJETO_NEO com o caminho correto
    echo.
    pause
    exit /b 1
)

REM Verifica se o ambiente virtual existe
if not exist "%PROJETO_NEO%\.venv\Scripts\python.exe" (
    echo ❌ ERRO: Ambiente virtual não encontrado!
    echo    Esperado em: %PROJETO_NEO%\.venv\Scripts\python.exe
    echo.
    pause
    exit /b 1
)

REM Verifica se o script principal existe
if not exist "%PROJETO_NEO%\executar_automatico.py" (
    echo ❌ ERRO: Script principal não encontrado!
    echo    Esperado em: %PROJETO_NEO%\executar_automatico.py
    echo.
    pause
    exit /b 1
)

echo ✅ Projeto encontrado
echo ✅ Ambiente virtual encontrado  
echo ✅ Script principal encontrado
echo.

REM Muda para o diretório do projeto para execução
cd /d "%PROJETO_NEO%"

echo 🚀 Executando automação Neo Energia...
echo ⏱️ Aguarde o processamento...
echo.

REM Executa o script usando o Python do ambiente virtual
"%PROJETO_NEO%\.venv\Scripts\python.exe" "%PROJETO_NEO%\executar_automatico.py"

set RESULTADO=%ERRORLEVEL%

echo.
echo =============================================
if %RESULTADO% EQU 0 (
    echo ✅ Automação finalizada com SUCESSO!
) else (
    echo ⚠️ Automação finalizada com AVISOS (código: %RESULTADO%)
)
echo =============================================
echo.
echo Pressione qualquer tecla para sair...
pause >nul
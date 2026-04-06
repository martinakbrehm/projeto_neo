@echo off
chcp 65001 >nul
title Neo Energia - Setup do Ambiente

echo =============================================
echo   NEO ENERGIA - CONFIGURACAO DO AMBIENTE
echo =============================================
echo.

cd /d "%~dp0"

:: Verifica se Python 3.12+ está disponível
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python nao encontrado. Instale Python 3.12 ou superior.
    echo        Download: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/4] Verificando Python...
python --version

:: Remove venv antigo se existir
if exist ".venv" (
    echo [2/4] Removendo ambiente virtual antigo...
    rmdir /s /q .venv
)

:: Cria novo venv
echo [2/4] Criando ambiente virtual...
python -m venv .venv
if %errorlevel% neq 0 (
    echo [ERRO] Falha ao criar ambiente virtual.
    pause
    exit /b 1
)

:: Atualiza pip
echo [3/4] Atualizando pip...
.venv\Scripts\python.exe -m pip install --upgrade pip --quiet

:: Instala dependências
echo [4/4] Instalando dependencias...
.venv\Scripts\pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ERRO] Falha ao instalar dependencias.
    pause
    exit /b 1
)

echo.
echo =============================================
echo   AMBIENTE CONFIGURADO COM SUCESSO!
echo =============================================
echo.
echo Proximo passo: configure o arquivo .env
echo   Copie .env.example para .env e preencha:
echo   SSH_USER, SSH_SERVER, SSH_PASSWORD, REMOTE_HOST
echo.
echo Para executar: EXECUTAR.bat
echo.
pause

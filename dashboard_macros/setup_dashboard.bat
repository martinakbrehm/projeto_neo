@echo off
:: ============================================================
:: Setup do Dashboard de Macros
:: Duplo-clique para instalar e configurar tudo.
:: ============================================================
title Setup - Dashboard de Macros

:: Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ERRO: Python nao encontrado no PATH.
    echo  Baixe e instale em: https://www.python.org/downloads/
    echo  Marque a opcao "Add Python to PATH" durante a instalacao.
    echo.
    pause
    exit /b 1
)

:: Ir para o diretório do script
cd /d "%~dp0"

:: Executar o setup Python
python setup_dashboard.py

pause

@echo off
chcp 65001 >nul
title Neo Energia - Launcher Automatico
echo =============================================
echo 🚀 NEO ENERGIA - LAUNCHER AUTOMATICO
echo =============================================
echo.

cd /d "%~dp0"

echo Executando script Python automatico...
echo.

.venv\Scripts\python.exe executar_automatico.py

echo.
echo Pressione qualquer tecla para sair...
pause >nul
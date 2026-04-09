@echo off
echo ============================================
echo   Dashboard de Macros - Autenticado
echo ============================================
echo.
echo Credenciais de acesso:
echo   Usuario: neo
echo   Senha:   dashboard2026
echo.
echo Pressione Ctrl+C para parar o servidor
echo ============================================
echo.

cd /d "%~dp0.."
python -m dashboard_macros

pause
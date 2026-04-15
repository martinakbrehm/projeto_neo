@echo off
echo ===================================================
echo  Scheduler de Refresh - Dashboard Macros
echo  Atualiza tabelas materializadas a cada 1 hora
echo ===================================================
echo.
echo  Uso:
echo    iniciar_refresh.bat           - Loop a cada 1h
echo    iniciar_refresh.bat --once    - Executa uma vez
echo    iniciar_refresh.bat --interval 1800 - A cada 30min
echo.

cd /d "%~dp0"
cd ..
python -m dashboard_macros.refresh_scheduler %*

pause

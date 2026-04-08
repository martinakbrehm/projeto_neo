@echo off
chcp 65001 > nul
cd /d "%~dp0.."

echo ============================================================
echo  AUDITORIA DA MACRO
echo ============================================================
echo.

where python > nul 2>&1
if errorlevel 1 (
    echo [ERRO] Python nao encontrado no PATH.
    pause
    exit /b 1
)

python auditoria_macro\auditar.py %*

echo.
if errorlevel 2 (
    echo  ** Ha alertas. Verifique o relatorio acima. **
) else (
    echo  Auditoria concluida sem alertas.
)

pause

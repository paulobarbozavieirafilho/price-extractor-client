@echo off
setlocal

echo Encerrando janelas do painel...

taskkill /FI "WINDOWTITLE eq PEC Backend*" /T /F >nul 2>nul
taskkill /FI "WINDOWTITLE eq PEC Frontend*" /T /F >nul 2>nul

echo OK. Servicos encerrados.

endlocal
exit /b 0

@echo off
setlocal

set "ROOT=%~dp0"
set "FRONT_DIR=%ROOT%frontend"
cd /d "%FRONT_DIR%"

where npm.cmd >nul 2>nul
if errorlevel 1 (
  echo [ERRO] npm nao encontrado. Instale Node.js LTS primeiro.
  pause
  exit /b 1
)

if not exist "node_modules" (
  echo Instalando dependencias do frontend...
  npm.cmd install
  if errorlevel 1 (
    echo [ERRO] Falha ao instalar dependencias do frontend.
    pause
    exit /b 1
  )
)

echo Iniciando frontend em http://127.0.0.1:5173 ...
npm.cmd run dev -- --host 127.0.0.1 --port 5173

endlocal
exit /b %errorlevel%

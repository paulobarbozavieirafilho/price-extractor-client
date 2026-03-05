@echo off
setlocal

set "ROOT=%~dp0"
cd /d "%ROOT%"

set "PY_EXE=%ROOT%.venv\Scripts\python.exe"
set "FRONT_DIR=%ROOT%frontend"

if not exist "%PY_EXE%" (
  echo [ERRO] Python da venv nao encontrado: "%PY_EXE%"
  echo Crie a venv com: python -m venv .venv
  pause
  exit /b 1
)

if not exist "%FRONT_DIR%\package.json" (
  echo [ERRO] Frontend nao encontrado em "%FRONT_DIR%"
  pause
  exit /b 1
)

where npm.cmd >nul 2>nul
if errorlevel 1 (
  echo [ERRO] npm nao encontrado. Instale Node.js LTS primeiro.
  pause
  exit /b 1
)

if /I "%~1"=="--check" (
  echo [OK] Ambiente valido.
  echo Backend: http://127.0.0.1:8001
  echo Frontend: http://127.0.0.1:5173
  exit /b 0
)

if not exist "%ROOT%run-backend.bat" (
  echo [ERRO] Arquivo nao encontrado: "%ROOT%run-backend.bat"
  pause
  exit /b 1
)

if not exist "%ROOT%run-frontend.bat" (
  echo [ERRO] Arquivo nao encontrado: "%ROOT%run-frontend.bat"
  pause
  exit /b 1
)

echo Iniciando backend em nova janela...
start "PEC Backend" cmd /k ""%ROOT%run-backend.bat""

echo Iniciando frontend em nova janela...
start "PEC Frontend" cmd /k ""%ROOT%run-frontend.bat""

timeout /t 2 /nobreak >nul
start "" "http://127.0.0.1:5173"

echo.
echo Servicos iniciados:
echo - Backend:  http://127.0.0.1:8001
echo - Frontend: http://127.0.0.1:5173
echo.
echo Para encerrar, rode: parar-painel.bat

endlocal
exit /b 0

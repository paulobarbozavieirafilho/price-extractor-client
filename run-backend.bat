@echo off
setlocal

set "ROOT=%~dp0"
cd /d "%ROOT%"

set "PY_EXE=%ROOT%.venv\Scripts\python.exe"
if not exist "%PY_EXE%" (
  echo [ERRO] Python da venv nao encontrado: "%PY_EXE%"
  echo Crie a venv com: python -m venv .venv
  pause
  exit /b 1
)

echo Iniciando backend em http://127.0.0.1:8001 ...
if /I "%~1"=="--reload" (
  "%PY_EXE%" -m app.cli serve --host 127.0.0.1 --port 8001 --reload
) else (
  "%PY_EXE%" -m app.cli serve --host 127.0.0.1 --port 8001
)

endlocal
exit /b %errorlevel%

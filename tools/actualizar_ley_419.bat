@echo off
setlocal
cd /d "%~dp0.."

set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"

echo [1/2] Simulando clasificacion Ley 419 sin ficha...
"%PYTHON_EXE%" -m tools.backfill_process_law --retry-errors
if errorlevel 1 exit /b %errorlevel%

echo [2/2] Aplicando migracion verificada y sincronizando Supabase...
"%PYTHON_EXE%" -m tools.backfill_process_law --apply --retry-errors --sync-postgres
exit /b %errorlevel%

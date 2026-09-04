@echo off
setlocal
cd /d "%~dp0.."

set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
set "UPDATER=%CD%\db\db_api_updater.py"
set "BACKFILL=%CD%\db\backfill_rir_price_evidence.py"
set "ANALYTICS=%CD%\db\build_intelligence_tables.py"
set "PUBLISH_SHEETS=%CD%\db\publish_rir_price_sheets.py"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] No se encontro el Python del repositorio: %PYTHON_EXE%
    exit /b 1
)

if not defined SUPABASE_DB_URL if not defined DATABASE_URL (
    echo [ERROR] Define SUPABASE_DB_URL o DATABASE_URL antes de ejecutar.
    exit /b 2
)

echo [1/5] Creando respaldo e inicializando columnas...
"%PYTHON_EXE%" "%UPDATER%" --reclassify-only --skip-reclassify --skip-postgres --skip-publish --skip-drive
if errorlevel 1 exit /b %ERRORLEVEL%

echo [2/5] Recuperando precios oficiales por renglon para fichas sin CT ni RS...
"%PYTHON_EXE%" "%BACKFILL%" --workers 8
if errorlevel 1 exit /b %ERRORLEVEL%

echo [3/5] Publicando la base operacional enriquecida...
"%PYTHON_EXE%" "%UPDATER%" --reclassify-only --skip-reclassify --skip-backup --postgres-full --require-postgres
if errorlevel 1 exit /b %ERRORLEVEL%

echo [4/5] Reconstruyendo y publicando la capa analitica...
"%PYTHON_EXE%" "%ANALYTICS%" --publish-postgres --require-postgres
if errorlevel 1 exit /b %ERRORLEVEL%

echo [5/5] Publicando referencias historicas y preparando investigacion en Google Sheets...
"%PYTHON_EXE%" "%PUBLISH_SHEETS%"
if errorlevel 1 exit /b %ERRORLEVEL%

echo [OK] Precios historicos RIR actualizados y publicados.
exit /b 0

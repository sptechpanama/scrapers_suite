@echo off
setlocal
cd /d "%~dp0.."

set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
set "UPDATER=%CD%\db\db_api_updater.py"
set "ANALYTICS_BUILDER=%CD%\db\build_intelligence_tables.py"

if not exist "%PYTHON_EXE%" (
    echo [ERROR] No se encontro el Python del repositorio: %PYTHON_EXE%
    exit /b 1
)

if not exist "%UPDATER%" (
    echo [ERROR] No se encontro el actualizador: %UPDATER%
    exit /b 1
)

if not exist "%ANALYTICS_BUILDER%" (
    echo [ERROR] No se encontro el constructor analitico: %ANALYTICS_BUILDER%
    exit /b 1
)

if not defined SUPABASE_DB_URL if not defined DATABASE_URL (
    echo [ERROR] SUPABASE_DB_URL/DATABASE_URL no esta definida en esta consola.
    echo [ERROR] No se iniciara una actualizacion que no pueda publicarse en Supabase.
    exit /b 2
)

"%PYTHON_EXE%" -c "import pandas, psycopg2, sqlalchemy" >nul 2>&1
if errorlevel 1 (
    echo [DEPENDENCIAS] Instalando paquetes faltantes del repositorio...
    "%PYTHON_EXE%" -m pip install -r "%CD%\requirements.txt"
    if errorlevel 1 (
        echo [ERROR] No fue posible instalar las dependencias requeridas.
        exit /b 3
    )
)

echo [INICIO] Recuperando enlaces recientes/faltantes, reclasificando la base completa y publicando...
"%PYTHON_EXE%" "%UPDATER%" --force-reclassify --postgres-full --require-postgres
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo [ERROR] La actualizacion termino con codigo %EXIT_CODE%.
    exit /b %EXIT_CODE%
)

echo [ANALITICA] Construyendo relaciones ficha-acto-proveedor y publicandolas...
"%PYTHON_EXE%" "%ANALYTICS_BUILDER%" --publish-postgres --require-postgres
set "ANALYTICS_EXIT_CODE=%ERRORLEVEL%"

if not "%ANALYTICS_EXIT_CODE%"=="0" (
    echo [ERROR] La base operacional se actualizo, pero la capa analitica fallo con codigo %ANALYTICS_EXIT_CODE%.
    exit /b %ANALYTICS_EXIT_CODE%
)

echo [OK] Base corregida, validada y publicada junto con la capa analitica.
exit /b 0

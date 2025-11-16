# Scraper de portales MINSA

Automatización basada en Selenium para extraer información pública de:

1. [Sistema de Fichas Técnicas](https://ctni.minsa.gob.pa/Home/ConsultarFichas)
2. [Criterios Técnicos](https://dndmcriterios.minsa.gob.pa/ct/Consultas/frmCRITERIOS_Criterios.aspx)
3. [Registro de Oferentes](https://appwebs.minsa.gob.pa/woferentes/Oferentes.aspx)

## Requisitos

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Uso rápido

```powershell
python -m minsa_scraper.scrape_minsa --headless --max-pages 1
```

Durante la fase actual recomendamos probar el flujo completo (incluida la exportación consolidada) con:

```powershell
python -m minsa_scraper.scrape_minsa --oferentes --headless --max-pages 5 --single-excel
```

### Opciones relevantes

- `--headless`: ejecuta Chrome sin interfaz.
- `--max-pages`: tope de páginas por portal (`0` recorre todas).
- `--skip fichas criterios oferentes`: omite uno o más portales.
- `--single-excel`: genera un único `.xlsx` con las hojas `actos`, `fichas`, `oferentes` y `resumen_run`.
- `--output-xlsx-dir`: carpeta destino del Excel único (por defecto `outputs/xlsx`).
- `--oferentes-source catalog|legacy`: usa el cat?logo p?blico por defecto o el flujo legacy por oferente.
- `--upload-to-drive`, `--drive-folder-id`, `--drive-credentials`: banderas heredadas; **la subida automática está deshabilitada en la Fase 1** y se ignoran aunque se indiquen.

## Salidas

- **Export clásico**: sin `--single-excel` se mantienen los archivos `fichas_ctni.xlsx`, `criterios_tecnicos.xlsx` y `oferentes_catalogos.xlsx` dentro de `outputs/`.
- **Excel unificado** (`--single-excel`):
  - Ruta por defecto `outputs/xlsx/scrape_YYYYMMDD_HHMM.xlsx`.
  - Hojas:
    - `fichas`: dataset CTNI deduplicado.
    - `oferentes`: registros del cat?logo p?blico (`Public/Catalogos.aspx`) ya limpios (sin paginadores ni botones de acci?n).
    - `actos`: mientras no exista un portal propio se reutiliza `criterios_tecnicos`.
    - `resumen_run`: métricas de la ejecución (`inicio`, `fin`, `duracion_s`, páginas/registros por portal, runtime errors, rutas `debug_html`, ruta del Excel generado).

## Anti-paginación y robustez

- Limpieza agresiva de tablas ASP.NET: elimina filas numéricas, etiquetas de navegación y contenedores `GridPager`; también descarta columnas de acción como “Imprimir”.
- Navegación segura de catálogos: cada `__doPostBack` se confirma mediante checksum del grid, incluye reintentos con backoff y recuperación ante `Runtime Error`.
- Métricas expuestas en consola y en `resumen_run`: páginas recorridas, registros guardados, filas descartadas y mismatches por catálogo (con HTML guardado en `outputs/debug_html`).
- Deduplicación consistente (`DEDUP_CONFIG`) tanto para los archivos clásicos como para el Excel único.

## Google Drive (Fase 1)

La subida automática (`--upload-to-drive`) queda temporalmente deshabilitada. El flag sigue aceptándose para mantener compatibilidad, pero se imprime una advertencia y no se realiza ninguna llamada a la API hasta la Fase 2.

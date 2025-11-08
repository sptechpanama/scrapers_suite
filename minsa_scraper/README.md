# Scraper de portales MINSA

Proyecto de automatización basado en Selenium para descargar la información pública de:

1. [Sistema de Fichas Técnicas](https://ctni.minsa.gob.pa/Home/ConsultarFichas)
2. [Criterios Técnicos](https://dndmcriterios.minsa.gob.pa/ct/Consultas/frmCRITERIOS_Criterios.aspx)
3. [Registro de Oferentes](https://appwebs.minsa.gob.pa/woferentes/Oferentes.aspx)

## Requisitos

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Uso

```powershell
python scrape_minsa.py
```

Opciones:

- `--headless`: ejecuta Chrome sin interfaz (útil para servidores).
- `--max-pages`: número máximo de páginas por portal (usa `0` o no lo definas para recorrer todo; útil fijarlo en `1` durante pruebas rápidas).
- `--skip fichas criterios oferentes`: omite alguno de los portales.
- `--upload-to-drive`: al terminar sube los archivos generados a la carpeta de Google Drive configurada.
- `--drive-folder-id`: ID de la carpeta destino (por defecto `0AMdsC0UugWLkUk9PVA`).
- `--drive-credentials`: ruta al JSON del service account con permisos de Drive (por defecto `~/orquestador/pure-beach-474203-p1-fdc9557f33d0.json` si existe).

Cada ejecución genera tres archivos `.xlsx` dentro de la carpeta indicada:

- `fichas_ctni.xlsx`
- `criterios_tecnicos.xlsx`
- `oferentes_catalogos.xlsx`

Estos archivos siempre se guardan en la carpeta `outputs/` del proyecto. Si ya existen, se sobrescriben.

> Nota: El scraper está preparado para ampliar la paginación una vez validados los resultados de la primera página.

### Subir automáticamente a Google Drive

Para replicar la configuración de GEAPP basta con ejecutar:

```powershell
python scrape_minsa.py --upload-to-drive
```

El script reutiliza las credenciales del service account `pure-beach-474203-p1` (carpeta `orquestador`) y actualiza/crea los archivos en la carpeta `Bases_de_datos` (`0AMdsC0UugWLkUk9PVA`). Si quieres usar otra ruta o credenciales basta con sobrescribir `--drive-folder-id` y `--drive-credentials`. Asegúrate de compartir la carpeta destino con `finapp-sa@pure-beach-474203-p1.iam.gserviceaccount.com` con permiso de editor para que la subida funcione.

# Monitor CTNI

Monitor HTTP diario integrado al orquestador existente. No usa Selenium.

## Fuentes oficiales

- `POST /Formularios/CargarFormulariosEstado`: solicitudes.
- `GET /Formularios/FormularioInfo?Id={id}`: hitos y estado de solicitudes.
- `POST /Home/LoadFichasTrabajadas`: fichas elaboradas o modificadas.
- `GET /`: homologaciones y avisos de cancelación, suspensión o reprogramación.
- `POST /Home/LoadFichas`: comprobación secundaria de fichas publicadas.

## Persistencia

- SQLite: `data/ctni/ctni_monitor.db` (histórico operativo y auditoría).
- Google Sheets: `ctni_solicitudes`, `ctni_homologaciones`, `ctni_fichas`,
  `ctni_eventos` y `ctni_health` en el libro de Panamá Compra.

Cada fuente crea su propia línea base en su primer éxito. Esa corrida no genera
notificaciones antiguas. Un fallo HTTP o de Google Sheets no borra datos; el
espejo pendiente se reconstruye desde SQLite en el siguiente éxito.

`LoadFichasTrabajadas` no ofrece un orden estable para su paginación. El monitor
realiza dos pasadas y une las filas por la clave oficial solicitada. Además,
conserva una marca de agua del ID oficial para incorporar hallazgos históricos
tardíos sin enviarlos por correo como publicaciones nuevas.

## Ejecución

```powershell
& "C:\Users\rodri\scrapers_repo\.venv\Scripts\python.exe" `
  "C:\Users\rodri\scrapers_repo\ctni_monitor\scrape_ctni.py"
```

El `config.json` del orquestador lo agenda diariamente a las 05:15. El trabajo
se inyecta como configuración obligatoria aunque todavía no exista una fila en
`pc_config`.

## Configuración opcional

- `CTNI_SPREADSHEET_ID`: libro de datos; por defecto usa el actual de Panamá Compra.
- `CTNI_REQUEST_DETAIL_LIMIT`: detalles máximos por corrida (por defecto 15000).
- `CTNI_REQUEST_DETAIL_WORKERS`: concurrencia de detalles (por defecto 6).
- `CTNI_FICHA_PASSES`: pasadas del historial de fichas (por defecto 2; máximo 4).
- `CTNI_SHEET_HISTORY_DAYS`: historial de fichas reflejado en Sheets (por defecto 1825).
- `ORQUESTADOR_CTNI_EMAIL_FROM`, `ORQUESTADOR_CTNI_EMAIL_APP_PASSWORD` y
  `ORQUESTADOR_CTNI_EMAIL_TO`: solo si se desea sobreescribir la configuración
  ya utilizada por CT_RIR.

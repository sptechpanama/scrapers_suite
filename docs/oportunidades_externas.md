# Oportunidades externas · primera etapa

La página `pages/oportunidades_externas.py` de GEAPP consulta las tablas `external_*` existentes en Supabase. El servidor sigue ejecutando el job `otras_fuentes`. No modifica los scrapers de Panamá Compra ni sus hojas.

## Fuentes locales

- ACP SLI: formulario público `https://apps.pancanal.com/sli/`, POST al action de `#frmBusqueda3`, estados AN/EN y paginación oficial. Identidad: número de licitación. Detalle público por `RedirectLicitaciones`, renglones y documentos expuestos por los controles del portal. Los PDF requieren el contexto de la sesión pública del acto y Referer. TLS permanece validado con el almacén de certificados del sistema (`truststore`).
- ACP: conserva los estudios de mercado existentes, separados de las licitaciones.
- ENSA: listado completo `/licitaciones/` y paginación, complementado con RSS. Conserva los identificadores anteriores, fechas de cierre y enlaces oficiales.
- ENA: agrupa código, título y todos los anexos de una misma tarjeta. Los antiguos registros de anexos se conservan vinculados al acto mediante `superseded_by`, sin duplicar la vista.
- IDAAN: tablas oficiales por encabezado. El importe adjudicado se conserva como tal y no se presenta como presupuesto de referencia.
- Cruz Roja Panameña: listado, fechas y adjuntos de cada compra.
- Ciudad del Saber: paginación de su API; detección de páginas repetidas.
- IFRC: adaptador público preparado. Un bloqueo HTTP se registra como fuente no disponible, nunca como captura vacía exitosa.

Las fuentes internacionales previamente instaladas permanecen disponibles. No se agregaron las etapas futuras de cooperación, donantes o subcontratación.

## Clasificación y persistencia

Cada corrida lee en un único batch las listas existentes `pc_palabras_clave`, `pc_palabras_negativas` y `ct_rir_fichas`, con credenciales de solo lectura y transporte Google compartido. Ante un fallo conserva el último perfil válido en `data/otras_fuentes/profiles.json`. Una lista vacía guardada por el usuario no se rellena con valores predeterminados.

Las fichas se vinculan solamente cuando existe un número explícito con la etiqueta ficha técnica. Los códigos de clasificación no son fichas. Las coincidencias generales de producto no prueban equivalencia MINSA ni elegibilidad comercial. Si una palabra tiene umbral y el portal no publica presupuesto, la coincidencia queda por revisar: nunca se inventa el monto.

Las capturas parciales conservan las filas recibidas y el histórico, pero no marcan una línea base completa. La primera corrida de una fuente es silenciosa. Se mantienen los eventos y la deduplicación del mecanismo de correo existente.

Los documentos se leen en el servidor, con caché SQLite persistente y presupuesto por corrida. Se priorizan enlaces nunca consultados. Límite de archivo: 10 MB; lectura de PDF hasta 150 páginas. PDF escaneados, formatos no interpretados y pendientes se identifican; no se afirma cobertura documental completa en esos casos.

## Operación

Horario de respaldo existente: todos los días a las 06:20, 12:20 y 18:20, según la configuración vigente del orquestador. El equipo servidor debe estar encendido y el orquestador activo.

`orquestador/run_otras_fuentes.py` exige Supabase de forma predeterminada. Admite `--silent`, `--sources acp_sli ensa ...` y `--local-only` para una ejecución que permita continuar sin publicación. El botón de captura escribe el job `otras_fuentes` en la misma cola `pc_manual`; no inicia un scraper dentro de Streamlit.

## Validación inicial · 14/09/2026

Consulta silenciosa publicada en Supabase: 1,210 registros leídos, 295 nuevos, 90 cambios y cero eventos de correo. ACP SLI: 42 actos; ENSA: 14; ENA: 260; IDAAN: 23; Cruz Roja: 36; Ciudad del Saber: 39. Estos conteos incluyen histórico; no equivalen a oportunidades vigentes para las empresas.

Bloqueos conservados con evidencia: UNGM regional HTTP 429, BID HTTP 409 e IFRC HTTP 403. No se borró su histórico. Copias previas de SQLite y tablas remotas en `data/otras_fuentes/backups/external_page_20260914_164414`.

Pruebas: paginación, identidad de actos, anexos, fechas ISO y zona Panamá, estados parciales, conservación de perfiles, fichas explícitas, montos desconocidos, ausencia de eventos en repetición, conservación de histórico, consultas parametrizadas, navegación selectiva, paginación y cola manual. Streamlit AppTest con Supabase: navegación entre todas/RS-SP/RIR/fuentes, órdenes y paginación sin excepciones.

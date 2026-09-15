# Revisión comercial de Otras fuentes

El monitor mantiene trece fuentes de avisos y dos canales de acceso a proveedores
(Naturgy y AES). Horario: todos los días a las 06:20, 12:20 y 18:20 de Panamá,
con el servidor y el orquestador activos. La captura,
la lectura de documentos y la clasificación corren en el servidor. Streamlit solo
consulta Supabase, con filtros y paginación en SQL.

- **Para evaluar**: coincidencia sectorial, plazo conocido vigente y sin una
  restricción detectada que requiera revisión. No certifica elegibilidad ni margen.
- **Por revisar**: coincidencia ambigua, fecha desconocida, documento pendiente,
  consulta de mercado, alcance integral o restricción de proveedor local extranjero.
- **Histórico**: cierre explícito o fecha/hora de cierre pasada. Una ampliación
  oficial de plazo puede devolver el aviso a las vistas actuales.
- **Sin encaje**: no se confirma relación comercial con RS/SP o RIR. Se conserva.
- **Todos** permite recuperar cualquier registro. La agrupación por código y URL oficial
  es reversible y no fusiona convocatorias solo por tener títulos parecidos.

La vigencia se calcula también al consultar, sin esperar la siguiente corrida.
Se respeta la zona horaria cuando el portal la publica. Sin hora confirmada se
conserva el día completo de cierre; sin fecha no se inventa una. Un aviso sin
reconfirmar durante siete días pasa a revisión, no se borra.

El detalle público usa caché de 4 horas (1 hora tras error), un presupuesto de
180 URLs por corrida, tres trabajadores, timeout y descarga máxima de 10 MB.
Se reserva presupuesto por fuente y para los adjuntos; los avisos vigentes se
leen antes que los históricos y las enmiendas invalidan la caché correspondiente.
PDF: máximo 150 páginas y 80,000 caracteres por documento. Escaneados, documentos incompletos o
descargas fallidas quedan para revisión; el texto anterior no se pierde. El módulo
no ejecuta OCR ni interpreta archivos Office/ZIP automáticamente. El presupuesto
pendiente continúa en corridas siguientes gracias a la caché.

Las alertas solo se crean para novedades sustantivas de la vista Para evaluar.
Se reutiliza la configuración SMTP de CTNI/CT_RIR. El proceso hijo del orquestador
gestiona una cola persistente en la misma base SQLite y replica comprobantes a
Supabase (`external_email_deliveries`). Un fallo previo a la entrega se reintenta
en la siguiente corrida, con al menos 15 minutos entre intentos. Cada destinatario
tiene su comprobante: una aceptación parcial no provoca reenvíos a quienes ya
recibieron aceptación SMTP. Un corte durante DATA se conserva como `uncertain`:
requiere revisar la bandeja antes de autorizar un reintento. SMTP no permite
garantizar exactamente una entrega ante todos los cortes de conexión.

Los eventos anteriores a la migración sin evidencia de envío quedan como
`legacy_unverified`; no se reenvían masivamente. Los avisos ya vencidos o sin encaje
dejan de ser candidatos de reintento. Los nuevos destinatarios no reciben historial.
`--silent` no crea eventos ni envía correos. El resumen del hijo deja `events=[]`
y `notifications_handled=true` para que la versión en memoria del orquestador
no envíe una segunda copia. No es necesario reiniciar otros scrapers.

ENSA sigue también `link[rel=next]` en la cabecera: el portal puede no mostrar
botones de paginación. El RSS es complementario; fallar no borra fechas conocidas.
Una página repetida o fallida produce estado parcial y conserva lo leído.

BID consulta primero el API público de BID for the Americas, enlazado desde
https://www.iadb.org/en/how-we-can-work-together/procurement/procurement-projects/procurement-notices
(`https://bidfa-admin.connectamericas.com/items/tenders`, estado `approved`).
Comprueba total oficial y paginación, usa IDs `bidfa:<id>` y conserva los documentos
oficiales. Las horas se muestran en Panamá conservando el instante original UTC.
El campo amount sin moneda explícita queda como evidencia, no como monto USD.
La fecha de publicación es el alta en el catálogo, no una fecha de creación del
proyecto inferida. CKAN/CSV quedan como respaldo y se rechazan cuando su última
publicación tiene más de 90 días: un archivo viejo no demuestra cero oportunidades.

Streamlit muestra comprobantes en «Fuentes y cobertura → Correo de alertas».
La confirmación SMTP se distingue de la llegada a la bandeja. IFRC puede bloquear
el acceso automático (403); su página queda enlazada y el estado permanece visible.

Mantenimiento manual: `python orquestador/reclasificar_otras_fuentes.py` simula.
`--apply --require-postgres` guarda, con respaldo SQLite y JSONL de Supabase antes
de modificar. Usa `SUPABASE_DB_URL` y `OTRAS_FUENTES_DB_PATH`; no almacena secretos.
`--detail-budget N` permite consultar hasta N documentos públicos (máximo 200).
La reclasificación conserva IDs, primeras detecciones, últimas observaciones,
historial, corridas y alertas. No cambia la fecha de última captura oficial.

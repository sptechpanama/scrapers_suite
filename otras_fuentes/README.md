# Revisión comercial de Otras fuentes

El monitor mantiene los once adaptadores y su horario del orquestador. La captura,
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

El detalle público usa caché de 12 horas (1 hora tras error), un presupuesto de
90 URLs por corrida, tres trabajadores, timeout y descarga máxima de 10 MB.
PDF: máximo 25 páginas y 80,000 caracteres. Escaneados, documentos incompletos o
descargas fallidas quedan para revisión; el texto anterior no se pierde. El módulo
no ejecuta OCR ni interpreta archivos Office/ZIP automáticamente. El presupuesto
pendiente continúa en corridas siguientes gracias a la caché.

Las alertas solo se crean para novedades sustantivas de la vista Para evaluar.
Se mantiene el envío existente del orquestador; los avisos en revisión se consultan
en Streamlit. No se envían correos antiguos al reclasificar.

Mantenimiento manual: `python orquestador/reclasificar_otras_fuentes.py` simula.
`--apply --require-postgres` guarda, con respaldo SQLite y JSONL de Supabase antes
de modificar. Usa `SUPABASE_DB_URL` y `OTRAS_FUENTES_DB_PATH`; no almacena secretos.
`--detail-budget N` permite consultar hasta N documentos públicos (máximo 200).
La reclasificación conserva IDs, primeras detecciones, últimas observaciones,
historial, corridas y alertas. No cambia la fecha de última captura oficial.

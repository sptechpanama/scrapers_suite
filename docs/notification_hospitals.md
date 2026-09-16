# Hospital y unidad de compra en las alertas

Las alertas de PanamáCompra conservan la entidad, hospital explícito, unidad
solicitante, unidad de compra y dependencia hasta componer el correo. Antes,
estos campos existían en las filas pero se perdían en los resúmenes de los
scrapers y del monitor del orquestador.

## Recorrido corregido

- CL abiertas, CL programadas y actos públicos: resúmenes CT RIR de los tres scrapers.
- Resúmenes de palabras clave RS/SP y ambos monitores que leen Sheets.
- Colas y correos CT RIR y RS/SP, conservando sus claves de deduplicación.
- Recordatorio adicional de la ficha 43358: confirma además la unidad con el
  detalle oficial al verificar la fecha vigente.

`common/notification_entity.py` normaliza los nombres de columnas de Sheets,
SQLite y API pública. Conserva los nombres oficiales, evita repetir la misma
unidad y no atribuye hospitales a partir del título, producto o provincia.
Si no hay información, el correo dice que el hospital/unidad no está informado.
Una compra centralizada de MINSA se presenta como tal, sin inventarle hospital.

No cambia destinatarios, credenciales, filtros, fichas vigiladas ni historial de
envío. No requiere migración de Supabase ni cambios en Streamlit. El orquestador
debe cargar el código actualizado; sus procesos activos se dejan terminar.

## Validación

`tests/test_notification_entity.py` prueba alias, filas incompletas, valores
nulos, unidades distintas, los bloques reales de resumen de CLV/CLRIR/RIR1,
los lectores de Sheets, el recorrido resumen-cola-SMTP con transporte simulado,
deduplicación y la unidad obtenida del detalle oficial para recordatorios.
Las pruebas no envían correos reales.

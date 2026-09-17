# Unidad de compra en las alertas

Los correos conservan su formato habitual. Inmediatamente debajo de `Entidad`
se muestra únicamente `Unidad de compra`, con el nombre oficial: hospital,
escuela, región o departamento de compras.

Se usa `unidad de compra` y, para filas antiguas, `unidad solicitante` (el nombre
que empleaban los scrapers para ese mismo campo). No se infiere a partir del
título, de la provincia ni de un hospital mencionado como destino. Si falta el
campo, se indica `No especificada en la fuente`.

Esto se aplica a CL abiertas, programadas, actos públicos, resúmenes de RS/SP,
CT RIR, monitores de Sheets y recordatorios de la ficha 43358. No cambia
destinatarios, filtros ni deduplicación. No requiere migraciones de Supabase.

`tests/test_notification_entity.py` verifica el recorrido scraper → resumen →
cola → correo con SMTP simulado, alias, unidades no hospitalarias y datos vacíos.

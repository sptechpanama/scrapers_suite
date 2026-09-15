# Fuentes externas: fiabilidad y acceso a proveedores

## Cambios

- UNGM conserva páginas ya leídas cuando falla una consulta posterior, contrasta
  el total oficial y marca límites, páginas repetidas y respuestas no interpretables.
  El máximo predeterminado pasa de 20 a 200 páginas por perfil. Se conserva el
  tamaño real de 15 registros: el portal ignora un tamaño solicitado de 50.
- Las consultas a UNGM comparten una pausa mínima de cuatro segundos, incluso en
  la lectura paralela de detalles. HTTP 429 respeta Retry-After; no se insiste
  antes del plazo indicado ni se rotan identidades. Los 4xx permanentes no se reintentan.
- ACP diferencia el alias de página 1 de una repetición real del contenido.
  ENSA también detecta paginación repetida. Una lectura incompleta conserva filas.
- El presupuesto documental se distribuye entre fuentes. UNGM puede enriquecer
  avisos aunque el título no contenga todavía palabras del perfil. Los enlaces
  nunca leídos se priorizan, con preferencia por coincidencias RS/SP o RIR.
- Los detalles se refrescan cada cuatro horas y se invalidan ante cambios de fecha
  o revisión oficial. Los errores conservan el texto anterior y se identifican
  como pendientes; no prueban la lectura de una enmienda nueva.
- BID pagina el recurso API y valida sus columnas; ante fallo consulta el CSV
  enlazado en el catálogo oficial. HTML, archivos vacíos y HTTP 202 no se aceptan
  como cero oportunidades. Si ambas vías fallan se conserva el historial.
- IFRC apunta a su dirección oficial actual. Un bloqueo 403 sigue siendo un fallo
  externo explícito; no se afirma que no existan licitaciones.
- Naturgy y AES verifican sus páginas oficiales, con estado `access_required`.
  No crean oportunidades ficticias, alertas ni una línea base de compras privadas.

## Integración

Se usa el job `otras_fuentes` existente y las tablas `external_*`. Se añade
`external_source_access` para el seguimiento por fuente y empresa, con RLS en
PostgreSQL. Las capturas nunca sobrescriben el seguimiento manual.

La publicación en Supabase es obligatoria por defecto. El wrapper conserva los
eventos de las fuentes correctas aunque otra quede parcial; registra una advertencia
con el desglose. Devuelve error si no logra confirmar la publicación obligatoria.
La corrida de validación usa `--silent` para no crear notificaciones de prueba.

## Límites conocidos

La captura verifica listados públicos y los adjuntos que alcanza a leer. No certifica
el universo privado de cada comprador ni la elegibilidad de una empresa. Los portales
pueden actualizar el listado durante la paginación; esa variación se marca como parcial.
Los límites HTTP, archivos sin texto, descargas bloqueadas y lecturas pendientes
se mantienen visibles, sin borrar oportunidades guardadas.

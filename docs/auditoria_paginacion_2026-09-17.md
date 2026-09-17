# Auditoría de paginación y detalle — 17 de septiembre de 2026

## Resultado

Los tres listados empiezan por la página 1, con 50 actos por página. El número
50 era el tamaño de página o el acumulado, no una instrucción para omitir los
primeros 49 actos. Los registros 51–100 pertenecen a la segunda página.

No era correcto asegurar que toda la extracción estaba garantizada: se
encontraron problemas reales de final de listado y tratamiento de timeouts.

## Evidencia en el portal público (solo lectura)

| Scraper | Resultado verificado en este corte |
|---|---|
| CL abiertas | El navegador se quedó en 900 actos, página 18, con `Cargando...` y `Total: 900 +`. La API oficial permitió recuperar un listado de 906 actos abiertos. |
| CL programadas | Verificación independiente: 200 actos en 4 páginas y carga pendiente del siguiente lote; la API oficial devolvió 213 programadas. |
| Actos públicos vigentes | 372 actos únicos en 8 páginas; última página con 22 registros y final confirmado. |

Son cortes de una fuente cambiante: algunos actos aparecen, se abren o dejan
de estar vigentes entre dos consultas. No equivalen a cantidades de nuevos
registros publicados en Sheets. La clasificación, historial y descartes
comerciales existentes continúan aplicándose.

Una primera prueba reutilizó la sesión del navegador entre CL abiertas y
programadas y observó filas retenidas de la vista anterior. Se añadió
verificación explícita del estado de cada fila y se repitió programadas en
una sesión independiente. No se atribuye ese resultado preliminar al listado
real de programadas.

## Correcciones

1. Verificar página 1, identidad de los actos y cambio real de filas antes de
   seguir. Un clic en Siguiente no equivale a una página descargada.
2. El indicador `Página x/y +` es un límite provisional: no se utiliza `x >= y`
   como prueba de final. Siguiente ausente, bloqueado o con carga pendiente
   produce una captura parcial, nunca un éxito silencioso.
3. Si el navegador se atasca, consultar el mismo estado en el endpoint público
   `POST /busqueda/proceso-lista-publico`, con ventanas de fechas y división
   de ventanas saturadas o fallidas. Los reintentos son limitados. El extremo
   superior incluye fechas futuras para no excluir cotizaciones programadas.
4. Deduplificar el listado por número oficial del acto. Registrar primera y
   última identidad, registros de cada página, total único y causa de faltantes.
5. Validar que cada detalle corresponde al número solicitado, con título y
   entidad cargados. Una pantalla anterior, vacía o de error no es un detalle.
6. En RIR1, eliminar el descarte automático `skip_timeout_xpath`: los errores
   técnicos se reintentan. Las marcas antiguas se ignoran como filtro de
   exclusión cuando el acto vuelve a aparecer en el listado vigente.
7. Guardar fallos en `data/scrape_coverage/*_pending_details.json`. Los resueltos
   se retiran después de publicar. Los que no reaparecen quedan como evidencia;
   no se insertan en una categoría activa sin verificar su estado.
8. El orquestador sigue notificando los resultados válidos de una corrida
   parcial, pero registra `partial` y su causa en vez de afirmar éxito completo.

El scraper histórico Selenium también usa la paginación verificada. El
actualizador de base por API ya divide ventanas al alcanzar 5,000 registros;
se prueba la continuidad de los límites y que un error de la fuente se propague.

## Unidad de compra

El correo conserva su formato y muestra `Unidad de compra` inmediatamente
debajo de `Entidad`. Usa el valor oficial (hospital, escuela, región u oficina).
No infiere un hospital ni agrega explicaciones o documentos al correo.

## Validación y límites

Se prueban páginas lentas, tablas retenidas, páginas repetidas, listados vacíos,
errores de API, ventanas saturadas, persistencia de pendientes, identidad de
detalle, deduplicación de correos y estados abierta/programada. Las pruebas
SMTP usan un transporte simulado, sin enviar correos de prueba.

La apertura real de detalles se comprueba en posiciones 1, 50, 51, intermedia
y final de cada listado. Es una muestra de detalles, no una lectura manual
de todos los documentos de todos los actos. Los scrapers validarán cada nuevo
detalle en las corridas ordinarias.

Esta auditoría no garantiza disponibilidad permanente de PanamáCompra ni
convierte una publicación parcial en completa. Los fallos quedan visibles.
Además, el log del actualizador de base de las 00:30 de hoy registra que la
publicación a Supabase falló por resolución DNS del host; SQLite/Drive sí se
actualizaron. Ese fallo es distinto de la paginación y no se presenta aquí como
una sincronización de Supabase verificada.

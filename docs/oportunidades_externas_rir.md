# Detección de productos RIR en portales externos

Las convocatorias externas se clasifican por palabras del producto, sin exigir
números de ficha MINSA. En cada corrida se leen las columnas A y B de
`ct_rir_fichas`: número y nombre. El número identifica el producto que se sigue;
no constituye una coincidencia comercial por sí solo.

Los nombres ausentes se resuelven con los catálogos existentes del servidor.
Si falla Sheets o el catálogo se conserva el último perfil válido. Una lista
válida vacía sí retira el seguimiento; quitar una ficha también quita sus
reglas derivadas. Los términos médicos generales existentes se mantienen.

El clasificador de oportunidades externas tolera acentos, plurales y nombres
recortados. Las familias revisadas de productos incorporan variantes españolas
e inglesas (circuitos de anestesia, tubos endotraqueales, nebulizadores, etc.).
Los términos deben aparecer próximos, en el mismo campo. Una coincidencia
relaciona una familia comercial y no certifica medidas, marca o equivalencia
con la ficha MINSA. Nombres genéricos aislados, como cilindro o cuña, no bastan.

`rir_product_matches` guarda nombre, ficha de referencia, términos, campo y
fragmento de evidencia. `explicit_fichas` conserva separadamente los números
realmente escritos en el anuncio. Las reglas RS/SP, montos y negativos son los
mismos que se guardan actualmente en Panamá Compra.

La comprobación del 14/09/2026 recuperó ACP SLI: 42 registros, ENSA: 14, ENA: 260,
IDAAN: 23, Cruz Roja: 36 y Ciudad del Saber: 39. Todos esos identificadores
estaban en Supabase. Esto verifica los listados públicos consultados, incluyendo
históricos, y no todos los documentos ni las convocatorias privadas. ACP
consulta anuncios y enmiendas; el portal de IDAAN seguía mostrando cierres de
2025. No se declara cobertura total de todas las compras de esas entidades.

La simulación sobre 3,689 registros descartó un circuito de control HVAC y
soluciones farmacológicas para nebulizar. Las coincidencias por producto
confirmadas aparecieron en cinco registros históricos de Cruz Roja. No cambió
la pertenencia de ningún registro a RS/SP. La reclasificación es silenciosa y
no cambia fechas de captura, contenido oficial ni eventos de correo.

Validación: pruebas de palabras, nombres, caché, eliminación, contextos
ambiguos y dos corridas iguales sin duplicados ni alertas; pruebas existentes
de fuentes, calidad, almacenamiento y orquestador. Los documentos escaneados,
fallidos o pendientes permanecen como tales y requieren completar su lectura.

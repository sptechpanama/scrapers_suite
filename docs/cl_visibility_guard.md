# Conservar CL cuando el cierre no esta confirmado

Caso verificado el 16/09/2026: `2026-1-10-01-03-CL-050508`, ficha 43358,
referencia USD 37,500. La captura lo habia guardado en `cl_abiertas_ct_rir` y
`cl_abiertas_rir_con_ct`. A las 12:08 lo archivo como pendiente al no aparecer
en un listado y no encontrar cuadro de cotizaciones. La consulta puntual al
API oficial seguia devolviendo estado 8, Abierta.

Correccion:

- Una ausencia del listado permite inspeccionar, pero no basta para retirar.
- Si la fecha no ha vencido y la inspeccion no confirma resultado final, la
  fila permanece visible. Los errores de carga y un cuadro ausente no son cierre.
- Para comparar con el listado se usa tambien el numero oficial; cambiar el
  token del enlace no equivale a desaparecer.
- Una observacion pendiente anterior al vencimiento no inventa `closed_at`.
- Al vencer la fecha o verificarse un resultado final, continua el flujo
  existente de archivo, conservando registros y observaciones durables.

Se recupero el caso indicado en las dos hojas, SQLite y Supabase, sin eliminar
su observacion anterior. La correccion general vive en `common/cl_lifecycle.py`
y el punto de retiro de filas de `clv/clv.py`.

Pruebas: `tests/test_cl_visibility_guard.py` y `tests/test_cl_lifecycle.py`.

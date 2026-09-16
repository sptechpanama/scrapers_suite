# Publicación de la base y sus capas analíticas

El servidor debe construir Inteligencia PC con el mismo código de GEAPP que
despliega Streamlit. Un constructor antiguo puede omitir cotizaciones recientes
y resultados de participación aunque termine sin excepciones.

## Ruta de GEAPP

El pipeline usa, en este orden:

1. La variable de entorno `GEAPP_ROOT`, cuando está definida.
2. El campo `geapp_root` del archivo local `db/update_config.json`.
3. La ruta predeterminada `~/GEAPP`.

Ejemplo del campo que se agrega a la configuración local, conservando los demás:

```json
{"geapp_root": "C:/ruta/al/checkout/GEAPP"}
```

No publiques credenciales ni la configuración privada del servidor. La ruta debe
apuntar a un checkout existente y actualizado, no a una copia antigua de scripts.

Antes de modificar la base, el pipeline comprueba que el constructor incluya el
ciclo de cotizaciones y las columnas de origen y resultado de participación. Si
ese contrato falta, informa el error y bloquea la publicación. No sustituye una
capa actual por otra que omita esas funciones.

## Ejecuciones

- La actualización diaria captura cambios y exige PostgreSQL. No reclasifica
  todo el archivo histórico por un cambio de catálogo.
- La tarea semanal mantiene la reclasificación y reconciliación completas.
- Las solicitudes manuales esperan cuando una publicación de base está en curso.
- Los tiempos máximos y las interrupciones terminan el árbol completo del
  proceso propio, evitando hijos que conserven el bloqueo de SQLite.
- Los errores previos a la captura no reutilizan metadatos de corridas anteriores.

Los registros de cada etapa quedan en `data/pipeline_logs`. Para confirmar una
recuperación hay que comprobar también los conteos y fechas de Supabase, las dos
capas analíticas, la publicación de precios RIR y los estados de `pc_state`.

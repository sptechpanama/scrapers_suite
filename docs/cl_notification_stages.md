# Alertas de cotizaciones: programada y abierta

Las alertas CT RIR y RS/SP conservan sus filtros, destinatarios y horarios.
Para CL, el número oficial del proceso y su etapa identifican el evento:

- Programada: un aviso al detectarla en `cl_prog_*`.
- Abierta: otro aviso al detectarla en `cl_abiertas*`, aunque no cambien el
  enlace, la fecha publicada ni el precio.
- Una misma etapa no vuelve a avisarse por cambiar el token del enlace o la
  clasificación entre hojas de esa etapa.
- RS/SP mantiene avisos de cambios de fecha o monto, separados por etapa.
- Si una fila programada antigua sigue presente junto a la abierta, no produce
  un aviso regresivo. La clasificación depende de la hoja fuente, no del job
  que revisa varias hojas.

## Persistencia y actualización

`orquestador/notification_stages.py` contiene el estado de eventos, sin red ni
acceso a archivos. `main.py` lo integra en los resúmenes de los scrapers y el
escaneo de recuperación de hojas. Usa el mismo `state.json` y las colas SMTP
existentes, con el campo adicional `<modulo>_cl_notification_stages`.

La primera observación de un acto legado se registra como línea base silenciosa
para evitar correos históricos al desplegar. No se añade a correos enviados.
Las transiciones posteriores de programada a abierta sí generan aviso. El
historial anterior no se borra ni se reinicializa. Los avisos pendientes de
RS/SP se reintentan después de cada corrida exitosa de CL/AP, incluso cuando
no aparecen nuevas coincidencias. Los pendientes se conservan si falla SMTP.

La etapa aparece explícitamente en el cuerpo del correo. Aceptación SMTP no
equivale a confirmación de llegada a la bandeja del destinatario.

Los cambios requieren reiniciar el orquestador en una ventana sin scrapers
activos. No requieren cambios de tablas en Supabase ni redespliegue de Streamlit.
No ejecutar funciones de envío para probar: usar transporte simulado.

## Validación

`tests/test_cl_notification_stages.py` prueba las dos empresas, transición,
repeticiones, cambios de enlace, coexistencia de etapas, migración silenciosa,
independencia de colas, cambios de fecha, preservación de pendientes, envío
simulado y reintento sin nuevos registros. El escaneo existente conserva sus
pruebas de palabras contextuales, límites de monto y horarios.

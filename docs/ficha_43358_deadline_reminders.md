# Recordatorio de ultimo dia: ficha 43358

Se ejecuta en el orquestador actual, con los destinatarios y credenciales de
CT RIR. Un monitor independiente consulta cada cinco minutos desde las 07:00,
hora de Panama, todos los dias. Si la computadora estuvo apagada, revisa al
arrancar. No depende de que termine un scraper ni necesita abrir Streamlit.

El recordatorio es adicional a los avisos de programada y abierta. Para cada
numero de acto y fecha de cierre solo se marca un envio despues de aceptacion
SMTP. Un fallo de API o SMTP queda registrado y permite reintentar en la
siguiente revision. Una fecha oficial reprogramada genera un nuevo recordatorio
en su nuevo ultimo dia. No se envian recordatorios de fechas vencidas.

Fuentes: hojas vigentes de CT RIR; comprobacion oficial por numero exacto en
`POST /busqueda/proceso-lista-publico` (CL abierta/programada o AP vigente) y
detalle publico del flujo mas reciente. Solo se usan filas que contienen la
ficha 43358. Las fechas se interpretan en America/Panama; el correo distingue
las fechas sin hora, sin presentar medianoche como una hora oficial.

Estado persistente en `state.json`:

- `ct_rir_43358_reminder_sent_keys`: numero + fecha, separado del historial normal.
- `ct_rir_43358_reminder_last_sent`: fecha, asunto y actos del ultimo envio.
- `ct_rir_43358_reminder_watchdog`: resultado de la ultima revision y errores.

Modulo: `orquestador/ficha_deadline_reminders.py`.
Pruebas sin red ni correos: `tests/test_ficha_deadline_reminders.py`.
El despliegue necesita reiniciar el orquestador sin trabajos activos.

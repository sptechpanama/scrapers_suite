# Inteligencia de oportunidades y proveedores V3

## Objetivo

La V3 reemplaza el análisis fragmentado de las páginas históricas por una sola
vista analítica, rápida y reproducible. La base operacional `panamacompra.db`
no se modifica: el proceso construye una capa derivada y normalizada que puede
consultarse localmente o publicarse en Supabase.

## Componentes

- Página unificada: `C:\Users\rodri\GEAPP\pages\inteligencia_oportunidades_proveedores.py`
- Servicio analítico: `C:\Users\rodri\GEAPP\services\inteligencia_proveedores_v3.py`
- Integración con el orquestador: `C:\Users\rodri\GEAPP\services\inteligencia_orquestador_v3.py`
- Constructor de datos: `C:\Users\rodri\scrapers_repo\db\build_intelligence_tables.py`
- Worker de estudio profundo: `C:\Users\rodri\scrapers_repo\orquestador\intel_ficha_worker.py`
- Base analítica local: `C:\Users\rodri\scrapers_repo\data\db\inteligencia_proveedores.db`

## Modelo analítico

La versión 3.2.0 genera estas tablas:

1. `intel_actos_fichas`: una fila por acto y ficha detectada. Conserva fecha,
   evidencia, confianza, unicidad, entidad, montos, ganador y participantes.
2. `intel_acto_proponentes`: una fila por proponente de cada acto, con monto
   ofertado e indicador de ganador.
3. `intel_metricas_ficha_mes`: métricas mensuales materializadas para cuatro
   dimensiones temporales y tres perfiles de confianza.
4. `intel_ficha_metadata`: nombre, descripción, área, tipo de producto,
   especialidad, CT, registro sanitario y enlace MINSA.
5. `intel_ficha_catalogo`: oferentes, contactos, productos, marcas y modelos.
6. `intel_build_metadata`: versión, fecha y conteos de la compilación.

El constructor deduplica por `(acto_key, ficha)`. Si un acto repite una misma
ficha varias veces, sigue contando como un acto y como ficha única cuando no
existe otra ficha distinta en ese acto.

## Filtros y alcance

El usuario puede combinar:

- periodo predefinido o fechas exactas;
- fecha de publicación, celebración, adjudicación o actualización;
- perfil flexible, moderado o estricto;
- estados, entidades, áreas y tipos de producto;
- presencia de CT y registro sanitario;
- frases de búsqueda con lógica AND u OR;
- rangos de precio de referencia y monto adjudicado;
- mínimos de actos, entidades y meses activos;
- máximo de participantes promedio;
- favoritos, catálogo Foyomed, proveedor en catálogo o proveedor contactable.

Los filtros de actos se ejecutan en SQL antes de agregar. Los filtros agregados
se aplican mediante `HAVING` o sobre el resultado ya resumido, según corresponda.
No se toma una muestra previa de filas.

## Métricas y ordenamiento

La tabla maestra calcula sobre todo el universo filtrado:

- actos totales y actos con ficha única;
- entidades y meses activos;
- montos de referencia y adjudicados;
- ticket promedio, mediano y máximo;
- participantes promedio y mediana;
- proponentes distintos;
- ganadores Top 1, 2 y 3;
- concentración Top 3 e índice HHI;
- coberturas de monto, ganador y participantes;
- tendencia reciente, confianza y recomendación.

El ordenamiento se realiza sobre todas las fichas filtradas y después se pagina.
Por eso el primer registro de una página ordenada por monto es realmente el
máximo global, no el máximo de una muestra visible.

## Score de oportunidad

El score está compuesto únicamente por cinco dimensiones configurables:

- demanda: 28 %;
- economía: 27 %;
- competencia: 18 %;
- viabilidad comercial: 17 %;
- complejidad favorable: 10 %.

La confianza se presenta por separado y considera cobertura de montos,
ganadores, participantes y calidad de detección. También existen perfiles de
peso para volumen, baja competencia, búsqueda de proveedor y baja complejidad.

## Vistas guardadas y estudios profundos

Las vistas guardadas persisten por usuario en la hoja
`intel_v3_saved_views`. Incluyen periodo, filtros, disponibilidad, perfil de
score, pesos y criterios finales.

El estudio profundo recibe el mismo contexto de la tabla maestra: periodo,
dimensión temporal, confianza, estados, entidades, rangos monetarios y búsqueda.
Las solicitudes se encolan en `pc_manual` y el worker mantiene compatibilidad
con bases históricas que todavía no tengan todas las columnas nuevas.

## Actualización y publicación

Para actualizar la fuente, reconstruir la capa analítica y publicarla de forma
atómica en Supabase:

```powershell
$env:SUPABASE_DB_URL = "DSN_VALIDO_DE_SUPABASE"
& "C:\Users\rodri\scrapers_repo\db\actualizar_base_corregida.bat"
```

El DSN debe permanecer en variables de entorno o Secrets; nunca debe guardarse
en el repositorio. La publicación usa tablas `__new`, valida la carga y luego
hace un intercambio transaccional. Una ejecución incompleta no sustituye las
tablas vigentes.

También puede reconstruirse únicamente la capa local:

```powershell
python "C:\Users\rodri\scrapers_repo\db\build_intelligence_tables.py"
```

## Controles ejecutados el 22 de julio de 2026

- Fuente operacional: 211,105 actos.
- Hechos acto-ficha: 95,624.
- Proponentes: 71,441.
- Métricas mensuales: 349,056.
- Metadata de fichas: 16,346.
- Filas de catálogo: 23,283.
- Duplicados `(acto, ficha)`: 0.
- Fichas únicas incoherentes: 0.
- Scores fuera del rango 0-100: 0.
- Proponentes huérfanos: 0.
- Control ficha 43358 en 2026, perfil moderado: 16 actos en la fuente y 16 en
  la capa analítica; 100 % con ficha única en ese periodo.

Rendimiento local observado:

- resumen 2026: 3,632 fichas en 1.32 s SQL;
- resumen histórico: 5,477 fichas en 3.94 s SQL;
- búsqueda por frase: 0.98 s;
- filtros agregados exigentes: 1.47 s;
- score: 0.12-0.51 s;
- ordenamiento global y paginación: 0.002-0.027 s.

La página completa se probó con `streamlit.testing`: cero excepciones, cero
errores, seis tablas, nueve indicadores y ocho pestañas.

## Mantenimiento

- Cambiar pesos: `DEFAULT_SCORE_WEIGHTS` y `SCORE_PRESETS` en el servicio V3.
- Cambiar perfiles de confianza: `PROFILE_THRESHOLDS`.
- Cambiar límites de interfaz: controles en la página unificada.
- Añadir métricas: primero al constructor, después al SQL del repositorio y por
  último a la tabla/explicación de score.
- Después de modificar el detector o catálogo, reconstruir y publicar la capa
  analítica para mantener consistencia de versión.

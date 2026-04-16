# Outreach (Español)

## Destinatario: hiring manager / Head of Data / responsable de Compliance en Securitize

### Versión A — LinkedIn / email (corto)

Asunto: Primera versión de una capa de auditoría y calidad de datos para un fondo tokenizado

Hola {{nombre}},

Vi la vacante de Data & AI Generalist y me tomé el brief al pie de la letra:
construí una primera versión de la capa de calidad de datos y trazabilidad
de auditoría que asumiría en los primeros 30 días del rol.

Es un prototipo funcional sobre un fondo sintético de private credit tokenizado
(estilo Apollo): 11 reglas que cubren reconciliación cap table ↔ on-chain,
control KYC, whitelisting, concentración, completitud y plausibilidad de NAV,
más una capa ligera de detección de anomalías sobre transferencias. Las reglas
están escritas dos veces — motor en Python y SQL portable a warehouse —
porque el rol alterna entre ambos.

Hallazgos principales de una ejecución sobre datos sintéticos con defectos plantados:

- 3 inversores con drift entre cap table y on-chain (el mayor ~$180k)
- 2 transferencias a una wallet con KYC vencido que el contrato no rechazó
- 1 transferencia secundaria a una wallet fuera del whitelist
- 1 breach de concentración al 26% de los tokens emitidos

El repo, el brief ejecutivo y el dashboard están aquí: {{link}}.

¿Podemos hablar 20 minutos la próxima semana para discutir cómo adaptaría
esto a vuestro stack real y qué construiría en las primeras 4 semanas?

Un saludo,
Etiosa

---

### Versión B — email extendido

Asunto: Prototipo de calidad de datos y auditoría para APCRED — ¿20 min?

Hola {{nombre}},

En vez de enviar un CV, construí una primera versión funcional del sistema
de calidad de datos y trazabilidad de auditoría que creo que realmente define
el rol de Data & AI Generalist. Lo monté sobre un fondo sintético de private
credit tokenizado al estilo Apollo porque es el equivalente público más cercano
sin acceso a vuestro warehouse real.

Qué incluye:

1. Datos sintéticos pero realistas para 5 tablas: transferencias on-chain, cap
   table del transfer agent, NAV diario, suscripciones / reembolsos, eventos KYC.
   Los defectos están plantados a propósito (ajuste off-chain del TA, mint a
   KYC vencido, destinatario fuera de whitelist, hueco de NAV, tx_hash duplicado,
   breach de concentración) para que la capa de validación tenga trabajo real.
2. Motor de validación de 11 reglas en Python, clasificado por severidad
   (critical / high / medium) con asignación de owner y texto de remediación.
   Mismas reglas expresadas en SQL portable a warehouse para un deployment
   BigQuery / dbt.
3. Dashboard en Streamlit con tres capas: resumen ejecutivo (data health score,
   cantidad de críticos, tendencia NAV), diagnóstico (estado por regla, hallazgos
   por owner, vista de reconciliación, anomalías) y acción (qué resolver, quién
   lo gestiona, cuándo).
4. Detector ligero de anomalías (z-score + isolation forest) sobre features de
   transferencia, con razón en lenguaje natural para cada flag.
5. Documentación: contexto de negocio, lineage (raw → staging → marts) y brief
   ejecutivo para Compliance + Finance.

Por qué lo armé así:

El rol no es un puesto puro de analista ni de ingeniero — es el asiento de
operador de los datos que sostienen cada obligación de transfer agent, fund
admin y ATS de Securitize. Escribir las queries es lo fácil. Lo difícil es
hacer que los hallazgos sean *accionables* (severidad, owner, remediación)
y *auditables* (cada hallazgo se remonta a una regla, cada regla a una fuente).
En eso optimicé.

Qué haría en los primeros 30 días sobre el stack real:

- Portar estas reglas a vuestro warehouse y ejecutarlas tras la ingesta nocturna
- Conectar la ruta de hallazgos (PagerDuty para críticos, tickets para alta
  severidad, digest para media) y un export mensual para auditoría
- Sentarme con Compliance y Finance para capturar las reglas que no pensé —
  serán varias, porque cada fondo tiene su waterfall y cada clase de activo
  tiene sus peculiaridades
- Empezar la capa de AI (detección de anomalías + chatbot interno sobre
  hallazgos + lineage) sobre datos ya confiables

Repo, brief ejecutivo, dashboard: {{link}}

Me encantaría recorrerlo contigo — ¿te viene bien una llamada de 20 minutos
la próxima semana?

Un saludo,
Etiosa

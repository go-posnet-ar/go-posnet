# Fraud Detection — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/fraud-detection/README.md) o de los
> comentarios del código, vale lo que está acá.

## 1. Resumen

Fraud Detection evalúa el riesgo de cada transacción: recibe un pedido de
[Authorization](AUTHORIZATION.md), aplica un conjunto de reglas, calcula un score de 0 a 100 con una
decisión y devuelve el resultado. No llama al adquirente: la Saga continúa en Authorization.

El motor funciona, pero tiene tres limitaciones que conviene conocer antes de confiar en él:

- **Los umbrales no son configurables**, aunque la API y las variables de entorno sugieran lo contrario.
- **Ante fallas de infraestructura, deja pasar las transacciones.** Si falla el motor, queda el caso
  con `REVIEW` y una métrica; si falla el historial, no queda rastro.
- **El flujo QR no puede llegar a `REJECT`** con los pesos por defecto (ver [§4.5](#45-alcance-real-de-reject)).

| Capacidad | Estado | Detalle |
|---|---|---|
| Evaluación de 5 reglas y cálculo del score | Implementado | Lógica en memoria, tras cargar el historial |
| Registro de casos (`FraudCase`) | Implementado | También los scores neutrales y los pedidos inválidos |
| Respuesta garantizada a Authorization | Implementado | Outbox en la transacción del caso; ver [§9](#9-eventos-nats) |
| Consulta de casos y reglas | Implementado | HTTP, sin autenticación |
| Cambio del peso de una regla sin redespliegue | Implementado | `PUT /rules/{id}/threshold` |
| Cambio del umbral de una regla | **No tiene efecto** | Se guarda, pero las reglas no lo leen |
| Umbrales de decisión por variable de entorno | **No tiene efecto** | Fijos en 50 y 70 en el código |
| Cola de revisión para `REVIEW` | **Pendiente** | Solo queda registrado en el caso |
| Servicio gRPC | **Placeholder** | El servidor arranca sin servicios registrados |

Las consecuencias de estos pendientes están en [§12](#12-brechas-conocidas).

## 2. Contexto y límites

```
                  fraud.check-requested
 Authorization ───────────────────────────►┌─────────────────┐
               ◄───────────────────────────│ Fraud Detection │
                  fraud.score-calculated   └───┬─────────┬───┘
                                               │         │
                  schema fraud_detection  ◄────┘         └────► pn_authorization.transactions
                  (casos y reglas)                              (solo lectura)
```

- **Es dueño de:** los casos de fraude, el catálogo de reglas y la decisión.
- **No decide:** el resultado final del pago. Authorization rechaza ante `REJECT` y continúa ante
  `APPROVE` o `REVIEW`, según el campo `decision`.
- **Dependencia con Authorization:** lee `pn_authorization.transactions` directamente, por lo que ambos
  servicios deben usar la misma base de datos. Es una excepción a la regla de la
  [documentación de arquitectura](../architecture/README.md), según la cual ningún contexto comparte
  estado por referencia.

## 3. Estructura del código

| Ruta | Contenido |
|---|---|
| [`cmd/fraud-detection/fd_main.go`](../../cmd/fraud-detection/fd_main.go) | Arranque y apagado |
| [`cmd/fraud-detection/fd_wire.go`](../../cmd/fraud-detection/fd_wire.go) | Wiring de dependencias |
| [`domain/aggregate/`](../../context/fraud-detection/domain/aggregate/) | `FraudCase`, score neutral, pedidos inválidos y sin reglas |
| [`domain/entity/`](../../context/fraud-detection/domain/entity/) | `FraudRule` |
| [`domain/service/`](../../context/fraud-detection/domain/service/) | `RuleEngine`: el motor y las reglas |
| [`domain/valueobject/`](../../context/fraud-detection/domain/valueobject/) | `FraudScore`, `FraudDecision`, `RuleEvaluation` |
| [`domain/repository/`](../../context/fraud-detection/domain/repository/) | Puertos de casos, reglas e historial |
| [`application/command/`](../../context/fraud-detection/application/command/) | Evaluación, administración y métricas |
| [`application/query/`](../../context/fraud-detection/application/query/) | Consulta de casos y reglas |
| [`infrastructure/nats/`](../../context/fraud-detection/infrastructure/nats/) | Publisher (al outbox) y subscriber |
| [`infrastructure/postgres/`](../../context/fraud-detection/infrastructure/postgres/) | Repositorios y caché de reglas |
| [`infrastructure/http/`](../../context/fraud-detection/infrastructure/http/) | Health checks, consulta y administración |
| [`infrastructure/grpc/server/`](../../context/fraud-detection/infrastructure/grpc/server/) | Servidor sin servicios |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/fraud-detection/`.
Las reglas están en
[`fd_rule_engine.go`](../../context/fraud-detection/domain/service/fd_rule_engine.go).

## 4. Modelo de dominio

### 4.1 `FraudCase`

Representa el análisis de una transacción. Se evalúa una sola vez y no tiene máquina de estados.

| Campo | Descripción |
|---|---|
| `id` | UUID propio del caso |
| `transactionID` | Correlación con la transacción; único en la tabla |
| `terminalID`, `merchantID` | Terminal y comercio |
| `amountCents`, `currency` | Monto, que debe ser positivo, y moneda de hasta 3 caracteres |
| `cardNetwork`, `entryMode` | Datos de la tarjeta; hasta 20 caracteres, sin validar contra un catálogo |
| `occurredAt` | Fecha de la transacción; si no se puede interpretar, se usa la actual |
| `evaluations` | Resultado de cada regla |
| `score` | Score, decisión y reglas activadas |
| `evaluatedAt` | Fecha de la evaluación |

### 4.2 Reglas

Cada regla suma su peso al score si se activa. Los pesos se leen de la tabla `fraud_rules`, pero las
condiciones están **fijas en el código**:

| ID | Nombre | Peso | Condición | Ventana |
|---|---|---|---|---|
| `RULE-001` | Velocity Check | 20 | Más de 60 transacciones en el terminal | 1 hora |
| `RULE-002` | Unusual Amount | 15 | Monto mayor a 3 veces el promedio aprobado del comercio | 30 días |
| `RULE-003` | Multiple Rejections | 25 | Más de 3 rechazos en el terminal | 10 min |
| `RULE-004` | Repeated Amount | 20 | Mismo monto exacto más de una vez en el terminal | 5 min |
| `RULE-005` | High Amount Magstripe | 30 | `MAGSTRIPE` con más de 5.000.000 de centavos | — |

Detalles que afectan el resultado:

- **La transacción actual cuenta:** Authorization la guarda antes de pedir la evaluación, así que
  `RULE-001` y `RULE-004` la incluyen. `RULE-004` se activa desde el segundo intento.
- **`RULE-002` no se activa sin historial:** si el comercio no tiene aprobaciones en 30 días, el
  promedio es 0 y la regla no aplica.
- **Las reglas no distinguen moneda:** `RULE-002` promedia todos los montos del comercio, y `RULE-005`
  compara con un límite pensado para ARS.
- **Las reglas no ven la tarjeta:** el evento de entrada no incluye `card_last4` ni `card_token`, así
  que no puede haber reglas por tarjeta.

Una fila nueva en `fraud_rules` no agrega una regla: sin una función registrada para ese ID, no se
activa. Agregar una regla requiere cambiar el código.

### 4.3 Decisión

El score es la suma de los pesos activados, con un máximo de 100. Los límites están fijos en
[`fd_fraud_decision.go`](../../context/fraud-detection/domain/valueobject/fd_fraud_decision.go):

| Score | Decisión | Efecto en Authorization |
|---|---|---|
| 0–49 | `APPROVE` | Continúa hacia el adquirente |
| 50–69 | `REVIEW` | Continúa hacia el adquirente; no hay revisión posterior |
| 70–100 | `REJECT` | Rechaza con `FRAUD_REJECTED` |

### 4.4 Scores que no salen de las reglas

Tres casos tienen un score fijo. Los tres se guardan como cualquier otro caso, con una evaluación
sintética que explica el motivo en `GET /fraud-cases/{id}`, y se responden a Authorization:

| Caso | Evaluación | Score | Decisión | Métrica |
|---|---|---|---|---|
| El motor falla: no se pueden cargar las reglas | `BYPASS`, activada | 50 | `REVIEW` | `unevaluated_total{reason="engine_failure"}` |
| El pedido no se puede evaluar: terminal o comercio que no son UUID, monto no positivo, o un campo más largo que su columna | `INVALID_EVENT`, activada, con los campos que fallaron | 100 | `REJECT` | `unevaluated_total{reason="invalid_event"}` |
| No hay reglas activas | `NO_ACTIVE_RULES`, no activada | 0 | `APPROVE` | `posnet_fraud_active_rules` en 0 |

- **Motor caído, *fail-open*:** se prioriza que el pago continúe por encima del control de fraude.
- **Pedido inválido, *fail-closed*:** Authorization lo rechaza al instante con `FRAUD_REJECTED`. Sin
  respuesta, esperaría su timeout y rechazaría con `FRAUD_TIMEOUT`, que invita a reintentar algo que
  no se va a arreglar. El caso guarda solo los campos válidos: un terminal o comercio ilegible queda
  como UUID nulo.
- **Sin reglas, control apagado:** desactivar todas las reglas apaga el motor, en lugar de mandar
  todo a `REVIEW`. Queda un `WARN` al arrancar y en cada recarga del caché, y la alerta
  `FraudSinReglasActivas`.
- **`transaction_id` ilegible en el payload:** se usa el `correlation_id` del envelope, que
  Authorization llena con el mismo ID, y el pedido se rechaza como `INVALID_EVENT`. Solo si tampoco
  ese es un UUID no hay a quién responder: se descarta con `Term`, un log y
  `unevaluated_total{reason="unanswerable"}`. Authorization no genera esos eventos.

### 4.5 Alcance real de `REJECT`

Con los pesos por defecto, ningún par de reglas llega a 70:

| Tipo de transacción | Combinaciones que llegan a `REJECT` |
|---|---|
| Con `MAGSTRIPE` y monto alto | `RULE-005` más otras dos (salvo `RULE-001` + `RULE-002`, o `RULE-002` + `RULE-004`, que suman 65), o las cuatro restantes |
| Sin `MAGSTRIPE` | Únicamente las cuatro reglas `RULE-001` a `RULE-004` juntas |

**Esto hace inalcanzable `REJECT` en el flujo QR actual:**

1. El simulador de Terminal Gateway solo crea sesiones QR, que se publican como `CONTACTLESS`, así que
   `RULE-005` nunca aplica.
2. Sin `MAGSTRIPE`, `REJECT` exige `RULE-003`, que necesita más de 3 rechazos previos en el terminal.
3. Hoy nada más genera esos rechazos: el adquirente mock aprueba todo y la blocklist no se activa.

El máximo posible en ese flujo es 55 (`REVIEW`). `REJECT` solo se alcanza publicando directamente a
NATS con `MAGSTRIPE`, cargando rechazos a mano o subiendo los pesos.

## 5. Flujo de evaluación

```
Authorization        Fraud Detection                     Postgres
     │ check-requested     │                                 │
     ├────────────────────►│ lee el transaction_id           │
     │                     │ TX: marca evento procesado ────►│
     │                     │ ¿la transacción ya tiene caso? ►│
     │                     │ valida el resto del pedido      │
     │                     │ crea FraudCase                  │
     │                     │ ── inicio de EvalTimeout ──     │
     │                     │ carga reglas (caché)            │
     │                     │ 4 consultas de historial ──────►│
     │                     │ evalúa reglas en goroutines     │
     │                     │ ── fin de EvalTimeout ──        │
     │                     │ guarda FraudCase y encola       │
     │                     │ el score en el outbox; COMMIT ─►│
     │                     │ relay (cada 200 ms) ◄───────────│
     │ score-calculated    │                                 │
     │◄────────────────────┤                                 │
```

El presupuesto de tiempo (`ENGINE_EVAL_TIMEOUT`, 200 ms por defecto) cubre la carga de reglas y el
historial. Las reglas en sí no hacen I/O: la evaluación en goroutines es prácticamente instantánea. El
relay del outbox agrega hasta 200 ms más.

Todo pasa en una transacción que empieza marcando el evento en `processed_events`:

- **Reentrega de un evento ya procesado:** la marca no inserta y se descarta sin evaluar.
- **Dos entregas simultáneas del mismo evento:** la segunda espera en la clave única hasta que la
  primera confirma, y entonces se descarta. Si la primera falla, la segunda evalúa.
- **Otro pedido de una transacción que ya tiene caso:** se responde con el score guardado, sin
  evaluar. Si dos pedidos de la misma transacción evalúan a la vez, el que pierde el `INSERT` responde
  con el caso del otro: el score publicado siempre coincide con el guardado.

La evaluación corre dentro de la transacción y retiene una conexión hasta `ENGINE_EVAL_TIMEOUT`. El
subscriber procesa un mensaje a la vez, así que es una por instancia, más las 4 del historial.

### Qué pasa cuando falla cada paso

| Fallo | Resultado | Decisión | ¿Queda registro? |
|---|---|---|---|
| `transaction_id` ilegible en el payload | Se usa el `correlation_id` del envelope y se rechaza | `REJECT` | Caso, log y métrica |
| Ni el payload ni el envelope traen un `transaction_id` | `Term`; no hay a quién responder | — | Log y métrica |
| Terminal o comercio inválidos, monto no positivo o campo demasiado largo | Se responde un rechazo | `REJECT` | Caso, log y métrica |
| No se pueden cargar las reglas | Score neutral | `REVIEW` | Caso, log y métrica |
| No hay reglas activas | Control apagado | `APPROVE` | Caso, `WARN` y gauge |
| La transacción ya tiene caso | Se responde con el score guardado | La guardada | `WARN` |
| Fallan **todas** las consultas de historial | Continúa con historial en cero | Solo por `RULE-005` | Ningún log |
| Fallan **algunas** consultas de historial | Esas reglas no se activan | Score subestimado | Ningún log |
| Se agota `EvalTimeout` durante el historial | Igual que los dos casos anteriores | Score subestimado | Solo `posnet_fraud_engine_timeouts_total` |
| Error al guardar el caso o al encolar el score | Se deshace todo, también la marca; `Nak` y reintento | Se recalcula | Log |
| NATS caído al publicar | El score espera en el outbox y sale cuando vuelve. Si tarda más que `FRAUD_CHECK_TIMEOUT`, Authorization ya rechazó y lo ignora | — | Caso, `posnet_fraud_outbox_stuck` |
| Caída después de confirmar | El relay publica el score al arrancar; la reentrega se descarta como duplicado | — | Caso guardado |

**El caso más importante es el historial:** una base de datos lenta o caída no produce un error
visible. Las reglas de historial simplemente no se activan y la transacción figura como un `APPROVE`
normal, con score 0.

## 6. Reglas en caliente

`PUT /rules/{rule_id}/threshold` actualiza `score_weight` y `threshold_value`:

- **Peso:** tiene efecto de inmediato en la instancia que recibió el pedido, porque invalida su caché.
  Las demás instancias lo toman al vencer su caché (`ENGINE_RULES_CACHE_TTL`, 5 minutos por defecto).
- **Umbral:** se guarda y se muestra en `GET /rules`, pero **ninguna regla lo lee**. Cambiarlo no altera
  el comportamiento.

Un cambio directo en la tabla, como desactivar una regla con `is_active = FALSE`, también se aplica al
vencer la caché. No hay endpoint para activar o desactivar reglas.

## 7. API HTTP

Puerto `HTTP_PORT`: 8082 por defecto en el código y **8083 en Docker Compose**. **No hay autenticación**,
ni siquiera para modificar reglas.

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; solo verifica Postgres | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `GET /fraud-cases/{transaction_id}` | Análisis de una transacción | `200` | `400`, `404`, `500` |
| `GET /rules` | Reglas activas | `200` | `500` |
| `PUT /rules/{rule_id}/threshold` | Modificar una regla | `200` | `400`, `404`, `500` |

Los errores tienen la forma `{"error": "CODIGO", "message": "..."}`.

### Consultar un caso

Responde con nombres en PascalCase, porque los structs no tienen tags JSON. `RuleName` siempre llega
vacío, porque ese dato no se guarda.

```json
{
  "FraudCaseID": "0c1d5a3e-6f0b-4f52-9a1c-3f3e2b7c9d11",
  "TransactionID": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "Score": 20,
  "Decision": "APPROVE",
  "RulesHit": ["RULE-004"],
  "EvaluatedAt": "2026-11-05T15:00:04Z",
  "Evaluations": [
    {
      "RuleID": "RULE-004",
      "RuleName": "",
      "Activated": true,
      "ScoreContribution": 20,
      "Reason": "amount 150000 attempted 2 times in last 5 min"
    }
  ]
}
```

`Evaluations` incluye todas las reglas evaluadas, también las que no se activaron; el ejemplo muestra
solo una. Un `404` no significa que la transacción no exista: puede haberse evaluado con score neutral.

### Listar reglas

```json
{
  "rules": [
    {
      "ID": "RULE-005",
      "Name": "High Amount Magstripe",
      "Description": "Magstripe con monto alto mayor ARS 50000",
      "ScoreWeight": 30,
      "ThresholdValue": 5000000,
      "IsActive": true
    }
  ],
  "count": 1
}
```

### Modificar una regla

```json
{ "new_threshold": 5000000, "new_score_weight": 40 }
```

`new_score_weight` es obligatorio y debe estar entre 1 y 100, así que no se puede anular una regla
poniendo su peso en 0. Solo se pueden modificar reglas activas. Respuesta: `{"status": "updated"}`.

## 8. gRPC

Hay un servidor en el puerto 9092 sin servicios registrados, y el contexto no tiene un `.proto`. Es un
placeholder: cualquier llamada devuelve `Unimplemented`. El puerto está fijo y no respeta `GRPC_PORT`.

## 9. Eventos NATS

Todos los eventos usan el envelope de [`pkg/events/envelope.go`](../../pkg/events/envelope.go), con el
`TransactionID` como `correlation_id`.

### Consumido

`posnet.fraud.check-requested.v1`, en el stream `POSNET_FRAUD`:

| Parámetro | Valor |
|---|---|
| Durable y queue group | `fraud-check-consumer` |
| Entregas máximas | 5 |
| `AckWait` | 30 s |
| Política de entrega | `DeliverNew` |

| Campo | Notas |
|---|---|
| `transaction_id`, `terminal_id`, `merchant_id` | UUID |
| `amount_cents`, `currency` | Monto y moneda |
| `card_network`, `entry_mode` | `entry_mode` activa `RULE-005` |
| `occurred_at` | RFC3339 UTC |

### Publicado

`posnet.fraud.score-calculated.v1`, en `POSNET_FRAUD`, consumido por Authorization. Se escribe en
`fraud_detection.outbox` en la misma transacción que guarda el caso, y el relay de
[`pkg/outbox`](../../pkg/outbox/outbox.go) lo publica cada 200 ms, en lotes de hasta 100, con el
`event_id` como `Nats-Msg-Id`: si lo publica dos veces, JetStream descarta la segunda. Varias
instancias pueden correr el relay a la vez (`FOR UPDATE SKIP LOCKED`).

| Campo | Notas |
|---|---|
| `transaction_id` | Correlación |
| `score` | 0–100 |
| `decision` | `APPROVE`, `REVIEW` o `REJECT` |
| `rules_hit` | IDs de las reglas activadas, `BYPASS` o `INVALID_EVENT` |
| `evaluated_at` | RFC3339 UTC; momento de la evaluación, no de la publicación |

### Tratamiento de errores

| Situación | Acción | Efecto |
|---|---|---|
| Envelope o payload inválido | `Term` | No se reintenta |
| `ValidationError` o `ConflictError` | `Term` | No se reintenta. Hoy solo un pedido sin `transaction_id` legible |
| Cualquier otro error | `Nak` | Reentrega inmediata, sin backoff |

Un mensaje que agota las entregas no pasa a una DLQ. Para recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md).

## 10. Persistencia

Schema `fraud_detection`. **Una base nueva sale de
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql):** `pgutil.Migrate` no
hace nada y [`fd_schema.sql`](../../context/fraud-detection/infrastructure/postgres/fd_schema.sql) no
se ejecuta. A una base existente hay que aplicarle a mano las migraciones de
[`migrations/fraud-detection/`](../../migrations/fraud-detection/):

| Migración | Contenido | Sin ella |
|---|---|---|
| `000001_outbox` | Tabla `outbox` e índice por `created_at` | El servicio no arranca: lo verifica antes de consumir |

```bash
psql "$POSTGRES_DSN" -f migrations/fraud-detection/000001_outbox.up.sql
```

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `fraud_cases` | Casos evaluados | PK `id`; `transaction_id` único |
| `fraud_rules` | Catálogo de reglas | PK `id`; cargado con las 5 reglas |
| `processed_events` | Idempotencia | PK `event_id` |
| `outbox` | Scores por publicar | PK `id`; índice por `created_at` |

Diferencia entre ambos archivos: `fd_schema.sql` define dos índices sobre `fraud_cases`
(`idx_fd_cases_terminal` e `idx_fd_cases_decision`) que `01-init.sql` no crea.

### Consultas sobre Authorization

Las cuatro consultas de historial se ejecutan en paralelo por cada transacción sobre
`pn_authorization.transactions`, y usan `terminal_id`, `merchant_id`, `amount_cents`, `state` y
`created_at`. En `01-init.sql`, esa tabla no tiene índices para estas columnas, de modo que **el costo
crece con el volumen de transacciones**. Superado el presupuesto de 200 ms, las reglas de historial
dejan de activarse sin ningún error.

### Garantías actuales

- **Atomicidad:** la marca en `processed_events`, el caso y el score en el outbox se confirman o se
  deshacen juntos, en una transacción `READ COMMITTED`.
- **Idempotencia:** cada evento se evalúa una vez, aunque llegue repetido o en paralelo. Cada
  transacción tiene un solo caso, y todo score publicado para ella es el de ese caso.
- **Respuesta:** todo pedido con un `transaction_id` legible, en el payload o en el envelope, recibe un
  score, *at-least-once*. Si no llega antes de `FRAUD_CHECK_TIMEOUT`, Authorization rechaza con
  `FRAUD_TIMEOUT` e ignora el score tardío.

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene HTTP (hasta 10 s), drena NATS, que espera a los handlers en curso,
detiene el relay y cierra el pool de Postgres. Mientras dura el drenaje, el relay sigue publicando; lo
que no llegue a salir queda en el outbox para el próximo arranque. El servidor gRPC no se detiene de
forma ordenada.

## 11. Observabilidad y operación

### Métricas

| Métrica | Tipo | Labels | Significado |
|---|---|---|---|
| `posnet_fraud_evaluations_total` | Counter | `decision` | Scores encolados, también los fijos de [§4.4](#44-scores-que-no-salen-de-las-reglas); no cuenta reentregas |
| `posnet_fraud_unevaluated_total` | Counter | `reason` | Scores que no salieron de las reglas: `engine_failure`, `invalid_event` o `unanswerable` |
| `posnet_fraud_outbox_stuck` | Gauge | — | Scores en el outbox desde hace más de un minuto |
| `posnet_fraud_active_rules` | Gauge | — | Reglas activas; en 0, todo se aprueba |
| `posnet_fraud_score_histogram` | Histogram | — | Distribución de scores |
| `posnet_fraud_engine_duration_seconds` | Histogram | — | Duración del motor |
| `posnet_fraud_rule_hits_total` | Counter | `rule` | Activaciones por regla |
| `posnet_fraud_engine_timeouts_total` | Counter | — | Evaluaciones que agotaron `EvalTimeout` |
| `posnet_fraud_rules_cache_hits_total` | Counter | — | Lecturas de reglas desde la caché |
| `posnet_fraud_rules_cache_misses_total` | Counter | — | Lecturas de reglas desde Postgres |

Los scores neutrales se ven en `posnet_fraud_unevaluated_total`, pero las fallas de historial no se
distinguen: **una tasa alta de `APPROVE` puede ocultar un historial degradado**. El indicador disponible
es `posnet_fraud_engine_timeouts_total`, y solo cubre los timeouts.

### Alertas y dashboards

Reglas en [`warning.yml`](../../deployments/docker/prometheus/rules/warning.yml):

| Alerta | Severidad | Condición |
|---|---|---|
| `EvalTimeoutsFraud` | Warning | Más de 0,01 timeouts/s durante 5 min |
| `CacheRulesLow` | Warning | Hit ratio de la caché por debajo del 90 % durante 10 min |
| `OutboxFraudTrabado` | Warning | Scores en el outbox hace más de un minuto, durante 5 min |
| `FraudSinReglasActivas` | Warning | Ninguna regla activa durante 5 min |
| `FraudSinEvaluar` | Warning | Algún score que no salió de las reglas en 5 min, por `reason` |

`CacheRulesLow` también puede activarse con poco tráfico, porque con menos de 10 lecturas por período
de caché el hit ratio no llega al 90 %.

**Dashboard:** `posnet-bc-detail` muestra la duración del motor. **Runbook:**
[incident-response](../runbooks/incident-response.md).

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; debe alcanzar también `pn_authorization` |
| `NATS_URL` | Obligatoria | Sí |
| `HTTP_PORT` | `8082` | Sí; Docker Compose usa `8083` |
| `ENGINE_EVAL_TIMEOUT` | `200ms` | Sí; con más de `400ms`, el servicio no arranca |
| `ENGINE_RULES_CACHE_TTL` | `5m` | Sí |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `15`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-fraud-detection` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `ENGINE_SCORE_THRESHOLD_REJECT` | `70` | **No**: solo se valida y se registra en el log |
| `ENGINE_SCORE_THRESHOLD_REVIEW` | `50` | **No**: solo se valida y se registra en el log |
| `GRPC_PORT` | `9092` | **No**: el puerto está fijo en 9092 |
| `MIGRATIONS_DIR` | `migrations/fraud-detection` | **No**: `Migrate` no hace nada; las migraciones se aplican a mano |

Si `REVIEW` es mayor o igual que `REJECT`, el servicio no arranca, aunque esos valores no se usen.

### Levantar y probar

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build \
  terminal-gateway authorization fraud-detection
```

Sin Docker, con Postgres y NATS disponibles:

```bash
POSTGRES_DSN='postgres://...' NATS_URL='nats://localhost:4222' go run ./cmd/fraud-detection
```

El comando `go run ./context/fraud-detection/...` del README no es válido: el entrypoint está en `cmd/`.
Los templates de Helm de este servicio están vacíos.

Para ver un caso, crear y pagar una sesión como se indica en
[Terminal Gateway](TERMINAL-GATEWAY.md#probar-un-pago), y luego consultar con el mismo ID:

```bash
curl -s localhost:8083/fraud-cases/$ID | jq
curl -s localhost:8083/rules | jq
```

### Tests

```bash
go test ./context/fraud-detection/...

# Carga: requiere el servicio y NATS activos
locust -f posnet-load-tests/fraud-detection/fraud_detection_locust.py \
  --headless -u 10 -r 2 --run-time 2m --host http://localhost:8083
```

Los tests no cubren `config` ni el wiring, y no hay tests de integración contra Postgres y NATS reales.
Para la carga, ver también [`LOCUST.md`](../../LOCUST.md).

## 12. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Efectividad del control de fraude

| Brecha | Consecuencia | Dónde |
|---|---|---|
| `REJECT` es inalcanzable en el flujo QR | Con los pesos por defecto, el fraude no puede rechazar ninguna transacción de ese flujo (ver [§4.5](#45-alcance-real-de-reject)) | `fraud_rules`, `RULE-003` |
| Las fallas de historial se ocultan | Una base lenta o caída produce `APPROVE` con score 0, sin log ni error. Solo los timeouts dejan una métrica | `RuleEngine.Evaluate`, `buildRuleContext` |
| Sin índices para el historial | A medida que crece la tabla, las consultas pueden superar los 200 ms y activar la falla anterior | `01-init.sql` |
| `threshold_value` no se usa | `PUT /rules/{id}/threshold` responde `updated` y `GET /rules` muestra el valor nuevo, pero las condiciones siguen fijas | `registerEvalFns` |
| Los umbrales de decisión no se usan | `ENGINE_SCORE_THRESHOLD_*` se muestran en el log de arranque, pero `NewFraudScore` usa 50 y 70 fijos | `NewFraudScore`, `fd_main.go` |
| `REVIEW` no genera ninguna revisión | La transacción continúa y solo queda registrada; no hay cola ni proceso | Authorization, `fraud_cases` |
| Las reglas no distinguen moneda | `RULE-002` mezcla monedas en el promedio y `RULE-005` usa un límite en ARS para cualquier moneda | `fd_postgres_repository.go`, `registerEvalFns` |
| Sin reglas por tarjeta | El evento no incluye datos de la tarjeta, así que no se puede detectar el uso repetido de una misma tarjeta | `FraudCheckRequestedPayload` |

### Confiabilidad de la Saga

No quedan brechas abiertas. Las garantías están en [§10](#garantías-actuales); el único pedido sin
respuesta es uno sin `transaction_id` legible ni en el payload ni en el envelope, que Authorization no
genera.

### Seguridad y administración

| Brecha | Consecuencia | Dónde |
|---|---|---|
| La API de reglas no tiene autenticación | Con acceso de red, cualquiera puede bajar todos los pesos a 1 y anular el control de fraude. Tampoco queda registro de quién hizo el cambio | `fd_http_router.go`, `fd_admin_handler.go` |
| El cambio de reglas modifica la caché antes de guardar | La regla es un puntero compartido con evaluaciones en curso: es una condición de carrera. Si `Save` falla, la instancia queda con un peso que la base no tiene | `UpdateRuleThreshold` |
| Sin endpoint para activar o desactivar reglas | `Deactivate()` existe, pero no se invoca: hay que cambiar la tabla a mano | `fd_fraud_rule.go` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Acceso directo a tablas de Authorization | Un cambio de columnas allí rompe las reglas, sin error de compilación. El fallo además queda oculto por la brecha de historial | `TransactionHistoryRepo` |
| `rule_name` no se guarda | `RuleName` llega vacío en `GET /fraud-cases` | `FraudCaseRepo.Save` |
| Sin migraciones reales e índices faltantes | Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `fd_schema.sql` difieren | `pkg/pgutil/migrate.go` |
| Sin dashboards de decisión ni alertas de negocio | Hay métricas de decisiones y reglas activadas, pero ningún dashboard las muestra ni hay alertas sobre ellas | Grafana, Prometheus |
| gRPC placeholder y sin apagado ordenado | Abre un puerto que no ofrece nada y que ignora `GRPC_PORT` | `fd_grpc_server.go`, `fd_wire.go` |
| `/readyz` no verifica NATS | Una instancia sin NATS sigue figurando como lista | `fd_http_handler.go` |
| Referencias a un timeout de 500 ms | La validación de `ENGINE_EVAL_TIMEOUT` y varios comentarios lo mencionan, pero Authorization no tiene ese timeout | `fd_config.go` |
| Código sin uso | `port.FraudService` y `port.AdminService` no se implementan; tampoco se usan `parseTerminalID` ni los eventos de dominio internos | `fd_port_inbound.go`, `fd_rule_engine.go`, `fd_fraud_case.go` |

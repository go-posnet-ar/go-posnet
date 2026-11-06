# Runbook: Respuesta ante incidentes

## Antes de empezar — qué existe hoy

Este runbook describe el estado **actual** del sistema, no el previsto. Revisado contra el código el
2026-11-05.

| Herramienta | Dónde (Docker Compose) | Estado |
|---|---|---|
| Health checks | `/healthz` y `/readyz` de cada servicio | Funcionan; `readyz` verifica Postgres; en Authorization, también el schema; en Settlement, también las tablas requeridas y NATS |
| Logs JSON | `docker logs posnet-<servicio>` | Funcionan; primera herramienta para un caso puntual |
| Métricas | Prometheus en `localhost:9099`, `/metrics` de cada servicio | Funcionan |
| Dashboards | Grafana en `localhost:3000` | Saga, detalle por servicio, SLO, runtime e infraestructura |
| Trazas | Jaeger en `localhost:16686` | Funcionan, con un corte conocido (ver [sección 2](#2-logs-y-trazas)) |
| Alertas | Alertmanager en `localhost:9097` | **Se evalúan, pero no avisan a nadie**: se envían a `webhook-mock` |
| NATS | CLI `nats`, puerto `4222`; monitoreo en `localhost:8222` | Funcionan |
| Postgres | `docker exec -it posnet-postgres psql -U posnet -d posnet` | Funciona |

**Que una alerta no haya llegado no significa que no se haya disparado:** revisá Alertmanager o
Prometheus directamente. Además, varias fallas no generan ninguna alerta (ver
[sección 5](#5-lo-que-ninguna-alerta-detecta)).

**Piezas simuladas o incompletas:** el adquirente aprueba todo, el procesador de liquidación es simulado
o no está configurado, y no hay WebSocket. Por eso, un comprobante en `SENT` no prueba que un terminal lo haya recibido. El detalle
de cada servicio está en [`docs/technical/`](../technical/).

---

## 1. Triage inicial

### 1.1 ¿Qué servicio está afectado?

| Servicio | Contenedor | HTTP | gRPC |
|---|---|---|---|
| Authorization | `posnet-authorization` | 8080 | 9090 |
| Terminal Gateway | `posnet-terminal-gateway` | 8081 | 9091 |
| Fraud Detection | `posnet-fraud-detection` | 8083 | 9092 |
| Settlement | `posnet-settlement` | 8084 | 9093 |
| Notification | `posnet-notification` | 8085 | 9094 |

Son los puertos de Docker Compose. Fuera de Docker, los defaults del código son distintos: Fraud
Detection 8082, Settlement 8083 y Notification 8084.

```bash
curl -s http://localhost:<port>/healthz   # 200 si el proceso está vivo
curl -s http://localhost:<port>/readyz    # hace ping a Postgres (en Authorization, también el schema; en Settlement, también tablas y NATS)
curl -s http://localhost:8222/healthz     # estado del servidor NATS

docker ps --filter name=posnet- --format '{{.Names}}\t{{.Status}}'
docker inspect -f '{{.RestartCount}}' posnet-settlement
```

`readyz` en `503` con `{"reason": "database unavailable"}` indica que el proceso está vivo pero no
alcanza Postgres: andá a la [sección 4](#4-postgres).

En Settlement, `503` con `{"reason": "database schema incomplete"}` indica que falta aplicar una
[migración](../technical/SETTLEMENT.md#aplicar-la-migración); el log `required tables are missing` nombra la tabla.
Authorization responde lo mismo, y su log `required schema is missing` nombra lo que falta. Authorization y Settlement son
los únicos servicios cuyo `healthcheck` de Docker usa `readyz`: un contenedor `unhealthy` puede indicar eso o una caída de
Postgres (en Settlement, también de NATS), aunque el proceso esté vivo. **Eso no frena el consumo de eventos.**

**`readyz` no verifica NATS, salvo en Settlement,** donde responde `503` con `{"reason": "nats unavailable"}`.
Los demás servicios pueden responder `200` sin poder consumir ni publicar eventos, por eso conviene
verificar NATS por separado.

### 1.2 ¿Qué tan grave es?

| Severidad | Ejemplo | Acción |
|---|---|---|
| **Crítico** | Un servicio no levanta, `readyz` en `503` sostenido, o la Saga no avanza para ningún terminal | Avisá al equipo de guardia y seguí este runbook completo |
| **Alto** | Transacciones trabadas en `FRAUD_CHECKING` o `INDETERMINATE`, o aprobaciones fuera de todo lote | [Sección 5](#5-lo-que-ninguna-alerta-detecta), y después logs y NATS |
| **Medio** | Lotes en `DISPUTED` o `CLOSED`, notificaciones en `DEAD` o `PENDING` | [batch-close-failure.md](batch-close-failure.md) o la [sección 5](#5-lo-que-ninguna-alerta-detecta) |
| **Bajo** | Un evento sin procesar, visible como error puntual | [nats-dlq-reprocess.md](nats-dlq-reprocess.md) |

### 1.3 Si llegaste por una alerta

| Alerta | Primer paso |
|---|---|
| `SagaCompletaDetenida` | Health de Authorization y Fraud Detection, y [transacciones trabadas](#5-lo-que-ninguna-alerta-detecta) |
| `SesionesBloqueadasProcessing` | Lo mismo. Las sesiones vencidas se borran, así que el valor puede bajar sin que nada se haya resuelto |
| `ErrorRateAlta` | `posnet_transactions_rejected_total` por `rejection_code` y `source`. Incluye rechazos de negocio válidos |
| `LatenciaSagaAlta`, `SagaLatencySLO*` | Duración de `posnet_fraud_engine_duration_seconds` y Postgres |
| `TxSuccessSLO*` | `posnet_acquirer_errors_total` y `posnet_nats_messages_failed_total`. Republicar eventos también aumenta esta última |
| `EvalTimeoutsFraud` | Postgres. **Hay transacciones que se están evaluando sin historial** (ver [sección 5](#5-lo-que-ninguna-alerta-detecta)) |
| `CacheRulesLow` | Volumen de tráfico: con poco tráfico puede activarse sin que haya un problema |
| `PoolPostgresAgotado`, `PostgresDeadlock` | [Sección 4](#4-postgres) |
| `DuplicadosNATSAltos` | Reinicios en bucle o `AckWait` insuficiente |
| `ReversasEnRevision` | [Escenario H](batch-close-failure.md#escenario-h--reversas-en-revisión). `MERCHANT_MISMATCH` es lo más urgente |
| `OutboxSettlementTrabado` | NATS y el log `outbox relay`. **No se pierde nada:** los eventos de Settlement salen solos al resolver la causa |
| `EventoSettlementPerdido` | **Falta una venta, una reversa o un cierre en Settlement.** Buscá `deliveries exhausted` en los logs y seguí [Reprocesamiento](nats-dlq-reprocess.md) |
| `LotesEnDisputa` | [Escenario A](batch-close-failure.md#escenario-a--lote-en-disputed) |
| `LotesCerradosSinEnviar`, `EnvioAlProcesadorFallando` | [Escenario C](batch-close-failure.md#escenario-c--lote-en-closed-sin-enviar) |
| `LotesAbiertosVencidos` | [Escenario F](batch-close-failure.md#escenario-f--lotes-abiertos-de-días-anteriores) |
| `GoroutineLeakSospechoso`, `HeapCreciente` | Dashboard de runtime. En Notification, revisar envíos a Terminal Gateway, que no tienen timeout |

---

## 2. Logs y trazas

Todos los logs son JSON. Para seguir un caso, buscá su `transaction_id` en **todos** los servicios de la
Saga:

```bash
for s in terminal-gateway authorization fraud-detection settlement notification; do
  echo "== $s"; docker logs "posnet-$s" 2>&1 | grep '<transaction_id>'
done
```

No todos los logs incluyen `transaction_id`: según el flujo, también sirven `event_id`, `terminal_id` o
`batch_id`.

En Jaeger, **la traza se corta entre Terminal Gateway y Authorization**, porque el outbox no propaga el
contexto. El envío de notificaciones también queda fuera de la traza del evento. Una traza que empieza
en Authorization es normal.

### Patrones de log

| Mensaje | Significado |
|---|---|
| `failed to unmarshal envelope`, `failed to unwrap` | Mensaje inválido: se descartó con `Term`. Revisá al productor |
| `permanent error — terminating` | `ValidationError` o `ConflictError`: no se reintenta. Termina en `message` solo en algunos servicios |
| `transient error — nacking for retry` | Se reintenta de inmediato, sin backoff, hasta `MaxDeliver`. En Settlement, con esperas de 5 s a 10 min |
| `deliveries exhausted` | Solo en Settlement: **el evento se agotó y no se reintenta.** Seguí [Reprocesamiento](nats-dlq-reprocess.md) |
| `fraud score event already processed — skipping` | **Si viene después de un error, la Saga se perdió:** la reentrega se descartó (ver [Reprocesamiento](nats-dlq-reprocess.md#3-confirmar-la-causa-y-entender-processed_events)) |
| `reversal event already processed — skipping` | Lo mismo, para una reversa |
| `failed to publish` | **Evento perdido.** Se registró el error y se confirmó el mensaje: nadie lo reintenta |
| `acquirer error — marking transaction as indeterminate` | La transacción queda en `INDETERMINATE`. El conciliador la revierte y la rechaza |
| `transaction not reconciled — retrying in the next run` | La reversa falló. La transacción sigue sin resultado y se reintenta sola |
| `reversal not completed — retrying in the next run` | Falló una reversa pedida. La venta sigue vigente, y se reintenta sola |
| `reversal ignored — nothing to reverse` | Se pidió la reversa de una transacción rechazada o ya revertida. No se llamó al adquirente |
| `acquirer response arrived after the reconciler took the transaction` | Respuesta tardía: se ignora, y vale el rechazo del conciliador. Si `response_state` es `APPROVED`, verificá que el host haya procesado la reversa |
| `acquirer reversal failed` | La reversa no se hizo y el mensaje se confirmó: requiere intervención manual |
| `rule engine failed — using neutral score` | La transacción pasó como `REVIEW`, sin caso de fraude guardado |
| `issuer ordered card capture` | Orden de retener la tarjeta; hoy no llega a ningún terminal |
| `modified concurrently` | Conflicto de versión en un lote. Esperable con tráfico simultáneo: se reintenta solo. Solo es un problema si termina agotando entregas |
| `no open batch found for terminal` | Cierre sin efecto ([Escenario B](batch-close-failure.md#escenario-b--cierre-sin-lote-abierto)) |
| `no settlement processor configured` | Lote sin enviar ([Escenario C](batch-close-failure.md#escenario-c--lote-en-closed-sin-enviar)) |
| `batch closed automatically without terminal totals` | Cierre automático, sin conciliar ([Escenario F](batch-close-failure.md#escenario-f--lotes-abiertos-de-días-anteriores)) |
| `notification delivery failed`, `failed to save notification` | Fallo de envío o de guardado de una notificación |
| `panic: runtime error` | Salida del proceso, no JSON. Revisá `docker logs` y el contador de reinicios |

Si el mismo `event_id` aparece varias veces con `transient error`, el consumidor está agotando sus
entregas: pasá a la [sección 3](#3-nats-jetstream).

---

## 3. NATS JetStream

Los comandos usan el CLI `nats` y se probaron con la versión `v0.4.0`. Desde el host, el servidor por
defecto (`localhost:4222`) coincide con el de Compose.

### 3.1 Estado de un consumidor

```bash
nats consumer info <STREAM> <DURABLE> --json \
  | jq '{num_pending, num_redelivered, num_ack_pending, max_deliver: .config.max_deliver}'
```

**La fuente de verdad son los `*_subscriber.go` de cada servicio.** `pkg/natsutil/consumers.go` tiene
nombres desactualizados y `EnsureConsumers` no se invoca.

| Durable | Stream | Subject | Entregas máx. |
|---|---|---|---|
| `auth-txn-receiver` | `POSNET_TRANSACTIONS` | `posnet.transaction.received.v1` | 5 |
| `auth-fraud-score-consumer` | `POSNET_FRAUD` | `posnet.fraud.score-calculated.v1` | 5 |
| `auth-reversal-processor` | `POSNET_TRANSACTIONS` | `posnet.transaction.reversal-requested.v1` | 3 |
| `fraud-check-consumer` | `POSNET_FRAUD` | `posnet.fraud.check-requested.v1` | 5 |
| `gateway-auth-consumer` | `POSNET_AUTH` | `posnet.auth.>` | 3 |
| `settlement-auth-consumer` | `POSNET_AUTH` | `posnet.auth.approved.v1` | 5 |
| `settlement-reversal-consumer` | `POSNET_AUTH` | `posnet.auth.reversal-completed.v1` | 5 |
| `settlement-batch-consumer` | `POSNET_TRANSACTIONS` | `posnet.transaction.batch-close.v1` | 5 |
| `notify-auth-approved` | `POSNET_AUTH` | `posnet.auth.approved.v1` | 3 |
| `notify-auth-rejected` | `POSNET_AUTH` | `posnet.auth.rejected.v1` | 3 |
| `notify-batch-closed` | `POSNET_SETTLEMENT` | `posnet.settlement.batch-closed.v1` | 3 |

Cómo leer los valores:

| Campo | Significado |
|---|---|
| `num_pending` alto y creciendo | El consumidor no da abasto o su proceso está caído |
| `num_ack_pending` | Mensajes entregados y sin confirmar, que todavía pueden reintentarse |
| `num_redelivered` | Cuántos **mensajes** se reentregaron. No es un contador de intentos, así que no sirve compararlo con `max_deliver` |

Un mensaje que agota sus entregas sale de `num_ack_pending`, y `num_redelivered` no vuelve a cero.
Nadie lo reintenta solo, pero sigue en el stream: ver [nats-dlq-reprocess.md](nats-dlq-reprocess.md).

### 3.2 Estado de un stream

```bash
nats stream info <STREAM> --json | jq '.state | {messages, first_seq, last_seq, consumer_count}'
```

Streams: `POSNET_TRANSACTIONS`, `POSNET_FRAUD`, `POSNET_AUTH`, `POSNET_SETTLEMENT`,
`POSNET_NOTIFICATION` y `POSNET_DLQ`. Todos usan `Replicas: 1`, sin tolerancia a la caída del nodo (ver
[ADR 002](../adr/002-nats-jetstream.md)), y `MaxAge: 0`, de modo que los mensajes no vencen por tiempo.
`POSNET_DLQ` existe, pero nada publica en él.

---

## 4. Postgres

Schemas: `terminal_gateway`, `pn_authorization`, `fraud_detection`, `settlement` y `notification`. Cada
uno tiene su tabla `processed_events`.

```sql
-- ¿Se marcó este evento? Una fila NO garantiza que el trabajo se haya hecho:
-- ver "Entender processed_events" en nats-dlq-reprocess.md
SELECT * FROM settlement.processed_events WHERE event_id = '<uuid>';

-- Conexiones activas, si readyz falla por agotamiento
SELECT count(*), state FROM pg_stat_activity WHERE datname = 'posnet' GROUP BY state;
```

`pkg/pgutil.HealthCheck` es un `pool.Ping(ctx)`: si falla, es conectividad o un pool agotado, no una
consulta puntual.

**Migraciones manuales:** no se aplican solas. Sin la de la
[blocklist](../technical/AUTHORIZATION.md#aplicar-la-migración-de-blocklist), todas las transacciones fallan
por la falta de `card_token`. Sin la de
[lotes](../technical/SETTLEMENT.md#aplicar-la-migración), Settlement falla por la falta de `version`, `settlement.outbox`, `settlement.batch_audit_log` o
`settlement.reversal_reviews`; sin esta última, fallan todas las ventas.

---

## 5. Lo que ninguna alerta detecta

Estas consultas encuentran fallas silenciosas. Conviene ejecutarlas durante un incidente, incluso si el
síntoma parece otro.

### Transacciones trabadas

```sql
SELECT t.id, t.state, t.terminal_id, t.amount_cents, t.created_at, fc.decision AS fraud_decision
FROM pn_authorization.transactions t
LEFT JOIN fraud_detection.fraud_cases fc ON fc.transaction_id = t.id
WHERE t.state IN ('FRAUD_CHECKING', 'PROCESSING', 'INDETERMINATE')
  AND t.created_at < NOW() - INTERVAL '5 minutes'
ORDER BY t.created_at;
```

| Resultado | Causa probable | Qué hacer |
|---|---|---|
| `FRAUD_CHECKING`, sin caso de fraude | El pedido se perdió, Fraud Detection no lo procesó, o falló su motor | [Reprocesar](nats-dlq-reprocess.md) desde `check-requested` |
| `FRAUD_CHECKING`, con caso de fraude | El score se perdió o falló su procesamiento | [Reprocesar](nats-dlq-reprocess.md) según la tabla de `processed_events` |
| `PROCESSING` o `INDETERMINATE` por más de 3 minutos | El conciliador no puede resolverla: alerta `TransaccionesSinResolver`. La reversa está fallando, o el conciliador está desactivado o sin la migración `000002` | Revisar el adquirente y los logs `transaction not reconciled`. Se resuelve sola cuando el host responda. **No la cierres a mano sin confirmar con el adquirente:** el emisor pudo haberla aprobado |

```sql
-- Reversas pedidas sin completar: la venta sigue vigente
SELECT id, terminal_id, amount_cents, reversal_requested_at
FROM pn_authorization.transactions
WHERE state = 'APPROVED' AND reversal_requested_at IS NOT NULL
ORDER BY reversal_requested_at;
```

**No hay timeout ni bypass de fraude**, aunque los logs y comentarios los mencionen: una transacción
trabada en `FRAUD_CHECKING` no se resuelve sola. Para el cliente, Terminal Gateway borra la sesión vencida, y `GET
/api/sessions/{id}/status` pasa a responder `404`.

### Aprobaciones fuera de todo lote

```sql
SELECT t.id, t.terminal_id, t.amount_cents, t.authorized_at
FROM pn_authorization.transactions t
LEFT JOIN settlement.batch_transactions bt ON bt.transaction_id = t.id AND bt.tx_type = 'PURCHASE'
WHERE t.state = 'APPROVED'
  AND bt.transaction_id IS NULL
  AND t.authorized_at < NOW() - INTERVAL '5 minutes'
ORDER BY t.authorized_at DESC;
```

Ver el [Escenario D](batch-close-failure.md#escenario-d--aprobaciones-fuera-de-todo-lote).

### Notificaciones perdidas

```sql
SELECT id, transaction_id, channel, state, attempt_count, created_at
FROM notification.notifications
WHERE state = 'DEAD'
   OR (state = 'PENDING' AND created_at < NOW() - INTERVAL '5 minutes')
ORDER BY created_at DESC;
```

Las notificaciones en `PENDING` no se reintentan nunca, y `force-retry` no saca a ninguna de `DEAD`. Ver
[Notification](../technical/NOTIFICATION.md#reenviar-una-notificación-dead).

En un entorno recién levantado, que todos los webhooks lleguen a `DEAD` suele indicar que `webhook-mock`
no tiene una respuesta configurada.

### Control de fraude degradado

```promql
rate(posnet_fraud_engine_timeouts_total[5m])
```

Si fallan las consultas de historial, las reglas no se activan y la transacción puede quedar como
`APPROVE` con score 0, **sin error ni log**. Solo los timeouts dejan esta métrica. Para confirmar:

```sql
SELECT decision, count(*), avg(score)::int AS avg_score
FROM fraud_detection.fraud_cases
WHERE evaluated_at > NOW() - INTERVAL '1 hour'
GROUP BY decision;
```

Las reglas también pueden haberse cambiado: `PUT /rules/{id}/threshold` no tiene autenticación ni deja
registro. Compará `GET /rules` con los pesos por defecto (20, 15, 25, 20 y 30).

---

## 6. Cuándo derivar a un runbook específico

| Síntoma | Runbook |
|---|---|
| Lotes en `DISPUTED` o `CLOSED`, cierres sin efecto, aprobaciones fuera de lote o reversas sin descontar | [batch-close-failure.md](batch-close-failure.md) |
| Mensajes que agotaron sus entregas, eventos perdidos o transacciones trabadas | [nats-dlq-reprocess.md](nats-dlq-reprocess.md) |

## 7. Después del incidente

- Guardá los `event_id` y `transaction_id` afectados, y cualquier SQL que hayas ejecutado.
- Si algo se resolvió a mano, documentalo. Las brechas conocidas están en
  [`docs/technical/`](../technical/): si encontrás una nueva, agregala ahí y no la dejes solo en la
  memoria de quien respondió.

# Notification — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/notification/README.md) o de los
> comentarios del código, vale lo que está acá.

## 1. Resumen

Notification informa los resultados del sistema hacia afuera: envía comprobantes al terminal, a través
de [Terminal Gateway](TERMINAL-GATEWAY.md), y webhooks al comercio. Consume eventos de
[Authorization](AUTHORIZATION.md) y [Settlement](SETTLEMENT.md), reintenta con espera creciente y deja
en `DEAD` lo que no pudo entregar. No toma decisiones de negocio.

El mecanismo de reintentos funciona, pero **que una notificación figure como `SENT` no garantiza que
haya llegado**, y tres fallas afectan a la operación:

- **Los comprobantes al terminal se marcan como `SENT` sin entregarse**, porque Terminal Gateway responde
  `delivered=true` sin enviar nada.
- **Todos los webhooks van a una misma URL**, sea cual sea el comercio.
- **`force-retry` no recupera las notificaciones `DEAD`:** vuelve a enviar, pero el estado no cambia.

| Capacidad | Estado | Detalle |
|---|---|---|
| Registro de notificaciones | Implementado | Aprobación, rechazo y cierre de lote |
| Reintentos con espera creciente | Implementado | Solo desde `RETRYING` |
| Envío de webhooks | **Parcial** | Una sola URL global, sin firma |
| Comprobante al terminal | **Parcial** | Llama a Terminal Gateway, que no lo entrega |
| Reintento manual (`force-retry`) | **Roto** | Reenvía sin actualizar el estado |
| Consulta de notificaciones | **Parcial** | No muestra el historial de intentos |
| Evento de auditoría `dispatched` | Implementado | Ningún contexto lo consume |
| Comprobante de reversa | **Pendiente** | No consume `reversal-completed` |
| Aviso de retención de tarjeta | **Pendiente** | Ignora `capture_card` |
| Canales `EMAIL` y `SMS` | **Pendiente** | Solo definidos |
| Servicio gRPC | **Placeholder** | Hay contrato, pero ningún servicio registrado |

Las consecuencias de estos pendientes están en [§11](#11-brechas-conocidas).

## 2. Contexto y límites

```
Authorization ── auth.approved ───►┌──────────────┐── gRPC SendReceipt ──► Terminal Gateway ┄┄► Terminal
Authorization ── auth.rejected ───►│ Notification │                        (no entrega)
Settlement ── settlement.batch-closed ►│          │── HTTP POST ──────────► Endpoint de webhook
                                   └──────┬───────┘
                                          │ notification.dispatched
                                          ▼
                                     Sin consumidores
```

- **Es dueño de:** las notificaciones, sus intentos de entrega y la política de reintentos.
- **No decide:** el resultado de un pago ni el contenido del lote; solo los informa.
- **Es el único cliente gRPC del sistema:** usa su propio cliente hacia Terminal Gateway.

### Qué genera cada evento

| Evento | Terminal | Webhook |
|---|---|---|
| `auth.approved` | Sí | Sí |
| `auth.rejected` | Sí | No |
| `settlement.batch-closed` | No | Sí |

## 3. Estructura del código

| Ruta | Contenido |
|---|---|
| [`cmd/notification/main.go`](../../cmd/notification/main.go) | Arranque y apagado |
| [`cmd/notification/nt_wire.go`](../../cmd/notification/nt_wire.go) | Wiring y job de reintentos |
| [`domain/aggregate/`](../../context/notification/domain/aggregate/) | `Notification` (aggregate root) |
| [`domain/entity/`](../../context/notification/domain/entity/) | `DeliveryAttempt` |
| [`domain/valueobject/`](../../context/notification/domain/valueobject/) | Estados, canales y `ReceiptPayload` |
| [`domain/repository/`](../../context/notification/domain/repository/) | Puerto de notificaciones |
| [`domain/service/`](../../context/notification/domain/service/) | Puertos de terminal, webhook y publisher |
| [`application/command/`](../../context/notification/application/command/) | `NotifyHandler`, `AdminHandler` y métricas |
| [`application/query/`](../../context/notification/application/query/) | Consultas |
| [`infrastructure/grpc/client/`](../../context/notification/infrastructure/grpc/client/) | Cliente hacia Terminal Gateway |
| [`infrastructure/webhook/`](../../context/notification/infrastructure/webhook/) | Envío de webhooks |
| [`infrastructure/nats/`](../../context/notification/infrastructure/nats/) | Publisher y subscriber |
| [`infrastructure/postgres/`](../../context/notification/infrastructure/postgres/) | Repositorio |
| [`infrastructure/http/`](../../context/notification/infrastructure/http/) | Health checks, consulta y administración |
| [`infrastructure/grpc/server/`](../../context/notification/infrastructure/grpc/server/) | Servidor sin servicios |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/notification/`.

## 4. Modelo de dominio

### 4.1 `Notification`

Cada notificación corresponde a un único canal: una aprobación genera dos.

| Campo | Descripción |
|---|---|
| `id` | UUID de la notificación |
| `transactionID` | Transacción relacionada; en un cierre de lote guarda el `batch_id` |
| `merchantID` | Comercio |
| `channel` | `TERMINAL_WEBSOCKET` o `WEBHOOK` (`EMAIL` y `SMS` no se usan) |
| `state` | Estado actual |
| `receipt` | Contenido del comprobante |
| `attempts`, `maxAttempts` | Intentos realizados; el máximo es 5 |
| `createdAt`, `dispatchedAt`, `nextRetryAt` | Marcas de tiempo |

### 4.2 Máquina de estados

```
PENDING ──────────► SENT
   │                 ▲
   ▼                 │
 FAILED ──► RETRYING ┘
   │  ▲        │
   │  └────────┘ (el reintento falla)
   ▼
 DEAD
```

`SENT` y `DEAD` son finales: **ninguna transición sale de ellos**. `FAILED` es un paso intermedio que no
llega a guardarse: tras un fallo, la notificación se guarda en `RETRYING` o `DEAD`.

**El significado de `SENT` depende del canal:**

| Canal | `SENT` significa |
|---|---|
| `WEBHOOK` | El endpoint respondió con un `2xx` |
| `TERMINAL_WEBSOCKET` | Terminal Gateway encontró la sesión; **el terminal no recibió nada** |

### 4.3 Política de reintentos

| Intento | Espera antes del intento |
|---|---|
| 1 | Inmediato |
| 2 | 30 s |
| 3 | 2 min |
| 4 | 10 min |
| 5 | 1 h |
| Si falla el 5.º | Pasa a `DEAD` |

Las esperas están fijas en el código. Como el job corre cada minuto, cada reintento puede demorarse
hasta un minuto más. Una notificación llega a `DEAD` al cabo de unos 73 minutos o algo más.

### 4.4 `ReceiptPayload`

Lo que el comprobante incluye depende del evento:

| Campo | Aprobación | Rechazo | Cierre de lote |
|---|---|---|---|
| `TransactionID` | Sí | Sí | Guarda el `batch_id` |
| `Result` | `APPROVED` | `REJECTED` | `BATCH_CLOSED` |
| `AmountCents`, `Currency` | Sí | Sí | Sí |
| `TransactionAt` | Sí | Sí | Sí |
| `CardLast4`, `CardNetwork`, `EntryMode` | Sí | Sí | No |
| `AuthCode` | Sí | — | — |
| `RejectionCode`, `RejectionReason` | — | Sí | — |
| `TerminalCode` | Guarda el UUID del terminal | Guarda el UUID del terminal | No |
| `MerchantName`, `MerchantAddress` | Vacíos | Vacíos | Vacíos |

**El webhook de cierre de lote no informa las discrepancias**, ni la cantidad de transacciones, ni la
fecha, ni el terminal, aunque el evento de Settlement traiga esos datos. El comercio no puede distinguir
un cierre correcto de uno con diferencias.

## 5. Flujos

### 5.1 Envío inicial

```
Evento NATS
   │
   ▼
Valida los datos y arma las notificaciones
   │
   ▼
TX: marca el evento ─ guarda las notificaciones en PENDING
   │
   ├──► Confirma el mensaje a NATS
   │
   └──► En una goroutine, por cada notificación:
           envía ─► actualiza el estado ─► guarda ─► publica dispatched (si se entregó)
```

El envío ocurre en una goroutine separada, y el mensaje se confirma sin esperar el resultado. Por eso,
**un fallo durante el envío no provoca una reentrega de NATS**: de ahí en adelante, la entrega depende
del job de reintentos.

### 5.2 Job de reintentos

Cada `RETRY_JOB_INTERVAL` (1 minuto por defecto), el job busca hasta `RETRY_BATCH_SIZE` notificaciones en
`RETRYING` con `next_retry_at` vencido, y lanza una goroutine por cada una.

**Solo mira `RETRYING`.** Una notificación que queda en `PENDING` nunca se reintenta.

No hay bloqueo entre instancias ni marca de "en curso". Con más de una instancia, o si un envío tarda
más que el intervalo del job, la misma notificación puede enviarse dos veces.

### 5.3 Comprobante al terminal

1. Llama a `SendReceipt` en Terminal Gateway por gRPC, con [mTLS](#7-grpc) si está configurado y **sin timeout**.
2. Terminal Gateway verifica que la sesión exista y responde `delivered=true`, sin enviar nada.
3. La notificación pasa a `SENT`.

Si la sesión ya no existe, responde `SESSION_NOT_FOUND`: se registra como `terminal not connected`, y
la notificación pasa por los reintentos hasta llegar a `DEAD`. Es lo que ocurre con una aprobación
tardía cuya sesión borró el job de limpieza de Terminal Gateway.

El cliente tampoco envía `transaction_at`, aunque el contrato lo prevé.

### 5.4 Webhook

```http
POST <WEBHOOK_DEFAULT_ENDPOINT>
Content-Type: application/json
X-Posnet-Notification-ID: 5d1c7a54-0a7c-4a60-9c5e-2f6b8f1e3a77
X-Posnet-Transaction-ID: 7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10
```

```json
{
  "notification_id": "5d1c7a54-0a7c-4a60-9c5e-2f6b8f1e3a77",
  "transaction_id": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "channel": "WEBHOOK",
  "dispatched_at": "2026-11-05T15:00:05Z",
  "receipt": {
    "TransactionID": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
    "MerchantName": "",
    "MerchantAddress": "",
    "TerminalCode": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "Result": "APPROVED",
    "AuthCode": "482913",
    "RejectionCode": "",
    "RejectionReason": "",
    "AmountCents": 150000,
    "Currency": "ARS",
    "CardLast4": "4242",
    "CardNetwork": "VISA",
    "EntryMode": "CONTACTLESS",
    "TransactionAt": "2026-11-05T15:00:04Z"
  }
}
```

Para quien lo reciba:

- **Los nombres mezclan formatos:** el sobre usa snake_case, y `receipt`, PascalCase.
- **Puede llegar más de una vez:** hay que deduplicar por `X-Posnet-Notification-ID`.
- **No está firmado:** el receptor no puede comprobar que venga de este sistema.
- **Cualquier respuesta que no sea `2xx` se reintenta**, incluidos los `4xx` permanentes.

### 5.5 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| IDs o monto inválidos | `Term`; no se crea la notificación | No |
| Error de base de datos antes de confirmar | `Nak` y reintento | **Sí** |
| Falla el envío | Pasa a `RETRYING` | **Sí**, hasta 5 intentos |
| Sin `WEBHOOK_DEFAULT_ENDPOINT` | Cada intento falla hasta llegar a `DEAD` | No |
| Caída del proceso durante el envío | Queda en `PENDING` | No |
| Falla el guardado tras un fallo de envío | Queda en `PENDING` | No |
| Falla el guardado tras una entrega | Queda en `PENDING`; ya se publicó `dispatched` | No |
| Terminal Gateway no responde y no corta la conexión | La goroutine queda esperando, sin timeout | No |
| Se agotan los 5 intentos | `DEAD`, sin métrica ni alerta | No: `force-retry` tampoco lo resuelve |

## 6. API HTTP

Puerto `HTTP_PORT`: 8084 por defecto en el código y **8085 en Docker Compose**. **No hay autenticación**,
ni siquiera en `force-retry`.

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; solo verifica Postgres | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `GET /notifications/{id}` | Consultar una notificación | `200` | `404`, `500` |
| `GET /transactions/{tx_id}/notifications` | Notificaciones de una transacción | `200` | `400`, `500` |
| `GET /notifications/dead?limit=50` | Notificaciones en `DEAD` | `200` | `500` |
| `POST /notifications/{id}/force-retry` | Reenviar | `202` | `404`, `500` |

Un ID que no sea un UUID en `GET /notifications/{id}` o en `force-retry` responde `500`, no `400`. Los
errores tienen la forma `{"error": "CODIGO", "message": "..."}`.

### Consultar una notificación

Responde con nombres en PascalCase, porque el struct no tiene tags JSON:

```json
{
  "ID": "5d1c7a54-0a7c-4a60-9c5e-2f6b8f1e3a77",
  "TransactionID": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "MerchantID": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "Channel": "WEBHOOK",
  "State": "RETRYING",
  "AttemptCount": 2,
  "MaxAttempts": 5,
  "CreatedAt": "2026-11-05T15:00:05Z",
  "DispatchedAt": "",
  "NextRetryAt": "2026-11-05T15:02:36Z",
  "Attempts": null
}
```

**`Attempts` siempre llega en `null`**: el repositorio carga los intentos, pero el mapper no los copia.
Para saber por qué falló una entrega hay que consultar la base (ver [§10](#10-observabilidad-y-operación)).
`NextRetryAt` también puede conservar su último valor después de pasar a `SENT`.

Las consultas de listado devuelven `{"notifications": [...], "count": N}`, con la misma forma.

### Reintento manual

`POST /notifications/{id}/force-retry` no lleva body y **siempre responde `202`** si la notificación
existe, con `{"status": "retry_dispatched"}`, aunque el envío falle. No exige `operator_id`, de modo que
no queda registro de quién lo ejecutó. Su comportamiento depende del estado:

| Estado | Qué ocurre |
|---|---|
| `DEAD` | Envía de nuevo; **el estado no cambia** porque `DEAD` no admite transiciones. Sigue en la lista, aunque se haya entregado |
| `SENT` | **Vuelve a enviar** un comprobante ya entregado; el estado no cambia |
| `PENDING`, `RETRYING` | Envía y actualiza el estado normalmente |

Si el webhook se entrega, se cuenta en `posnet_notifications_sent_total`, pero no queda un intento
registrado ni se publica `dispatched`. El envío ocurre durante la solicitud HTTP, cuyo `WriteTimeout` es
de 10 s: con un `WEBHOOK_TIMEOUT` mayor, el cliente puede perder la respuesta.

## 7. gRPC

### Como cliente

Usa `SendReceipt` de Terminal Gateway, definido en
[`terminal_gateway.proto`](../../pkg/proto/terminalgateway/v1/terminal_gateway.proto). La dirección se
toma de `TERMINAL_GATEWAY_GRPC_ADDR`. La conexión es perezosa, por lo que el log de arranque
`gRPC client connected` no confirma que Terminal Gateway esté disponible.

**mTLS:** con `GRPC_CLIENT_TLS_CA`, la conexión usa TLS y verifica el certificado de Terminal Gateway con esa CA. Con
`GRPC_CLIENT_TLS_CERT` y `GRPC_CLIENT_TLS_KEY`, presenta el certificado de Notification, que Terminal Gateway exige con
`GRPC_TLS_CLIENT_CA` (ver [Terminal Gateway](TERMINAL-GATEWAY.md#mtls)). Si el certificado del servidor no incluye el host de
`TERMINAL_GATEWAY_GRPC_ADDR`, se declara el nombre con `GRPC_CLIENT_TLS_SERVER_NAME`. **Fuera de
`ENVIRONMENT=development`, el servicio no arranca sin los tres primeros.** Si fallan el handshake o la verificación, el envío
falla y la notificación sigue el camino de reintentos. Docker Compose no usa TLS.

### Como servidor

Hay un servidor en el puerto 9094 sin servicios registrados: cualquier llamada devuelve `Unimplemented`.
[`notification.proto`](../../pkg/proto/notification/v1/notification.proto) define
`GetNotificationStatus`, `RetryNotification` y `ListFailedWebhooks`, pero ningún código los usa. El puerto
está fijo y no respeta `GRPC_PORT`.

## 8. Eventos NATS

Todos los eventos usan el envelope de [`pkg/events/envelope.go`](../../pkg/events/envelope.go).

### Consumidos

Son durables con queue group del mismo nombre, hasta 3 entregas, política `DeliverNew` y `AckWait` de
30 s:

| Durable | Subject | Stream | Acción |
|---|---|---|---|
| `notify-auth-approved` | `posnet.auth.approved.v1` | `POSNET_AUTH` | `NotifyApproval` |
| `notify-auth-rejected` | `posnet.auth.rejected.v1` | `POSNET_AUTH` | `NotifyRejection` |
| `notify-batch-closed` | `posnet.settlement.batch-closed.v1` | `POSNET_SETTLEMENT` | `NotifyBatchClosed` |

`pkg/natsutil/consumers.go` menciona `notify-auth-consumer` y `notify-settlement-consumer`, que **no
existen**: al diagnosticar, usar los nombres de esta tabla.

No se consumen `auth.reversal-completed` ni `settlement.completed`. Del evento de rechazo se ignoran
`capture_card`, `source` e `is_retryable`.

| Situación | Acción | Efecto |
|---|---|---|
| Envelope o payload inválido | `Term` | No se reintenta |
| `ValidationError` o `ConflictError` | `Term` | No se reintenta |
| Cualquier otro error | `Nak` | Reentrega inmediata, sin backoff |

Un mensaje que agota las entregas no pasa a una DLQ. Para recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md). Este mecanismo es independiente del estado
`DEAD`, que es propio de las notificaciones y se guarda en Postgres.

### Publicado

`posnet.notification.dispatched.v1`, en `POSNET_NOTIFICATION`, sin outbox. Es un evento de auditoría que
ningún contexto consume, con `notification_id`, `transaction_id`, `channel`, `attempts` y
`dispatched_at`. **Se publica antes de guardar el estado**, de modo que puede anunciar una entrega que la
base no registró.

## 9. Persistencia

Schema `notification`. **La fuente real es
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql):** `pgutil.Migrate` no
hace nada, `migrations/notification/` está vacío y
[`nt_schema.sql`](../../context/notification/infrastructure/postgres/nt_schema.sql) no se ejecuta.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `notifications` | Notificaciones | PK `id`; `receipt` en JSONB |
| `delivery_attempts` | Historial de intentos | PK `{notification_id}-{attempt_number}`; FK `notification_id` |
| `processed_events` | Idempotencia | PK `event_id` |

**`01-init.sql` no crea ningún índice secundario.** `nt_schema.sql` define cuatro: por transacción,
reintentos pendientes, `DEAD` e intentos por notificación. Sin ellos, el job de reintentos y cada carga
de intentos recorren la tabla, y su costo crece con el historial.

### Garantías actuales

- **Idempotencia:** el evento se marca en `processed_events`, pero `Save` usa su propia transacción. Si
  falla el commit o el guardado de la segunda notificación de una aprobación, la reentrega crea
  notificaciones nuevas, y la anterior queda en `PENDING`.
- **Entrega:** *at-least-once* mientras la notificación esté en `RETRYING`; sin garantía si queda en
  `PENDING`.

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene HTTP (hasta 10 s), cierra el cliente gRPC, drena y cierra NATS, y
cierra el pool de Postgres. **No espera a las goroutines de envío:** las que estén en curso pueden fallar
al guardar y dejar la notificación en `PENDING`. El servidor gRPC no se detiene de forma ordenada.

## 10. Observabilidad y operación

### Métricas

| Métrica | Tipo | Labels | Significado |
|---|---|---|---|
| `posnet_notifications_sent_total` | Counter | `channel` | Envíos exitosos; incluye los comprobantes al terminal no entregados |
| `posnet_notifications_failed_total` | Counter | `channel` | **Intentos** fallidos, no notificaciones perdidas |
| `posnet_grpc_request_duration_seconds` | Histogram | `channel` | Duración del envío. A pesar del nombre, también mide los webhooks |

`channel` vale `terminal` o `webhook`. También se exponen métricas del pool de Postgres y de mensajes
NATS.

**No hay métrica ni alerta para `DEAD`**, y ninguna regla de alertas cubre este contexto. **Dashboard:**
`posnet-bc-detail` muestra la duración del envío. `sent` y `failed` no aparecen en ningún dashboard.

### Para detectar problemas

```sql
-- Notificaciones trabadas o perdidas
SELECT id, transaction_id, channel, state, attempt_count, created_at, next_retry_at
FROM notification.notifications
WHERE state = 'DEAD'
   OR (state = 'PENDING' AND created_at < NOW() - INTERVAL '5 minutes')
ORDER BY created_at DESC;

-- Motivo de cada intento (la API no lo muestra)
SELECT attempt_number, success, http_status, error_message, attempted_at
FROM notification.delivery_attempts
WHERE notification_id = '<id>'
ORDER BY attempt_number;
```

### Reenviar una notificación `DEAD`

Hoy `force-retry` no actualiza el estado. Si la entrega se confirma en el destino, hay que corregir la
fila a mano. Para que vuelva a la cola de reintentos, en cambio, se puede actualizar así, revisando antes
el caso:

```sql
UPDATE notification.notifications
SET state = 'RETRYING', next_retry_at = NOW()
WHERE id = '<id>' AND state = 'DEAD';
```

Con esto se hace un solo intento más: si falla, la notificación vuelve a `DEAD`, porque ya acumula 5
intentos. El mismo `UPDATE`, con `state = 'PENDING'` en la condición, recupera las que quedaron en
`PENDING`. No se debe aplicar a un webhook sin confirmar antes que no se haya entregado, porque se
reenviaría.

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `NATS_URL` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `TERMINAL_GATEWAY_GRPC_ADDR` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `HTTP_PORT` | `8084` | Sí; Docker Compose usa `8085` |
| `WEBHOOK_TIMEOUT` | `10s` | Sí; con más de `30s`, el servicio no arranca |
| `WEBHOOK_DEFAULT_ENDPOINT` | Vacía | Sí; **es el destino de todos los webhooks** |
| `RETRY_JOB_INTERVAL` | `1m` | Sí |
| `RETRY_BATCH_SIZE` | `50` | Sí; fuera de 1–500, el servicio no arranca |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `10`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-notification` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `GRPC_CLIENT_TLS_CA` | Vacía | Sí; CA de Terminal Gateway: habilita [TLS](#7-grpc). Fuera de desarrollo, hace falta junto con las dos siguientes |
| `GRPC_CLIENT_TLS_CERT`, `GRPC_CLIENT_TLS_KEY` | Vacías | Sí; certificado de Notification para mTLS. Van juntas y requieren `GRPC_CLIENT_TLS_CA`. Si no se pueden cargar, el servicio no arranca |
| `GRPC_CLIENT_TLS_SERVER_NAME` | Host de `TERMINAL_GATEWAY_GRPC_ADDR` | Sí; nombre con el que se verifica el certificado de Terminal Gateway |
| `GRPC_PORT` | `9094` | **No**: el puerto está fijo en 9094 |
| `MIGRATIONS_DIR` | `migrations/notification` | **No**: `Migrate` no hace nada |

### Levantar y probar

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build \
  terminal-gateway authorization fraud-detection notification webhook-mock
```

Sin Docker, con Postgres, NATS y Terminal Gateway disponibles:

```bash
POSTGRES_DSN='postgres://...' NATS_URL='nats://localhost:4222' \
TERMINAL_GATEWAY_GRPC_ADDR='localhost:9091' WEBHOOK_DEFAULT_ENDPOINT='http://localhost:9200/webhook' \
go run ./cmd/notification
```

El comando `go run ./context/notification/...` del README no es válido: el entrypoint está en `cmd/`. Los
templates de Helm de este servicio están vacíos.

**El servicio `webhook-mock` no trae ninguna respuesta configurada.** MockServer responde `404` a lo que
no reconoce, de modo que en un entorno recién levantado los webhooks fallarían hasta llegar a `DEAD`.
Antes de probar, registrar una respuesta:

```bash
curl -s -X PUT localhost:9200/mockserver/expectation -d '{
  "httpRequest":  { "method": "POST", "path": "/webhook" },
  "httpResponse": { "statusCode": 200 }
}'
```

Luego, crear y pagar una sesión como se indica en
[Terminal Gateway](TERMINAL-GATEWAY.md#probar-un-pago), y consultar con el mismo ID:

```bash
curl -s localhost:8085/transactions/$ID/notifications | jq
```

### Tests

```bash
go test ./context/notification/...
```

Los tests usan una base simulada. `TestForceRetry_ReturnsNilRegardlessOfDispatchOutcome` valida la
respuesta, pero no que el estado cambie, y por eso no detecta la falla de `force-retry`. No hay tests del
wiring, ni de integración contra Postgres y NATS reales, ni un archivo de carga de Locust para este
contexto.

## 11. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Entrega

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Los comprobantes al terminal figuran `SENT` sin entregarse | El historial y las métricas indican éxito, aunque ningún terminal reciba nada | `tg_grpc_server.go`, `dispatch` |
| Las notificaciones en `PENDING` nunca se reintentan | Una caída durante el envío o un fallo al guardar las deja perdidas, sin error visible | `FindPendingRetries`, `dispatch` |
| `force-retry` no saca de `DEAD` | Responde `202` y puede entregar, pero el estado no cambia y sigue en la lista | `ForceRetry`, `CanTransitionTo` |
| `force-retry` acepta cualquier estado | Puede reenviar comprobantes ya entregados | `ForceRetry` |
| `SendReceipt` no tiene timeout | Si Terminal Gateway deja de responder, las goroutines quedan esperando indefinidamente | `dispatch`, `nt_grpc_client.go` |
| Envíos duplicados en reintentos | El job no reserva lo que procesa: con varias instancias, o un envío lento, se repite la entrega | `runRetryJob` |
| Reintentos sin distinguir el error | Un `4xx` permanente o una URL ausente consumen igual los 5 intentos | `dispatch` |
| Goroutines sin límite ni espera al apagar | Pueden acumularse envíos, y al apagar se interrumpen sin guardar | `dispatch`, `runRetryJob`, `close` |

### Webhooks y seguridad

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Una sola URL para todos los comercios | **Los datos de cada comercio llegan al mismo destino.** Es un riesgo de exposición si se usa con más de un comercio | `resolveEndpoint` |
| Webhooks sin firma | El receptor no puede validar el origen ni la integridad | `Dispatch` |
| API sin autenticación ni auditoría | Con acceso de red, cualquiera puede leer notificaciones y disparar reenvíos, sin dejar registro | `nt_http_handler.go` |

### Contenido de las notificaciones

| Brecha | Consecuencia | Dónde |
|---|---|---|
| El cierre de lote omite discrepancias, cantidad, fecha y terminal | El comercio recibe un monto, pero no puede saber si el lote cerró con diferencias | `NotifyBatchClosed` |
| No se generan comprobantes de reversa | El contrato de `reversal-completed` lo prevé, pero no hay suscripción: ni terminal ni comercio se enteran | `nt_nats_subscriber.go` |
| `capture_card` se ignora | `pkg/domain/card_capture.go` prevé escalar la alerta, pero no ocurre, y el comprobante no lo indica | `NotifyRejection` |
| Los rechazos no generan webhook | Es una decisión explícita del código; el comercio no recibe un aviso automático | `NotifyRejection` |
| Comprobante incompleto | `MerchantName` y `MerchantAddress` están vacíos, `TerminalCode` guarda un UUID, y el cliente gRPC omite `transaction_at` | `NotifyApproval`, `nt_grpc_client.go` |
| No hay aviso por `settlement.completed` | Settlement lo publica, pero Notification no se suscribe | `nt_nats_subscriber.go` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Sin métrica ni alerta para `DEAD` o `PENDING` | Las pérdidas solo se detectan consultando la base | `metrics.go`, Prometheus |
| La API no muestra los intentos | `Attempts` siempre llega en `null`; el motivo del fallo solo se ve por SQL | `toResult` |
| `webhook-mock` sin respuesta configurada | En un entorno recién levantado, los webhooks fallan hasta llegar a `DEAD` | `docker-compose.yml` |
| Sin índices secundarios | El job y las consultas recorren tablas que crecen indefinidamente | `01-init.sql` |
| `dispatched` se publica antes de guardar | Puede anunciar una entrega no registrada | `dispatch` |
| Notificaciones huérfanas en reentregas | Un fallo parcial al guardar una aprobación puede dejar filas en `PENDING` y crear otras nuevas | `NotifyApproval` |
| Trazas cortadas en el envío | Las goroutines usan `context.Background()`, por lo que el envío queda fuera de la traza del evento | `NotifyApproval`, `NotifyRejection`, `NotifyBatchClosed` |
| Nombres de consumidores desactualizados | `consumers.go` menciona durables que no existen | `pkg/natsutil/consumers.go` |
| Métrica mal nombrada | `posnet_grpc_request_duration` también mide webhooks | `metrics.go` |
| Sin migraciones reales | Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `nt_schema.sql` difieren | `pkg/pgutil/migrate.go` |
| gRPC placeholder y sin apagado ordenado | Abre un puerto que no ofrece nada, y `notification.proto` no se usa | `nt_grpc_server.go`, `nt_wire.go` |
| `/readyz` no verifica NATS ni Terminal Gateway | Una instancia sin esas dependencias sigue figurando como lista | `nt_http_handler.go` |
| Código sin uso | `port.NotificationService`, `port.AdminService`, `ReceiptBuilder` y los eventos de dominio internos no se usan | `nt_port_inbound.go`, `nt_ports.go`, `nt_notification.go` |

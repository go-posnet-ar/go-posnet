# Authorization — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/authorization/README.md) o de los
> comentarios del código, vale lo que está acá.

## 1. Resumen

Authorization orquesta la Saga de autorización de cada pago: recibe la transacción desde
[Terminal Gateway](TERMINAL-GATEWAY.md), consulta la blocklist, pide la evaluación de fraude, llama al
adquirente y publica el resultado al resto del sistema.

La Saga funciona de punta a punta, pero **el adquirente es un mock que aprueba todo**. El único
rechazo posible hoy es el de fraude, y el flujo QR tampoco lo alcanza con los pesos por defecto (ver
[Fraud Detection](FRAUD-DETECTION.md#45-alcance-real-de-reject)). En la práctica, cuando la Saga de ese
flujo se completa, siempre termina aprobada.

| Capacidad | Estado | Detalle |
|---|---|---|
| Inicio de la Saga y coordinación con Fraud Detection | Implementado | Por eventos NATS |
| Máquina de estados de `Transaction` | Implementado | Ver [§4.2](#42-máquina-de-estados) |
| Publicación de resultados | Implementado | Directa a NATS, sin outbox |
| Reversas | Implementado | Contra el mock |
| Consulta de estado | Implementado | HTTP y gRPC, sin autenticación |
| Métricas, trazas, alertas y SLO | Implementado | Ver [§10](#10-observabilidad) |
| Adquirente real (ISO 8583 sobre TCP/TLS) | **Mock** | `MockAcquirerGateway` siempre aprueba |
| Blocklist de tarjetas | **Parcial** | El código está, pero ningún flujo real la activa |
| Bypass por timeout de fraude | **Pendiente** | `BypassFraudCheck` no se invoca |
| Conciliación de `INDETERMINATE` | **Pendiente** | Ningún proceso lee ese estado |
| `ListTerminalTransactions` (gRPC) | **Pendiente** | Devuelve `Unimplemented` |

Las consecuencias de estos pendientes están en [§12](#12-brechas-conocidas).

## 2. Contexto y límites

```
Terminal Gateway ── transaction.received ────────►┌───────────────┐
Terminal Gateway ── transaction.reversal-requested►│               │── fraud.check-requested ──► Fraud Detection
                                                   │ Authorization │◄─ fraud.score-calculated ── Fraud Detection
Adquirente (hoy: mock, en proceso) ◄──────────────►│               │
                                                   └───────┬───────┘
                                                           │ auth.approved / auth.rejected
                                                           │ auth.reversal-completed
                                                           ▼
                                     Terminal Gateway, Settlement, Notification
```

- **Es dueño de:** las transacciones, su resultado y la blocklist.
- **No decide:** el score de fraude. Authorization aplica el campo `decision` del evento y no compara
  el score con ningún umbral; los umbrales viven en Fraud Detection.
- **Entrada principal:** NATS. HTTP y gRPC son solo de consulta y operación.
- **Dato compartido:** Fraud Detection lee `pn_authorization.transactions` directamente para sus
  reglas de historial. Las columnas `terminal_id`, `merchant_id`, `amount_cents`, `state` y
  `created_at` funcionan como un contrato implícito: cambiarlas puede romper ese contexto.

## 3. Estructura del código

| Ruta | Contenido |
|---|---|
| [`cmd/authorization/main.go`](../../cmd/authorization/main.go) | Arranque y apagado |
| [`cmd/authorization/wire.go`](../../cmd/authorization/wire.go) | Wiring de dependencias |
| [`domain/aggregate/`](../../context/authorization/domain/aggregate/) | `Transaction` (aggregate root) |
| [`domain/valueobject/`](../../context/authorization/domain/valueobject/) | Estados, `EntryMode`, `RejectionCode`, `FraudDecision` |
| [`domain/event/`](../../context/authorization/domain/event/) | Eventos de dominio internos |
| [`domain/repository/`](../../context/authorization/domain/repository/) | Puertos de transacciones y blocklist |
| [`domain/service/`](../../context/authorization/domain/service/) | Puertos del adquirente y del publisher |
| [`application/port/`](../../context/authorization/application/port/) | Puertos de entrada y comandos |
| [`application/command/`](../../context/authorization/application/command/) | `AuthorizationHandler` (la Saga) y métricas |
| [`application/query/`](../../context/authorization/application/query/) | Consulta de estado |
| [`infrastructure/nats/`](../../context/authorization/infrastructure/nats/) | Publisher y subscriber |
| [`infrastructure/postgres/`](../../context/authorization/infrastructure/postgres/) | Repositorios y `MockAcquirerGateway` |
| [`infrastructure/http/`](../../context/authorization/infrastructure/http/) | Health checks, métricas y consulta |
| [`infrastructure/grpc/server/`](../../context/authorization/infrastructure/grpc/server/) | Servidor gRPC |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/authorization/`.
Toda la Saga está en
[`authorize_transaction.go`](../../context/authorization/application/command/authorize_transaction.go).

## 4. Modelo de dominio

### 4.1 `Transaction`

| Campo | Descripción |
|---|---|
| `id` | `TransactionID`, generado por Terminal Gateway |
| `terminalID`, `merchantID` | Terminal y comercio |
| `amount` | Monto en centavos y moneda; debe ser positivo |
| `stan` | Entre 1 y 999999 |
| `pan` | Solo últimos 4 dígitos y red |
| `cardToken` | HMAC-SHA256 del PAN en hexadecimal; opcional |
| `entryMode` | `CHIP`, `CONTACTLESS`, `MAGSTRIPE` o `MANUAL` |
| `state` | Estado actual |
| `fraudDecision` | Score (0–100), decisión y reglas activadas |
| `authCode` | 6 caracteres alfanuméricos en mayúscula, si se aprobó |
| `rejectionCode` | Código y fuente, si se rechazó |
| `emvDataBase64`, `iso8583Raw` | Datos opacos para el adquirente; hoy llegan vacíos |
| `receivedAt`, `authorizedAt`, `rejectedAt` | Marcas de tiempo |

Redes aceptadas: `VISA`, `MASTERCARD`, `AMEX`, `CABAL`, `NARANJA` y `UNKNOWN`.

### 4.2 Máquina de estados

```
RECEIVED ──────► FRAUD_CHECKING ──────► PROCESSING ──────► APPROVED ──► REVERSED
    │                  │                    │  │
    │ blocklist        │ fraude: REJECT     │  └─ rechazo ─► REJECTED
    ▼                  ▼                    │
 REJECTED           REJECTED                └─ error ────► INDETERMINATE ──► REVERSED | APPROVED | REJECTED
```

`APPROVED` admite una sola transición, a `REVERSED`. `REJECTED` y `REVERSED` son finales.
`INDETERMINATE` tampoco es final, pero hoy solo puede salir de ese estado mediante una reversa.

**No todos los estados llegan a la base de datos:**

| Estado | ¿Se persiste? | Cuándo |
|---|---|---|
| `RECEIVED` | No | La transacción se guarda recién después de pasar a `FRAUD_CHECKING` |
| `FRAUD_CHECKING` | Sí | Primer estado visible en una consulta |
| `PROCESSING` | No | Solo existe en memoria mientras se llama al adquirente |
| `APPROVED`, `REJECTED`, `REVERSED` | Sí | — |
| `INDETERMINATE` | Sí | El error al guardarlo se ignora |

Una transacción en `FRAUD_CHECKING` puede, por lo tanto, estar esperando el score o haber quedado
trabada durante la llamada al adquirente.

### 4.3 Rechazos

| Fuente | Código | Cuándo | Ordena retener la tarjeta |
|---|---|---|---|
| `FRAUD` | `FRAUD_REJECTED` | El motor devolvió `REJECT` | No |
| `BLOCKLIST` | `CARD_BLOCKED` | La tarjeta está en la blocklist | Sí |
| `ACQUIRER` | ISO 8583 (`05`, `51`, etc.) | El emisor rechazó | Solo `04`, `41` y `43` |
| `VALIDATION` | `UNKNOWN_RESPONSE` | El adquirente respondió sin código | No |
| `TIMEOUT` | `TIMEOUT` | Definido, pero ningún flujo lo genera | No |

Solo los códigos `91` y `96` se publican con `is_retryable=true`. La retención se decide por la
fuente: un rechazo local nunca puede ordenarla, aunque su código coincida con uno de retención.

## 5. La Saga

### 5.1 Flujo aprobado

```
TG             Authorization                     Fraud Detection      Adquirente
│ transaction.received │                                │                  │
├─────────────────────►│ valida y consulta blocklist    │                  │
│                      │ TX: marca evento procesado     │                  │
│                      │ guarda en FRAUD_CHECKING       │                  │
│                      │ fraud.check-requested          │                  │
│                      ├───────────────────────────────►│                  │
│                      │ fraud.score-calculated         │                  │
│                      │◄───────────────────────────────┤                  │
│                      │ marca evento procesado         │                  │
│                      │ Authorize                      │                  │
│                      ├──────────────────────────────────────────────────►│
│                      │◄──────────────────────────────────────────────────┤
│                      │ guarda en APPROVED             │                  │
│◄── auth.approved ────┤ (también Settlement y Notification)               │
```

### 5.2 Decisión de fraude

| `decision` | Resultado |
|---|---|
| `APPROVE` | Continúa hacia el adquirente |
| `REVIEW` | Continúa hacia el adquirente; no hay cola de revisión |
| `REJECT` | Rechaza con `FRAUD_REJECTED`, sin llamar al adquirente |

Un score fuera de 0–100 o una decisión desconocida es un error de validación: el mensaje se descarta y
no se publica ningún resultado.

### 5.3 Blocklist

La blocklist identifica las tarjetas por `card_token`, porque el PAN completo no debe llegar al
backend. No se usan los últimos 4 dígitos porque bloquearían a titulares legítimos.

1. **Consulta:** se hace al recibir la transacción, antes del fraud check. Si la tarjeta figura, se
   rechaza con `CARD_BLOCKED` y `capture_card=true`, sin llamar a Fraud Detection ni al adquirente.
2. **Alta:** cuando el adquirente rechaza con `04`, `41` o `43`, la tarjeta se agrega a la blocklist.
   Solo se conserva el primer bloqueo.
3. **Sin token:** la transacción sigue el flujo normal y la tarjeta no se puede bloquear.
4. **Si falla la consulta:** la Saga continúa sin el control. Es una decisión deliberada: un error de
   base de datos no debe convertirse en una orden de retener una tarjeta.
5. **Si falla el alta:** el rechazo sigue siendo válido y el error queda en el log.

No hay un caso de uso para desbloquear una tarjeta: hoy solo se puede hacer borrando la fila.

### 5.4 Reversa

Al recibir `reversal-requested`:

1. Marca el evento como procesado.
2. Busca la transacción y llama a `acquirer.Reverse`.
3. Pasa a `REVERSED`, guarda y publica `reversal-completed`.

Si el adquirente falla, el mensaje se confirma, la transacción no cambia y solo queda un error en el
log. Se requiere intervención manual.

### 5.5 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| `transaction.received` con datos inválidos | `Term`; no se guarda ni se publica nada | No |
| Error de base de datos antes de marcar el evento | `Nak` y reintento | **Sí** |
| Falla la publicación de `fraud.check-requested` | Se registra y se confirma; queda en `FRAUD_CHECKING` | No |
| Fraud Detection nunca responde | Queda en `FRAUD_CHECKING`; no hay timeout | No |
| Error después de marcar el score como procesado | `Nak`, pero la reentrega se descarta como duplicado | No |
| Error del adquirente | Pasa a `INDETERMINATE`; no se publica ningún evento | No |
| Falla el guardado tras responder el adquirente | `Nak`, y la reentrega se descarta; el resultado solo queda en logs | No |
| Falla la publicación del resultado | Se registra y se confirma; la base queda correcta, pero nadie se entera | No |

En todos los casos sin resultado publicado, Terminal Gateway mantiene la sesión en `PROCESSING` hasta
que su job de limpieza la borra.

## 6. API HTTP

Puerto `HTTP_PORT` (8080 por defecto). **No hay autenticación.**

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; solo verifica Postgres | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `GET /transactions/{id}` | Estado de una transacción | `200` | `400`, `404`, `500` |

`GET /transactions/{id}` responde con nombres en PascalCase, porque el struct no tiene tags JSON:

```json
{
  "TransactionID": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "State": "APPROVED",
  "AuthCode": "482913",
  "RejectionCode": "",
  "RejectionReason": "",
  "AmountCents": 150000,
  "Currency": "ARS",
  "AuthorizedAt": "2026-11-05T15:00:04Z",
  "RejectedAt": ""
}
```

Los errores tienen la forma `{"error": "CODIGO", "message": "..."}`.

## 7. API gRPC

Contrato: [`authorization.proto`](../../pkg/proto/authorization/v1/authorization.proto). El puerto es
9090, sin TLS. Hoy ningún otro servicio del repositorio lo consume.

| RPC | Comportamiento actual |
|---|---|
| `GetTransactionStatus` | Devuelve ID, estado, monto, moneda, código de autorización, datos del rechazo y sus fechas. Errores: `InvalidArgument`, `NotFound` e `Internal` |
| `ListTerminalTransactions` | Devuelve `Unimplemented` |

El contrato define más campos que el servidor no completa: `terminal_id`, `merchant_id`, `card_last4`,
`card_network`, `entry_mode`, `rejection_source`, `fraud_score`, `fraud_decision` y `received_at`. Esos
campos llegan vacíos.

## 8. Eventos NATS

Todos los eventos usan el envelope de [`pkg/events/envelope.go`](../../pkg/events/envelope.go). En los
que publica este contexto, `correlation_id` es el `TransactionID`.

### Publicados

Todos se publican directamente, sin outbox.

| Subject | Stream | Cuándo | Consumidores |
|---|---|---|---|
| `posnet.fraud.check-requested.v1` | `POSNET_FRAUD` | Transacción guardada | Fraud Detection |
| `posnet.auth.approved.v1` | `POSNET_AUTH` | Aprobación del adquirente | Terminal Gateway, Settlement, Notification |
| `posnet.auth.rejected.v1` | `POSNET_AUTH` | Rechazo por cualquier fuente | Terminal Gateway, Notification |
| `posnet.auth.reversal-completed.v1` | `POSNET_AUTH` | Reversa exitosa | Settlement (Terminal Gateway lo recibe y lo ignora) |

Campos relevantes de `auth.rejected`:

| Campo | Notas |
|---|---|
| `rejection_code`, `rejection_reason` | Código y descripción legible |
| `source` | `ACQUIRER`, `FRAUD`, `BLOCKLIST`, `VALIDATION` o `TIMEOUT` |
| `is_retryable` | `true` solo para `91` y `96` |
| `capture_card` | Orden de retener la tarjeta |

Los payloads completos están en [`pkg/events/`](../../pkg/events/).

### Consumidos

Son durables con queue group del mismo nombre, política `DeliverNew` y `AckWait` de 30 s:

| Durable | Subject | Stream | Entregas máx. | Acción |
|---|---|---|---|---|
| `auth-txn-receiver` | `posnet.transaction.received.v1` | `POSNET_TRANSACTIONS` | 5 | `AuthorizeTransaction` |
| `auth-fraud-score-consumer` | `posnet.fraud.score-calculated.v1` | `POSNET_FRAUD` | 5 | `ApplyFraudScore` |
| `auth-reversal-processor` | `posnet.transaction.reversal-requested.v1` | `POSNET_TRANSACTIONS` | 3 | `ProcessReversal` |

Tratamiento de errores:

| Situación | Acción | Efecto |
|---|---|---|
| Envelope o payload inválido | `Term` | No se reintenta |
| `ValidationError` o `ConflictError` | `Term` | No se reintenta |
| Cualquier otro error | `Nak` | Reentrega inmediata, sin backoff |

Un mensaje que agota las entregas no pasa a una DLQ. Para recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md).

## 9. Persistencia

Schema `pn_authorization`. **La fuente real es
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql)**, que solo se ejecuta
al crear una base vacía. `pgutil.Migrate` no hace nada y
[`schema.sql`](../../context/authorization/infrastructure/postgres/schema.sql) está desactualizado:
usa el schema `authorization` y no incluye la blocklist.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `transactions` | Transacciones | PK `id`; índice parcial en `card_token` |
| `processed_events` | Idempotencia de eventos | PK `event_id` |
| `blocked_cards` | Blocklist | PK `card_token`; sin FK, para sobrevivir a purgas de transacciones |

### Aplicar la migración de blocklist

**Una base creada antes de este cambio necesita la migración manualmente.** Sin la columna
`card_token`, fallan tanto `Save` como `FindByID`, y con ellos todas las transacciones:

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000001_card_blocklist.up.sql
```

El script es idempotente. Existe `000001_card_blocklist.down.sql` para deshacerlo, pero **destruye
datos que no se pueden reconstruir**: la blocklist y los `card_token` de las transacciones. En un
entorno con datos reales, exportarlos antes de ejecutarlo.

### Garantías actuales

- **Publicación:** *at-most-once* en la práctica. No hay outbox: si NATS falla después de guardar, el
  evento se pierde. La reentrega no ayuda, porque el mensaje se confirma igual.
- **Idempotencia:** el `event_id` se registra en `processed_events`. En `ApplyFraudScore` y
  `ProcessReversal`, ese registro se confirma **antes** de hacer el trabajo, para evitar llamar dos
  veces al adquirente. El costo es que un fallo posterior no se reintenta (ver
  [§12](#12-brechas-conocidas)).

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene HTTP (hasta 10 s), drena y cierra NATS, y cierra el pool de
Postgres. El servidor gRPC no se detiene de forma ordenada.

## 10. Observabilidad

### Métricas

| Métrica | Tipo | Labels |
|---|---|---|
| `posnet_transactions_received_total` | Counter | — |
| `posnet_transactions_approved_total` | Counter | — |
| `posnet_transactions_rejected_total` | Counter | `rejection_code`, `source` |
| `posnet_acquirer_errors_total` | Counter | `reason` |
| `posnet_acquirer_request_duration_seconds` | Histogram | `result` |
| `posnet_saga_duration_seconds` | Histogram | `outcome` |

`posnet_saga_duration_seconds` solo registra sagas que terminan en aprobación o rechazo. Las trabadas
y las `INDETERMINATE` no aparecen en esta métrica.

También se exponen métricas del pool de Postgres, de mensajes NATS y de duplicados descartados
(`posnet_idempotency_duplicates_total`).

### SLO y alertas

Reglas en [`deployments/docker/prometheus/rules/`](../../deployments/docker/prometheus/rules/).

| SLO | Objetivo | Alertas de burn rate |
|---|---|---|
| Latencia de la Saga | 95 % por debajo de 3 s | `SagaLatencySLOFastBurn`, `SagaLatencySLOSlowBurn` |
| Éxito de transacción | 99 % sin error de sistema | `TxSuccessSLOFastBurn`, `TxSuccessSLOSlowBurn` |

| Alerta | Severidad | Condición |
|---|---|---|
| `SagaCompletaDetenida` | Crítica | Entran transacciones y ninguna se aprueba durante 2 min |
| `ErrorRateAlta` | Crítica | Rechazos por encima del 10 % de las recibidas durante 5 min |
| `LatenciaSagaAlta` | Warning | p95 de la Saga por encima de 3 s durante 5 min |
| `DuplicadosNATSAltos` | Warning | Duplicados por encima de 0,05/s durante 5 min |

`ErrorRateAlta` incluye rechazos de negocio válidos, como fondos insuficientes.

**Dashboards de Grafana:** `posnet-saga-overview`, `posnet-bc-detail` y `posnet-slo`.
**Runbook:** [incident-response](../runbooks/incident-response.md).

### Trazas y logs

Cada comando abre un span (`command.AuthorizeTransaction`, `command.ApplyFraudScore`,
`command.callAcquirer`, etc.). Para seguir un pago en los logs, buscar por `transaction_id`. Los logs
más relevantes para detectar pérdidas son `failed to publish` y `acquirer error`, porque ninguna
métrica cuenta las publicaciones fallidas.

## 11. Configuración y ejecución

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `NATS_URL` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `HTTP_PORT` | `8080` | Sí |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `20`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-authorization` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `GRPC_PORT` | `9090` | **No**: el puerto está fijo en 9090 |
| `ACQUIRER_HOST`, `ACQUIRER_PORT` | `localhost`, `9100` | **No**: se usa el mock |
| `ACQUIRER_TIMEOUT_SECONDS` | `30` | **No**: se usa el mock |
| `ACQUIRER_TLS_CERT`, `ACQUIRER_TLS_KEY`, `ACQUIRER_TLS_CA` | Vacías | **No**: se usa el mock |
| `MIGRATIONS_DIR` | `migrations/authorization` | **No**: `Migrate` no hace nada, y esa carpeta no existe |

La variable del README `ACQUIRER_TIMEOUT` no existe. `.env.docker` apunta al servicio `acquirer-mock`
de Compose, pero Authorization no se conecta con él: usa `MockAcquirerGateway`, dentro del propio
proceso.

### Levantar el servicio

Authorization necesita Fraud Detection para completar la Saga:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build \
  terminal-gateway authorization fraud-detection
```

Sin Docker, con Postgres y NATS disponibles:

```bash
POSTGRES_DSN='postgres://...' NATS_URL='nats://localhost:4222' go run ./cmd/authorization
```

El comando `go run ./context/authorization/...` del README no es válido: el entrypoint está en `cmd/`.
Los templates de Helm de este servicio están vacíos.

### Probar una transacción

Crear y pagar una sesión como se indica en [Terminal Gateway](TERMINAL-GATEWAY.md#probar-un-pago), y
luego consultar con el mismo ID:

```bash
curl -s localhost:8080/transactions/$ID | jq
```

### Tests

```bash
# Unitarios
go test ./context/authorization/... ./cmd/authorization/...

# Integración: wiring completo con Postgres y NATS en Testcontainers; requiere Docker
go test -tags=integration -run Integration ./cmd/authorization/ -v

# Carga
locust -f posnet-load-tests/authorization/authorization_locust.py \
  --headless -u 10 -r 2 --run-time 2m --host http://localhost:8080
```

Para la carga, ver también [`LOCUST.md`](../../LOCUST.md).

## 12. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Resolver antes de conectar un adquirente real

Hoy el mock oculta estas fallas: nunca devuelve errores ni rechazos.

| Brecha | Consecuencia | Dónde |
|---|---|---|
| El evento de score se marca como procesado antes de hacer el trabajo | Tras cualquier error o reinicio posterior, la reentrega se descarta. `MaxDeliver: 5` no protege este tramo. Un pago aprobado por el emisor puede quedar sin registro ni evento | `ApplyFraudScore`, `callAcquirer` |
| `PROCESSING` no se guarda antes de llamar al adquirente | Una caída durante la llamada deja `FRAUD_CHECKING`, sin rastro de que se haya enviado la autorización | `callAcquirer` |
| `INDETERMINATE` no publica eventos ni tiene conciliación | Los comentarios mencionan una conciliación nocturna que no existe. El cliente no recibe respuesta y el resultado real queda sin resolver | `callAcquirer` |
| Se llama a `Reverse` antes de validar el estado | Se puede revertir en el adquirente una transacción rechazada o en curso; recién después falla la transición | `ProcessReversal` |
| Una reversa fallida no se informa | La transacción sigue `APPROVED`, solo queda un log, y Terminal Gateway ya respondió `202` | `ProcessReversal` |
| El guardado del `INDETERMINATE` ignora errores | `_ = h.repo.Save(...)`: si falla, el estado se pierde sin registro de ese fallo | `callAcquirer` |
| La llamada corre dentro del handler de NATS | Cada subscriber procesa de a un mensaje; un adquirente lento frena toda la cola. Además, el timeout previsto (30 s) iguala al `AckWait` (30 s) | `subscriber.go`, `callAcquirer` |

### Saga

| Brecha | Consecuencia | Dónde |
|---|---|---|
| No existe el bypass ni el timeout de fraude | El log dice `fraud bypass will apply` y la documentación del evento menciona 500 ms, pero `BypassFraudCheck` no se invoca. Si Fraud Detection no responde, la transacción queda trabada para siempre | `AuthorizeTransaction` |
| Resultados publicados sin outbox | Si falla la publicación de una aprobación, Settlement no la liquida y el cajero no la ve, aunque figure `APPROVED` | `callAcquirer`, `publishRejection` |
| Una transacción inválida no recibe respuesta | Terminal Gateway no valida `card_network` ni `card_last4`: `/pay` responde `202`, Authorization descarta el mensaje y la sesión queda en `PROCESSING` hasta su limpieza | `AuthorizeTransaction` |
| Un segundo evento con el mismo `transaction_id` reinicia la transacción | `Save` es un UPSERT: un `event_id` distinto la devuelve a `FRAUD_CHECKING` y puede cobrarse otra vez. Hoy Terminal Gateway no genera este caso, pero Authorization no se defiende | `AuthorizeTransaction`, `TransactionRepo.Save` |
| `Save` queda fuera de la transacción de idempotencia | Usa el pool en lugar de `dbTx`, aunque el comentario diga lo contrario. Si falla el commit, queda guardado sin el evento marcado y la reentrega reprocesa | `AuthorizeTransaction`, `rejectBlockedCard` |
| Notification no consume `reversal-completed` | El contrato del evento prevé un comprobante de anulación, pero el subscriber de Notification no se suscribe, así que no se genera | `nt_nats_subscriber.go` |

### Blocklist y datos de tarjeta

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Ningún flujo real activa la blocklist | Terminal Gateway siempre envía `card_token` vacío, y el mock nunca devuelve `04`, `41` ni `43`. Está probada solo con tests | `tg_qr_handler.go`, `mock_acquirer.go` |
| Sin caso de uso para desbloquear | Un bloqueo es permanente, salvo que se borre la fila a mano | `BlockedCardRepository` |
| `iso8583_raw` se guarda tal como llega | Hoy llega vacío, pero un mensaje ISO 8583 real suele incluir el PAN, y la invariante del sistema es que no llegue al backend. Definir qué se guarda antes de procesar mensajes reales | `TransactionRepo.Save` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Sin migraciones automáticas | Cambios como `card_token` requieren aplicarse a mano; si no, fallan todas las transacciones (ver [§9](#9-persistencia)) | `pkg/pgutil/migrate.go` |
| `fraud_rules_hit` nunca se guarda | `Save` no incluye la columna: las reglas que motivaron una decisión se pierden para auditoría | `TransactionRepo.Save` |
| Faltan índices en `01-init.sql` | Solo está el de `card_token`. Las consultas de historial de Fraud Detection, por terminal y fecha, recorren la tabla | `01-init.sql` |
| Acoplamiento con Fraud Detection | Lee las tablas de este contexto directamente, en contra de lo que indica la documentación de arquitectura | `fd_postgres_repository.go` |
| gRPC sin TLS, sin autenticación y sin apagado ordenado | Si falla el listen, solo se registra el error y el proceso sigue. El puerto ignora `GRPC_PORT` | `server.go`, `wire.go` |
| API HTTP sin autenticación | Cualquiera con acceso de red y un ID puede leer el estado y el monto de una transacción | `router.go` |
| `/readyz` no verifica NATS | Una instancia sin NATS sigue figurando como lista | `handler.go` |
| La fuente del rechazo no se reconstruye bien | `BLOCKLIST` y `VALIDATION` se leen como `ACQUIRER`. Hoy no tiene efecto, porque un rechazo no se vuelve a guardar | `reconstitute.go` |
| Código sin uso | `FindBySTAN`, `UpdateState`, `ExistsByID`, `parseRFC3339` y los eventos de dominio internos no se usan | `repository.go`, `server.go`, `transaction.go` |

# Settlement — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/settlement/README.md) o de los
> comentarios del código, vale lo que está acá. Para operar un incidente, ver el
> [runbook de cierre de lote](../runbooks/batch-close-failure.md).

## 1. Resumen

Settlement agrupa las transacciones aprobadas en lotes diarios por terminal, los cierra cuando el
terminal lo pide y compara los totales del terminal con los del backend. El siguiente paso previsto es
enviar la remesa a un procesador externo y registrar la liquidación.

**Solo la primera mitad funciona.** Un lote puede abrirse, acumular compras y cerrarse, pero no hay
procesador ni liquidación, y además hay cuatro fallas que afectan directamente el resultado:

- **Un cierre sin discrepancias hace caer el servicio**, porque el procesador es `nil`.
- **Las reversas no descuentan nada** del lote.
- **Una aprobación posterior al cierre del día no entra en ningún lote.**
- **Un lote en `DISPUTED` no tiene salida** sin intervenir la base de datos.

| Capacidad | Estado | Detalle |
|---|---|---|
| Apertura de lotes y registro de compras | Implementado | Un lote por terminal y fecha UTC |
| Cierre y detección de discrepancias | Implementado | Solo por pedido del terminal |
| Consulta de lotes | Implementado | HTTP, sin autenticación |
| Publicación de `batch-closed` | Implementado | Directa a NATS, sin outbox |
| Registro de reversas | **Roto** | Se confirma, pero no se persiste |
| Cierre forzado por soporte | **Parcial** | Registra discrepancias falsas y no publica eventos |
| Envío al procesador externo | **Pendiente** | `processor` es `nil`: provoca un panic |
| Reenvío manual (`resubmit`) | **Pendiente** | Siempre responde `500` por el mismo motivo |
| Liquidación (`SETTLED`) y comisión (MDR) | **Pendiente** | Nada invoca `MarkSettled` ni calcula el neto |
| Publicación de `settlement.completed` | **Pendiente** | Existe el método, pero no se invoca |
| Cierre automático de lotes | **Pendiente** | El job solo escribe un log |

Las consecuencias de estos pendientes están en [§11](#11-brechas-conocidas).

## 2. Contexto y límites

```
Authorization ── auth.approved ────────────►┌────────────┐
Authorization ── auth.reversal-completed ──►│ Settlement │── settlement.batch-closed ──► Notification
Terminal Gateway ── transaction.batch-close►│            │
                                            └─────┬──────┘
                                                  ┆ Submit (no implementado)
                                                  ▼
                                        Procesador externo (Visa/MC)
```

- **Es dueño de:** los lotes, sus transacciones y la conciliación de totales.
- **No decide:** si una transacción se aprueba; solo registra las que Authorization aprobó.
- **Depende de los eventos:** no consulta otras bases. Un evento perdido en Authorization significa una
  transacción ausente en el lote.

## 3. Estructura del código

| Ruta | Contenido |
|---|---|
| [`cmd/settlement/main.go`](../../cmd/settlement/main.go) | Arranque y apagado |
| [`cmd/settlement/st_wire.go`](../../cmd/settlement/st_wire.go) | Wiring y job de cierre |
| [`domain/aggregate/`](../../context/settlement/domain/aggregate/) | `SettlementBatch` (aggregate root) |
| [`domain/entity/`](../../context/settlement/domain/entity/) | `BatchTransaction` |
| [`domain/valueobject/`](../../context/settlement/domain/valueobject/) | `BatchState`, `BatchSummary`, tipos de transacción |
| [`domain/repository/`](../../context/settlement/domain/repository/) | Puerto de lotes |
| [`domain/service/`](../../context/settlement/domain/service/) | Puertos del publisher y del procesador |
| [`application/command/`](../../context/settlement/application/command/) | `BatchHandler`, `AdminHandler` y métricas |
| [`application/query/`](../../context/settlement/application/query/) | Consulta de lotes |
| [`infrastructure/nats/`](../../context/settlement/infrastructure/nats/) | Publisher y subscriber |
| [`infrastructure/postgres/`](../../context/settlement/infrastructure/postgres/) | Repositorio |
| [`infrastructure/http/`](../../context/settlement/infrastructure/http/) | Health checks, consulta y administración |
| [`infrastructure/grpc/server/`](../../context/settlement/infrastructure/grpc/server/) | Servidor sin servicios |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/settlement/`.

## 4. Modelo de dominio

### 4.1 `SettlementBatch`

| Campo | Descripción |
|---|---|
| `id` | UUID del lote |
| `terminalID`, `merchantID` | Terminal y comercio |
| `batchDate` | Fecha **UTC** de la primera transacción del lote |
| `state` | Estado actual |
| `currency` | Moneda de la primera transacción; se aplica a todo el lote |
| `transactions` | Transacciones incluidas |
| `summary` | Totales; `nil` hasta el cierre |
| `discrepancies` | Resultado de la conciliación |
| `createdAt`, `closedAt`, `submittedAt`, `settledAt` | Marcas de tiempo |

**Solo puede existir un lote por terminal y fecha, en cualquier estado.** La restricción es
`UNIQUE (terminal_id, batch_date)`, aunque el comentario del schema hable de un lote *abierto*. Esta
diferencia causa la pérdida de aprobaciones descrita en [§5.3](#53-aprobación-después-del-cierre).

### 4.2 Máquina de estados

```
OPEN ──► PENDING_CLOSE ──► CLOSED ──► SUBMITTED ──► SETTLED
              │               │            │
              │               ▼            ▼
              └─────────► DISPUTED ◄───────┘
                              │
                              └──► SUBMITTED
```

Qué transiciones ocurren realmente:

| Transición | ¿Se usa? |
|---|---|
| `OPEN` → `PENDING_CLOSE` → `CLOSED` | Sí, en un solo paso: `PENDING_CLOSE` nunca se guarda |
| `CLOSED` → `DISPUTED` | Sí, cuando hay discrepancias |
| `CLOSED` → `SUBMITTED` | No: el procesador es `nil` |
| `SUBMITTED` → `SETTLED` | No: `MarkSettled` no se invoca |
| `PENDING_CLOSE` o `SUBMITTED` → `DISPUTED` | No |
| `DISPUTED` → `SUBMITTED` | No: `resubmit` exige `CLOSED` |

`IsTerminal()` considera final a `DISPUTED`, aunque la máquina de estados le permita una salida.

### 4.3 `BatchTransaction`

| Tipo | Uso real |
|---|---|
| `PURCHASE` | Compra aprobada |
| `REVERSAL` | Se crea en memoria, pero no llega a guardarse (ver [§5.2](#52-reversa)) |
| `OFFLINE` | Definido, pero ningún flujo lo genera |

`transaction_id` es único en la tabla: una transacción no puede estar en dos lotes ni contarse dos
veces, incluso si la aprobación llega con otro `event_id`.

### 4.4 Conciliación

Al cerrar, el backend calcula sus totales y los compara con los que informó el terminal:

```
cantidad = compras + reversas
monto    = monto de compras − monto de reversas
```

| Resultado | `discrepancies` |
|---|---|
| Cantidad y monto coinciden | `0` |
| La cantidad difiere | Diferencia absoluta de cantidades |
| Solo el monto difiere | `1` |

El valor no mide la diferencia de dinero: solo indica cuántas transacciones faltan o sobran.

## 5. Flujos

### 5.1 Registro de una compra

Al recibir `auth.approved`:

1. Valida los IDs y toma la fecha UTC de `authorized_at`.
2. Marca el evento en `processed_events` dentro de una transacción.
3. Busca el lote `OPEN` del terminal para esa fecha o lo crea, en una transacción `SERIALIZABLE`.
4. Agrega la transacción y guarda el lote, en otra transacción.

El guardado queda fuera de la transacción de idempotencia. Si el commit final falla, la reentrega
reprocesa sin duplicar, gracias a `ON CONFLICT (transaction_id) DO NOTHING`.

### 5.2 Reversa

**Hoy una reversa no modifica el lote:**

1. Al recibir `auth.reversal-completed`, se crea una fila `REVERSAL` con el `transaction_id` de la
   compra original.
2. Como esa compra ya está en la tabla, `UNIQUE (transaction_id)` y `ON CONFLICT DO NOTHING` descartan
   la fila sin error.
3. El mensaje se confirma y `posnet_settlement_reversals_total` aumenta, como si hubiera funcionado.

El efecto en la conciliación depende de lo que informe el terminal:

| El terminal informa | Resultado |
|---|---|
| El total neto, descontando la reversa | `DISPUTED`: no coincide con el backend |
| El total bruto, sin descontarla | El lote cierra sin discrepancias e incluye la venta anulada |

Aunque la fila se guardara, tampoco sería correcta: `RemoveTransaction` usa **un monto fijo de 1
centavo** e ignora el monto de la reversa. Los tests no lo detectan porque validan el aggregate en
memoria y usan una base simulada.

Si la reversa llega en otra fecha UTC, además crea un lote `OPEN` vacío, que nadie cierra.

### 5.3 Aprobación después del cierre

Si el terminal cierra su lote y sigue operando durante la misma fecha UTC:

1. `FindOrCreateOpen` no encuentra un lote `OPEN`, porque el del día está cerrado.
2. Intenta crear uno nuevo, pero `UNIQUE (terminal_id, batch_date)` lo impide.
3. Vuelve a buscar un lote `OPEN`, no lo encuentra y devuelve un error.
4. El mensaje se reintenta de inmediato hasta agotar sus 5 entregas, y deja de procesarse.

**La venta queda aprobada y nunca entra en un lote.** Lo mismo ocurre con una reversa posterior al
cierre. Los mensajes siguen en el stream y se pueden recuperar con el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md), pero volverán a fallar mientras el lote del
día no esté `OPEN`.

### 5.4 Cierre de lote

Terminal Gateway publica `transaction.batch-close`, y `ProcessBatchClose` hace lo siguiente:

```
Busca el lote OPEN del terminal para batch_date
   │
   ├─ No existe ─► WARN y confirma (sin respuesta al terminal)
   │
   ▼
Calcula totales y discrepancias
   │
   ▼
TX: marca el evento ─ guarda el lote en CLOSED
   │
   ▼
Publica settlement.batch-closed (si falla, solo log)
   │
   ├─ discrepancies > 0 ─► guarda DISPUTED ─► fin
   │
   └─ discrepancies = 0 ─► processor.Submit ─► PANIC: el proceso se cae
```

**El caso exitoso es el que falla.** `processor` es una interfaz `nil`, a pesar de que el comentario de
`st_wire.go` dice que es un no-op, y `submitBatch` no lo verifica. El handler de NATS no tiene `recover`
ni lo tiene `nats.go`, de modo que el panic termina el proceso. Al reiniciar:

- El lote ya está guardado en `CLOSED` y el evento `batch-closed` ya se publicó.
- La reentrega no encuentra un lote `OPEN`, así que confirma el mensaje sin hacer nada.
- El lote queda en `CLOSED` y no hay forma de avanzar, porque `resubmit` tampoco funciona.

### 5.5 Desfase por fecha UTC

Los lotes se agrupan por fecha UTC, y Terminal Gateway siempre publica la fecha UTC actual, ignorando
el `batch_date` del request. Para un terminal en UTC-3:

- Las ventas posteriores a las 21:00 caen en el lote del día siguiente.
- Un cierre después de las 21:00 solo ve ese lote nuevo: el lote del día queda `OPEN` para siempre, y
  los totales del terminal no coinciden.

### 5.6 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| IDs inválidos | `Term`; no se reintenta | No |
| Error de base de datos al registrar | `Nak` y reintento | **Sí** |
| Conflicto de serialización (`40001`) | `Nak` y reentrega inmediata, hasta 5 veces | Generalmente |
| Aprobación con lote del día ya cerrado | `Nak` hasta agotar entregas | No |
| Se pierde `auth.approved` en Authorization | La transacción falta en el lote; el cierre da `DISPUTED` | No |
| Falla la publicación de `batch-closed` | Solo log; Notification no se entera | No |
| Falla el guardado de `DISPUTED` | `Nak`; la reentrega no hace nada y queda `CLOSED` con discrepancias | No |
| Cierre sin discrepancias | Panic y reinicio; queda `CLOSED` | No |
| El terminal nunca pide el cierre | El lote queda `OPEN` para siempre | No |

## 6. API HTTP

Puerto `HTTP_PORT`: 8083 por defecto en el código y **8084 en Docker Compose**. **No hay autenticación**,
ni siquiera en las operaciones de escritura.

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; solo verifica Postgres | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `GET /batches/{id}` | Consultar un lote | `200` | `404`, `500` |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Lotes de un comercio; `date` es obligatorio | `200` | `400`, `500` |
| `POST /batches/{id}/force-close` | Forzar el cierre | `200` | `400`, `404`, `500` |
| `POST /batches/{id}/resubmit` | Reintentar el envío | — | `400`, `404`, `500` |

Un ID que no sea un UUID responde `500`, no `400`. Los errores tienen la forma
`{"error": "CODIGO", "message": "..."}`.

### Consultar un lote

Responde con nombres en PascalCase, porque el struct no tiene tags JSON:

```json
{
  "ID": "3b1f6d2a-7c44-4e0b-9a55-1e2f3c4d5e6f",
  "TerminalID": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "MerchantID": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "BatchDate": "2026-11-05",
  "State": "DISPUTED",
  "Currency": "ARS",
  "TotalCount": 42,
  "TotalAmount": 1250000,
  "Discrepancies": 1,
  "ClosedAt": "2026-11-05T20:00:04Z",
  "SubmittedAt": "",
  "SettledAt": ""
}
```

**Un lote `OPEN` muestra `TotalCount` y `TotalAmount` en 0**, aunque tenga transacciones, porque esos
campos se calculan al cerrar. La respuesta no incluye el desglose de compras y reversas.

`GET /merchants/{merchant_id}/batches` devuelve `{"batches": [...], "count": N}`, con la misma forma,
pero sin completar `SubmittedAt` ni `SettledAt`.

### Forzar el cierre

```json
{ "operator_id": "soporte-juan" }
```

Responde `{"status": "closed"}`. Tiene varias limitaciones:

- Solo funciona sobre lotes `OPEN`. En cualquier otro estado responde `500`, no `400`.
- Cierra comparando contra 0, por lo que **cualquier lote con transacciones queda con discrepancias
  falsas**, aunque su estado sea `CLOSED`.
- No publica `batch-closed` ni intenta el envío.
- `operator_id` es obligatorio, pero **no se guarda ni se registra en ningún log**.

### Reenviar al procesador

Lleva el mismo body. Si el lote no está en `CLOSED`, responde `400`. Si lo está, llama al procesador
`nil`: el panic lo captura el middleware HTTP y responde `500`. Nunca puede completarse.

## 7. gRPC

Hay un servidor en el puerto 9093 sin servicios registrados, y el contexto no tiene un `.proto`. Es un
placeholder: cualquier llamada devuelve `Unimplemented`. El puerto está fijo y no respeta `GRPC_PORT`.

## 8. Eventos NATS

Todos los eventos usan el envelope de [`pkg/events/envelope.go`](../../pkg/events/envelope.go).

### Consumidos

Son durables con queue group del mismo nombre, hasta 5 entregas, política `DeliverNew` y `AckWait` de
30 s:

| Durable | Subject | Stream | Acción |
|---|---|---|---|
| `settlement-auth-consumer` | `posnet.auth.approved.v1` | `POSNET_AUTH` | `RegisterApproval` |
| `settlement-reversal-consumer` | `posnet.auth.reversal-completed.v1` | `POSNET_AUTH` | `RegisterReversal` |
| `settlement-batch-consumer` | `posnet.transaction.batch-close.v1` | `POSNET_TRANSACTIONS` | `ProcessBatchClose` |

| Situación | Acción | Efecto |
|---|---|---|
| Envelope o payload inválido | `Term` | No se reintenta |
| `ValidationError` o `ConflictError` | `Term` | No se reintenta |
| Cualquier otro error | `Nak` | Reentrega inmediata, sin backoff |

Un mensaje que agota las entregas no pasa a una DLQ. Para recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md).

### Publicados

| Subject | Stream | ¿Se publica? | Consumidor |
|---|---|---|---|
| `posnet.settlement.batch-closed.v1` | `POSNET_SETTLEMENT` | Sí, sin outbox | Notification |
| `posnet.settlement.completed.v1` | `POSNET_SETTLEMENT` | **No**: nada lo invoca | Ninguno |

Payload de `batch-closed`:

| Campo | Notas |
|---|---|
| `batch_id`, `terminal_id`, `merchant_id` | `batch_id` también es el `correlation_id` |
| `batch_date` | `YYYY-MM-DD` |
| `total_count`, `total_amount`, `currency` | Totales del backend |
| `discrepancies` | Permite distinguir un cierre con diferencias |
| `closed_at` | RFC3339 UTC |

## 9. Persistencia

Schema `settlement`. **La fuente real es
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql):** `pgutil.Migrate` no
hace nada, `migrations/settlement/` está vacío y
[`st_schema.sql`](../../context/settlement/infrastructure/postgres/st_schema.sql) no se ejecuta.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `settlement_batches` | Lotes | PK `id`; `UNIQUE (terminal_id, batch_date)` |
| `batch_transactions` | Transacciones del lote | PK `id`; `transaction_id` único; FK `batch_id` |
| `processed_events` | Idempotencia | PK `event_id` |

Diferencia entre ambos archivos: `st_schema.sql` define tres índices
(`idx_settlement_batches_merchant_date`, `idx_settlement_batches_state` e `idx_batch_transactions_batch`)
que `01-init.sql` no crea. **`loadTransactions` recorre `batch_transactions` por `batch_id` en cada
aprobación, sin índice.**

### Costo por aprobación

Cada aprobación carga todas las transacciones del lote y las vuelve a insertar con `ON CONFLICT DO
NOTHING`. El costo por evento crece con el tamaño del lote, y con él el costo acumulado del día.

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene HTTP (hasta 10 s), drena y cierra NATS, y cierra el pool de Postgres.
El servidor gRPC no se detiene de forma ordenada.

## 10. Observabilidad y operación

### Métricas

| Métrica | Tipo | Labels | Significado |
|---|---|---|---|
| `posnet_settlement_auth_approved_total` | Counter | — | Aprobaciones registradas |
| `posnet_settlement_reversals_total` | Counter | — | Reversas procesadas; **aumenta aunque no se guarden** |
| `posnet_settlement_batches_total` | Counter | `currency`, `state` | Cierres; `state` siempre es `CLOSED`, porque se registra antes de marcar la disputa |
| `posnet_settlement_batch_duration_seconds` | Histogram | — | Duración del cierre |

También se exponen métricas del pool de Postgres y de mensajes NATS.

**No hay alertas sobre lotes.** Ninguna regla detecta lotes en `DISPUTED`, `CLOSED` trabados u `OPEN`
vencidos, y `posnet_settlement_batches_total` no permite contar las disputas. `PostgresDeadlock` es la
única alerta que menciona este contexto. **Dashboard:** `posnet-bc-detail` muestra la duración del
cierre.

### Para detectar problemas

```sql
-- Lotes que requieren atención
SELECT id, terminal_id, batch_date, state, discrepancies, closed_at
FROM settlement.settlement_batches
WHERE state IN ('DISPUTED', 'CLOSED')
   OR (state = 'OPEN' AND batch_date < CURRENT_DATE)
ORDER BY batch_date DESC;
```

En los logs, buscar `reload after conflict` para detectar aprobaciones rechazadas tras un cierre, y
`no open batch found` para cierres sin efecto. Un reinicio del servicio cerca de un cierre puede
deberse al panic descrito en [§5.4](#54-cierre-de-lote).

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `NATS_URL` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `HTTP_PORT` | `8083` | Sí; Docker Compose usa `8084` |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `15`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-settlement` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `SETTLEMENT_MDR_PERCENT` | `2.5` | **No**: se valida y se registra, pero no se aplica a ningún cálculo |
| `SETTLEMENT_BATCH_CLOSE_HOUR` | `23` | **No**: el job usa 23 fijo, y solo escribe un log |
| `SETTLEMENT_SUBMIT_RETRIES` | `3` | **No**: no hay procesador |
| `SETTLEMENT_SUBMIT_TIMEOUT` | `30s` | **No**: no hay procesador |
| `GRPC_PORT` | `9093` | **No**: el puerto está fijo en 9093 |
| `MIGRATIONS_DIR` | `migrations/settlement` | **No**: `Migrate` no hace nada |

Un `SETTLEMENT_MDR_PERCENT` fuera de 0–10 o un `SETTLEMENT_BATCH_CLOSE_HOUR` fuera de 0–23 impiden el
arranque, aunque esos valores no se usen.

### Levantar y probar

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build \
  terminal-gateway authorization fraud-detection settlement
```

Sin Docker, con Postgres y NATS disponibles:

```bash
POSTGRES_DSN='postgres://...' NATS_URL='nats://localhost:4222' go run ./cmd/settlement
```

El comando `go run ./context/settlement/...` del README no es válido: el entrypoint está en `cmd/`. Los
templates de Helm de este servicio están vacíos.

Para generar un lote, crear y pagar una sesión como se indica en
[Terminal Gateway](TERMINAL-GATEWAY.md#probar-un-pago), y luego consultar:

```bash
MERCHANT=b2c3d4e5-f6a7-8901-bcde-f12345678901
curl -s "localhost:8084/merchants/$MERCHANT/batches?date=$(date -u +%F)" | jq
```

Para probar el cierre, informar totales que **no** coincidan, de modo que el lote pase a `DISPUTED`. Con
totales correctos, hoy el servicio se cae (ver [§5.4](#54-cierre-de-lote)):

```bash
curl -s localhost:8081/batch-close -H 'Content-Type: application/json' -d '{
  "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "terminal_count": 999,
  "terminal_amount": 1,
  "currency": "ARS"
}'
```

Ese lote ya no se puede reutilizar: para seguir probando el mismo día, hay que borrarlo a mano o usar
otro terminal.

### Tests

```bash
go test ./context/settlement/...

# Carga: requiere el servicio y NATS activos
locust -f posnet-load-tests/settlement/settlement_locust.py \
  --headless -u 10 -r 2 --run-time 2m --host http://localhost:8084
```

Los tests usan una base simulada y un procesador falso, por lo que **no detectan** el panic ni la
pérdida de reversas. No hay tests de integración contra Postgres y NATS reales, ni tests del wiring.
Para la carga, ver también [`LOCUST.md`](../../LOCUST.md).

## 11. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Montos incorrectos o pérdida de datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Las reversas no se guardan | Una venta anulada puede liquidarse como si siguiera vigente, mientras la métrica indica que se procesó (ver [§5.2](#52-reversa)) | `SettlementBatchRepo.Save`, `batch_transactions` |
| La reversa usa un monto fijo de 1 centavo | Corregir solo la persistencia no alcanza: se descontaría 1 centavo | `RemoveTransaction` |
| Las aprobaciones posteriores al cierre se pierden | La venta queda aprobada y fuera de todo lote. Es parte de la operación normal si el terminal sigue vendiendo (ver [§5.3](#53-aprobación-después-del-cierre)) | `FindOrCreateOpen`, `UNIQUE (terminal_id, batch_date)` |
| El lote mezcla monedas | Una transacción en USD se suma a un lote en ARS como si fuera la misma moneda; `cmd.Currency` se ignora tras crear el lote | `RegisterApproval`, `AddTransaction` |
| Un cierre puede sobrescribirse | Si una aprobación ya cargó el lote `OPEN` cuando otro handler lo cierra, su `Save` lo devuelve a `OPEN` y borra los totales. No hay bloqueo entre la carga y el guardado | `Save`, `ProcessBatchClose` |
| El agrupamiento usa fecha UTC | Los lotes no coinciden con el día local del comercio (ver [§5.5](#55-desfase-por-fecha-utc)) | `RegisterApproval`, `PublishBatchCloseRequested` |

### Ciclo de vida del lote

| Brecha | Consecuencia | Dónde |
|---|---|---|
| El procesador es `nil` | Todo cierre sin discrepancias provoca un panic y un reinicio del servicio. Debería al menos fallar con un error controlado o ser un no-op real, como dice el comentario | `st_wire.go`, `submitBatch` |
| `DISPUTED` no tiene salida | Ni `resubmit` ni `force-close` lo aceptan, y tampoco hay reapertura o recálculo: requiere SQL manual | `AdminHandler` |
| `CLOSED` no tiene salida real | `resubmit` siempre falla mientras no exista el procesador | `ResubmitBatch` |
| No hay liquidación | `SUBMITTED`, `SETTLED`, el MDR, el archivo de remesa y `settlement.completed` no están implementados | `MarkSettled`, `PublishSettlementCompleted` |
| No hay cierre automático | Un terminal que no pide el cierre deja su lote `OPEN` para siempre. El job solo escribe un log | `runBatchCloser` |
| `resubmit` no revisa discrepancias | Un lote `CLOSED` con discrepancias, como los de `force-close`, se enviaría igual cuando exista el procesador | `ResubmitBatch` |

### Cierre forzado y seguridad

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Operaciones sin autenticación | Con acceso de red, cualquiera puede cerrar el lote de cualquier terminal; un cierre hecho así también hace perder las aprobaciones posteriores | `st_http_router.go` |
| `force-close` compara contra 0 | Todo lote con transacciones queda con discrepancias falsas | `ForceClose` |
| `operator_id` no se audita | Es obligatorio, pero `ForceClose` no lo guarda ni lo registra. `ResubmitBatch` solo lo registra en un log si tiene éxito | `AdminHandler` |
| `force-close` no notifica | No se publica `batch-closed`, así que Notification no se entera | `ForceClose` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Sin alertas de lotes | Los lotes trabados o en disputa solo se descubren consultando la base | Prometheus |
| Las disputas no se ven en las métricas | `state` siempre vale `CLOSED` en `posnet_settlement_batches_total` | `ProcessBatchClose` |
| Faltan índices y hay costo creciente por aprobación | Cada evento recorre las transacciones del lote sin índice | `01-init.sql`, `Save`, `loadTransactions` |
| `batch-closed` se publica sin outbox | Si falla, Notification no se entera y nada lo reintenta | `ProcessBatchClose` |
| Un cierre sin lote no recibe respuesta | Terminal Gateway ya respondió `202`, y Settlement solo deja un `WARN` | `ProcessBatchClose` |
| Sin reintentos con backoff | Los conflictos de serialización y los errores persistentes consumen las 5 entregas de inmediato | `nak` |
| Sin migraciones reales | Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `st_schema.sql` difieren | `pkg/pgutil/migrate.go` |
| gRPC placeholder y sin apagado ordenado | Abre un puerto que no ofrece nada y que ignora `GRPC_PORT` | `st_grpc_server.go`, `st_wire.go` |
| `/readyz` no verifica NATS | Una instancia sin NATS sigue figurando como lista | `st_http_handler.go` |
| Errores de estado como `500` | `force-close` sobre un lote cerrado o un ID inválido devuelven `500` en lugar de `400` | `st_http_handler.go` |
| Código sin uso | `port.BatchService` y `port.AdminService` no se implementan; tampoco se usan `mdrPercent`, `BatchTxOffline` ni los eventos de dominio internos | `st_port_inbound.go`, `st_batch_handler.go`, `st_settlement_batch.go` |

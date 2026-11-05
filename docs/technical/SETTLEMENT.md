# Settlement — Documentación técnica

> Revisado el 2026-11-05. Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/settlement/README.md) o de los
> comentarios del código, vale lo que está acá. Para operar un incidente, ver el
> [runbook de cierre de lote](../runbooks/batch-close-failure.md).

## 1. Resumen

Settlement agrupa las transacciones aprobadas en lotes diarios por terminal, los cierra a pedido del
terminal o de forma automática, compara los totales con los del backend, los envía a un procesador y
registra la liquidación.

**El ciclo completo funciona, pero el envío y la confirmación no son reales:** no hay un procesador
externo. Con `SETTLEMENT_PROCESSOR=mock`, el envío se simula; con `none`, el default, el lote queda en
`CLOSED`. La liquidación la confirma un operador.

**Una base anterior a esta versión necesita la
[migración](#aplicar-la-migración) antes del despliegue:** sin ella, el servicio falla al leer y guardar
lotes.

| Capacidad | Estado | Detalle |
|---|---|---|
| Apertura de lotes y registro de compras | Implementado | Un lote abierto por terminal, día de negocio y moneda |
| Registro de reversas | Implementado | Con su monto real, en el lote abierto del momento |
| Cierre y detección de discrepancias | Implementado | Por pedido del terminal |
| Cierre automático | Implementado | Lotes de días de negocio anteriores, sin conciliación |
| Resolución de disputas | Implementado | `reopen` y `resolve-dispute` |
| Liquidación y comisión (MDR) | Implementado | Confirmación manual con `settle` |
| Publicación de `batch-closed` y `settlement.completed` | Implementado | Directa a NATS, sin outbox. Nadie consume `settlement.completed` |
| Consulta de lotes | Implementado | HTTP, sin autenticación |
| Envío al procesador | **Simulado** | `none` no envía; `mock` acepta sin enviar nada |
| Cierre forzado por soporte | **Parcial** | Registra discrepancias falsas y no publica eventos |
| Archivo de remesa | **Pendiente** | Requiere la especificación del procesador |

Las consecuencias de estos pendientes están en [§11](#11-brechas-conocidas).

## 2. Contexto y límites

```
Authorization ── auth.approved ────────────►┌────────────┐
Authorization ── auth.reversal-completed ──►│ Settlement │── settlement.batch-closed ──► Notification
                                            │            │── settlement.completed ──► (sin consumidores)
Terminal Gateway ── transaction.batch-close►│            │
                                            └─────┬──────┘
                                                  ┆ Submit (none o mock)
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
| [`infrastructure/processor/`](../../context/settlement/infrastructure/processor/) | Procesador simulado |
| [`infrastructure/http/`](../../context/settlement/infrastructure/http/) | Health checks, consulta y administración |
| [`infrastructure/grpc/server/`](../../context/settlement/infrastructure/grpc/server/) | Servidor sin servicios |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/settlement/`.

## 4. Modelo de dominio

### 4.1 `SettlementBatch`

| Campo | Descripción |
|---|---|
| `id` | UUID del lote |
| `terminalID`, `merchantID` | Terminal y comercio |
| `batchDate` | **Día de negocio** de la primera transacción, según `SETTLEMENT_BUSINESS_TIMEZONE` |
| `state` | Estado actual |
| `currency` | Moneda del lote. Las transacciones de otra moneda van a otro lote |
| `transactions` | Transacciones incluidas |
| `summary` | Totales; `nil` hasta el cierre |
| `discrepancies` | Resultado de la conciliación |
| `createdAt`, `closedAt`, `submittedAt`, `settledAt` | Marcas de tiempo |
| `version` | Bloqueo optimista (ver [§5.10](#510-concurrencia)) |

**Solo puede haber un lote `OPEN` por terminal, día de negocio y moneda.** Puede haber varios lotes de esa
misma combinación en otros estados, por ejemplo si el terminal sigue vendiendo después de un cierre.

### 4.2 Máquina de estados

```
OPEN ──► PENDING_CLOSE ──► CLOSED ──► SUBMITTED ──► SETTLED
  ▲           │            ▲  │            │
  │           ▼            │  ▼            ▼
  └───────────────── DISPUTED ◄────────────┘
```

| Transición | ¿Cuándo ocurre? |
|---|---|
| `OPEN` → `PENDING_CLOSE` → `CLOSED` | Al cerrar, en un solo paso: `PENDING_CLOSE` nunca se guarda |
| `CLOSED` → `DISPUTED` | Al cerrar con discrepancias, o en un `resubmit` si el lote las tiene |
| `DISPUTED` → `OPEN` | Con `reopen` |
| `DISPUTED` → `CLOSED` | Con `resolve-dispute` |
| `CLOSED` → `SUBMITTED` | Si el procesador acepta el envío (hoy, solo `mock`) |
| `SUBMITTED` → `SETTLED` | Con `settle` |
| `PENDING_CLOSE` o `SUBMITTED` → `DISPUTED`, `DISPUTED` → `SUBMITTED` | Permitidas, pero sin uso |

Solo `SETTLED` es un estado final.

### 4.3 `BatchTransaction`

| Tipo | Uso real |
|---|---|
| `PURCHASE` | Compra aprobada |
| `REVERSAL` | Reversa, con su monto real (ver [§5.2](#52-reversa)) |
| `OFFLINE` | Definido, pero ningún flujo lo genera |

`(transaction_id, tx_type)` es único: una compra o su reversa no se cuentan dos veces, incluso si el evento
llega con otro `event_id`.

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

**El terminal debe informar `terminal_count` contando compras y reversas, y `terminal_amount` como monto
neto.** Después de cada cierre, sus contadores deben volver a cero: el cierre siguiente solo abarca lo
posterior. Esta es la regla del backend: hay que confirmarla al integrar terminales reales.

## 5. Flujos

### 5.1 Registro de una compra

Al recibir `auth.approved`:

1. Valida los IDs, la moneda y el monto, y calcula el día de negocio de `authorized_at`.
2. Marca el evento en `processed_events` dentro de una transacción.
3. Busca el lote `OPEN` del terminal para ese día y moneda o lo crea, en una transacción `SERIALIZABLE`.
4. Agrega la transacción y guarda el lote, en otra transacción.

El guardado queda fuera de la transacción de idempotencia. Si el commit final falla, la reentrega
reprocesa sin duplicar, gracias a `ON CONFLICT (transaction_id, tx_type) DO NOTHING`.

### 5.2 Reversa

Al recibir `auth.reversal-completed`, se agrega una fila `REVERSAL` con el `transaction_id` de la compra y el
monto del evento. La fila va al **lote abierto del momento**, no necesariamente al de la compra:

| Situación | Resultado |
|---|---|
| La compra está en el lote abierto | Ese lote queda con el monto neto |
| La compra está en un lote ya cerrado | La reversa entra en un lote nuevo, que puede dar un **total negativo** |

No se verifica que la compra exista en Settlement ni que el monto coincida: se confía en el evento de
Authorization.

**Las reversas anteriores a esta corrección no se recuperan solas:** se descartaron, y su evento ya figura
como procesado. Ver el
[Escenario E](../runbooks/batch-close-failure.md#escenario-e--reversas-anteriores-sin-descontar).

### 5.3 Aprobación después del cierre

Si el terminal cierra su lote y sigue vendiendo durante el mismo día, la venta abre un **lote nuevo** para
esa fecha y moneda. Ese lote se cierra con un nuevo pedido del terminal o con el cierre automático.

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
   └─ discrepancies = 0 ─► envía al procesador
                              ├─ none ─► queda CLOSED, WARN y métrica
                              └─ mock ─► guarda SUBMITTED
```

Sin procesador, el mensaje se confirma: reintentarlo no cambiaría nada. El lote sigue en `CLOSED` hasta
un `resubmit`. Con cualquier otro error del procesador, el mensaje se reintenta, pero la reentrega ya no
encuentra un lote `OPEN` y no hace nada: la vía para reintentar también es `resubmit`.

### 5.5 Día de negocio

Los lotes se agrupan por día de negocio, en la zona de `SETTLEMENT_BUSINESS_TIMEZONE`
(`America/Argentina/Buenos_Aires` por defecto). Una venta de las 22:30 en Argentina pertenece a ese día,
aunque en UTC ya sea el siguiente.

El cierre usa el `batch_date` que informa el terminal. Sin él, usa el día de negocio del pedido. Terminal
Gateway rechaza una moneda o una fecha inválidas.

Hay una sola zona horaria para todo el servicio. Las fechas de los lotes creados antes de este cambio
siguen en UTC.

### 5.6 Cierre automático

Con `SETTLEMENT_BATCH_CLOSE_HOUR` en 3 (00:00 en Argentina), el job corre una vez por día desde las 03:00
UTC. Esa hora es **UTC**: si se cambia la zona de negocio, hay que ajustarla para que sea posterior a la
medianoche de esa zona. También corre al arrancar, por si el servicio estaba caído a esa hora. Con el valor `0` queda
desactivado.

1. Busca los lotes `OPEN` de **días de negocio anteriores**. No toca el lote del día, porque sigue
   recibiendo ventas.
2. Los cierra con los totales del backend: **no hay conciliación**, porque el terminal no informó los
   suyos, y por eso tampoco hay discrepancias.
3. Publica `batch-closed` y envía el lote al procesador. Un lote vacío solo se cierra.

Un evento sintético en `processed_events`, determinístico por lote y día, evita que dos instancias cierren
el mismo lote. Si falla el cierre de un lote, el job sigue con los demás y reintenta en el minuto
siguiente.

### 5.7 Disputas

| Operación | Efecto | Cuándo usarla |
|---|---|---|
| `reopen` | `DISPUTED` → `OPEN`. Descarta los totales y vuelve a aceptar transacciones | Faltan transacciones, o el terminal puede volver a cerrar con los totales correctos |
| `resolve-dispute` | `DISPUTED` → `CLOSED`. Recalcula con las transacciones actuales y deja las discrepancias en 0 | El backend es correcto y los totales del terminal no |

**Para volver a cerrar un lote de otro día,** el pedido debe informar su `batch_date`. Si no llega ninguno,
lo cierra el job automático al día siguiente, sin conciliar.

**`reopen` falla si ya hay otro lote `OPEN`** de ese terminal, día y moneda, porque solo puede haber uno. En
ese caso, usar `resolve-dispute`.

Después de `resolve-dispute`, hay que enviar el lote con `resubmit`. Ninguna de las dos operaciones publica
un nuevo evento: si los totales cambian, Notification no se entera.

### 5.8 Liquidación

`settle` confirma un lote `SUBMITTED` y lo pasa a `SETTLED`. La confirmación es manual, porque no hay un
procesador que la informe.

Cuando se liquidó el último lote con transacciones de un comercio en esa fecha, se publica
`settlement.completed`, un evento por moneda:

```
neto = bruto − redondeo(bruto × MDR / 100)
```

Si después un terminal del mismo comercio abre otro lote para esa fecha, al liquidarlo se publica otro
evento con el total acumulado.

### 5.9 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| IDs inválidos | `Term`; no se reintenta | No |
| Error de base de datos al registrar | `Nak` y reintento | **Sí** |
| Conflicto con otro guardado del mismo lote | Reintento interno, y después `Nak` | **Sí** (ver [§5.10](#510-concurrencia)) |
| Moneda inválida o monto no positivo | `Term`; no se reintenta | No |
| Se pierde `auth.approved` en Authorization | La transacción falta en el lote; el cierre da `DISPUTED` | No |
| Falla la publicación de `batch-closed` | Solo log; Notification no se entera | No |
| Falla el guardado de `DISPUTED` | `Nak`; la reentrega no hace nada y queda `CLOSED` con discrepancias. El próximo `resubmit` lo pasa a `DISPUTED` | Con operador |
| Cierre sin procesador configurado | Queda `CLOSED` | Con `resubmit` |
| El procesador falla | Queda `CLOSED` | Con `resubmit` |
| El terminal nunca pide el cierre | El job lo cierra sin conciliar | **Sí** |
| Falla el cierre automático de un lote | Reintenta en el minuto siguiente | **Sí** |
| Falla la publicación de `settlement.completed` | Solo log; el lote queda `SETTLED`, y nada lo reintenta | No |

### 5.10 Concurrencia

Cada lote tiene una `version`. `Save` solo actualiza si la versión de la base es la que se cargó: si otro
proceso la cambió, nada se guarda y devuelve `ErrStaleBatch`. Así, una venta simultánea no puede devolver a
`OPEN` un lote recién cerrado, y un cierre no calcula totales sin esa venta.

Las ventas, reversas y cierres **reintentan internamente** hasta 8 veces, con espera aleatoria, volviendo a
cargar el lote. Si el conflicto persiste, el error llega a NATS como transitorio. Las operaciones de
administración responden `409`, y hay que repetirlas.

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
| `POST /batches/{id}/force-close` | Forzar el cierre | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/resubmit` | Enviar un lote `CLOSED` | `200` | `400`, `404`, `409`, `500`, `503` |
| `POST /batches/{id}/reopen` | Reabrir un lote `DISPUTED` | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/resolve-dispute` | Aceptar los totales del backend | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/settle` | Confirmar la liquidación | `200` | `400`, `404`, `409`, `500` |

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
pero sin completar `SubmittedAt` ni `SettledAt`. Puede haber varios lotes de un terminal en una misma fecha.

### Forzar el cierre

```json
{ "operator_id": "soporte-juan" }
```

Responde `{"status": "closed"}`. Tiene varias limitaciones:

- Solo funciona sobre lotes `OPEN`. En cualquier otro estado responde `500`, no `400`.
- Cierra comparando contra 0, por lo que **cualquier lote con transacciones queda con discrepancias
  falsas**, aunque su estado sea `CLOSED`.
- No publica `batch-closed` ni intenta el envío. Por las discrepancias falsas, el lote pasa a `DISPUTED`
  en el primer `resubmit`, y sale con `resolve-dispute`.
- `operator_id` es obligatorio, pero **no se guarda ni se registra en ningún log**.

### Enviar al procesador

Lleva el mismo body y responde `{"status": "submitted"}`.

| Situación | Respuesta |
|---|---|
| El lote no está en `CLOSED` | `400` |
| El lote tiene discrepancias | `400`, y **el lote pasa a `DISPUTED`**, sin enviarse |
| Sin procesador configurado | `503` con `PROCESSOR_UNAVAILABLE`; el lote queda en `CLOSED` |
| Falla el procesador | `500` |

### Resolver disputas

`reopen` y `resolve-dispute` exigen `operator_id` y `reason`, y responden `{"status": "reopened"}` o
`{"status": "closed"}`. Con un lote que no esté en `DISPUTED`, responden `400`. `reopen` también responde
`400` si ya hay otro lote `OPEN` de ese terminal, día y moneda.

```json
{ "operator_id": "soporte-juan", "reason": "Conciliado con el comercio, ticket 123" }
```

Ambos datos quedan **solo en los logs**, en nivel `WARN`.

### Confirmar la liquidación

`settle` exige `operator_id` y responde `{"status": "settled"}`. Con un lote que no esté en `SUBMITTED`,
responde `400`. El `operator_id` queda en los logs.

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
| `posnet.settlement.completed.v1` | `POSNET_SETTLEMENT` | Sí, sin outbox | Ninguno: Notification no lo consume |

Payload de `batch-closed`:

| Campo | Notas |
|---|---|
| `batch_id`, `terminal_id`, `merchant_id` | `batch_id` también es el `correlation_id` |
| `batch_date` | `YYYY-MM-DD` |
| `total_count`, `total_amount`, `currency` | Totales del backend |
| `discrepancies` | Permite distinguir un cierre con diferencias |
| `closed_at` | RFC3339 UTC |

Payload de `settlement.completed`:

| Campo | Notas |
|---|---|
| `merchant_id`, `settlement_date` | `merchant_id` también es el `correlation_id` |
| `total_batches`, `currency` | Lotes liquidados en esa moneda |
| `total_amount`, `net_amount`, `mdr_percent` | Bruto, neto y comisión aplicada; el MDR no se guarda por lote |
| `completed_at` | RFC3339 UTC |

## 9. Persistencia

Schema `settlement`. **La fuente real es
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql):** `pgutil.Migrate` no
hace nada, `migrations/settlement/` está vacío y
[`st_schema.sql`](../../context/settlement/infrastructure/postgres/st_schema.sql) no se ejecuta.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `settlement_batches` | Lotes | PK `id`; único `(terminal_id, batch_date, currency)` solo para `OPEN` |
| `batch_transactions` | Transacciones del lote | PK `id`; único `(transaction_id, tx_type)`; FK `batch_id` |
| `processed_events` | Idempotencia | PK `event_id` |

Diferencia entre ambos archivos: `st_schema.sql` define tres índices
(`idx_settlement_batches_merchant_date`, `idx_settlement_batches_state` e `idx_batch_transactions_batch`)
que `01-init.sql` no crea. **`loadTransactions` recorre `batch_transactions` por `batch_id` en cada
aprobación, sin índice.**

### Aplicar la migración

**Una base existente necesita esta migración antes de desplegar esta versión del servicio.** `01-init.sql`
solo corre sobre una base vacía, y `pgutil.Migrate` no hace nada:

```bash
psql "$POSTGRES_DSN" -f migrations/settlement/000001_batch_integrity.up.sql
```

Es idempotente y no toca los datos. Se probó sobre una base con el schema anterior: el resultado coincide
con el de una base nueva.

`000001_batch_integrity.down.sql` la revierte, pero **falla sin cambiar nada** si ya hay varios lotes por
terminal y fecha o filas `REVERSAL`, porque el schema anterior no los admite. Resolver esos datos es una
decisión financiera, y el script no los borra.

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
| `posnet_settlement_reversals_total` | Counter | — | Reversas procesadas; incluye duplicados que no agregan fila |
| `posnet_settlement_batches_total` | Counter | `currency`, `state` | Cierres; `state` siempre es `CLOSED`, porque se registra antes de marcar la disputa |
| `posnet_settlement_batch_duration_seconds` | Histogram | — | Duración del cierre |
| `posnet_settlement_submissions_total` | Counter | `result` | Envíos tras un cierre: `submitted`, `unavailable` o `error`. No incluye `resubmit` |

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

En los logs, `modified concurrently` indica conflictos de versión, que son esperables con tráfico simultáneo,
y `no open batch found` indica cierres sin efecto. `batch closed automatically` identifica los cierres sin
conciliar, y `no settlement processor configured` los lotes que quedaron sin enviar.

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
| `SETTLEMENT_MDR_PERCENT` | `2.5` | Sí; se usa para calcular el neto. Fuera de 0–10, el servicio no arranca |
| `SETTLEMENT_BATCH_CLOSE_HOUR` | `3` | Sí; hora UTC del cierre automático, y `0` lo desactiva. Fuera de 0–23, el servicio no arranca |
| `SETTLEMENT_PROCESSOR` | `none` | Sí; `none` o `mock`. Docker Compose usa `mock`. Con otro valor, el servicio no arranca |
| `SETTLEMENT_BUSINESS_TIMEZONE` | `America/Argentina/Buenos_Aires` | Sí; zona IANA del día de negocio. Si es inválida, el servicio no arranca |
| `SETTLEMENT_SUBMIT_RETRIES` | `3` | **No**: no hay procesador real |
| `SETTLEMENT_SUBMIT_TIMEOUT` | `30s` | **No**: no hay procesador real |
| `GRPC_PORT` | `9093` | **No**: el puerto está fijo en 9093 |
| `MIGRATIONS_DIR` | `migrations/settlement` | **No**: `Migrate` no hace nada |

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

Para probar una disputa, cerrar con totales que **no** coincidan, y después resolverla:

```bash
curl -s localhost:8081/batch-close -H 'Content-Type: application/json' -d '{
  "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "terminal_count": 999,
  "terminal_amount": 1,
  "currency": "ARS"
}'
```

```bash
BATCH=$(curl -s "localhost:8084/merchants/$MERCHANT/batches?date=$(date -u +%F)" | jq -r '.batches[0].ID')
OP='"operator_id":"prueba"'

curl -s -X POST localhost:8084/batches/$BATCH/resolve-dispute -d "{$OP,\"reason\":\"prueba\"}"
curl -s -X POST localhost:8084/batches/$BATCH/resubmit -d "{$OP}"
curl -s -X POST localhost:8084/batches/$BATCH/settle -d "{$OP}"
```

### Tests

```bash
go test ./context/settlement/...

# Carga: requiere el servicio y NATS activos
locust -f posnet-load-tests/settlement/settlement_locust.py \
  --headless -u 10 -r 2 --run-time 2m --host http://localhost:8084
```

Los tests usan una base simulada y un procesador falso, por lo que **no detectan** problemas que dependan
del SQL real, como restricciones o concurrencia. No hay tests de integración automatizados contra Postgres y NATS reales, ni tests del
wiring.
Para la carga, ver también [`LOCUST.md`](../../LOCUST.md).

## 11. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Montos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Reversas anteriores sin recuperar | Las procesadas antes de la corrección no se guardaron, y sus eventos figuran como procesados | Datos históricos |
| Un cierre tardío usa solo lo pendiente | Si los contadores del terminal no se reinician tras cada cierre, el segundo cierre del día da `DISPUTED` | `ProcessBatchClose` |
| La reversa no se valida contra la compra | Se confía en el monto y la moneda del evento | `RegisterReversal` |
| Una sola zona horaria | No sirve para comercios de distintas zonas | `SETTLEMENT_BUSINESS_TIMEZONE` |
| `batch_date` no valida coherencia | Un terminal puede pedir el cierre de cualquier fecha con un lote `OPEN` | `closeDate` |

### Ciclo de vida del lote

| Brecha | Consecuencia | Dónde |
|---|---|---|
| No hay procesador real | `mock` marca `SUBMITTED` sin enviar nada. Falta el adaptador y el archivo de remesa, que requieren la especificación del procesador. `SETTLEMENT_SUBMIT_RETRIES` y `SETTLEMENT_SUBMIT_TIMEOUT` siguen sin efecto | `st_mock_processor.go` |
| La confirmación de liquidación es manual | Un operador debe llamar a `settle`, sin autenticación ni referencia del procesador | `SettleBatch` |
| Nadie consume `settlement.completed` | El comercio no recibe el neto liquidado: Notification no se suscribe | `nt_nats_subscriber.go` |
| `settlement.completed` sin outbox | Si falla la publicación, nada la reintenta y el lote ya está `SETTLED` | `publishCompletedIfDone` |
| Evento adicional por lotes tardíos | Un lote abierto después de la liquidación diaria genera otro evento con el total acumulado | `publishCompletedIfDone` |
| El cierre automático no concilia | Los lotes se envían con los totales del backend, sin comparar con el terminal | `CloseStaleBatches` |
| Doble envío posible | Si el procesador acepta y falla el guardado, el lote queda `CLOSED`, y `resubmit` lo enviaría otra vez | `submitBatch`, `ResubmitBatch` |
| `resolve-dispute` no notifica | Si los totales cambian, Notification solo conoce los del primer cierre | `ResolveDispute` |
| Auditoría solo en logs | `operator_id` y `reason` no se guardan en la base | `AdminHandler` |
| Lotes vacíos en `CLOSED` | Los lotes vacíos que cierra el job quedan en `CLOSED` y aparecen en las consultas de lotes pendientes | `autoCloseBatch` |

### Cierre forzado y seguridad

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Operaciones sin autenticación | Con acceso de red, cualquiera puede cerrar el lote de cualquier terminal; un cierre hecho así también hace perder las aprobaciones posteriores | `st_http_router.go` |
| `force-close` compara contra 0 | Todo lote con transacciones queda con discrepancias falsas, y debe pasar por `resolve-dispute` antes de enviarse | `ForceClose` |
| `force-close` no registra el operador | `operator_id` es obligatorio, pero no se guarda ni se registra | `ForceClose` |
| `force-close` no notifica | No se publica `batch-closed`, así que Notification no se entera | `ForceClose` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Sin alertas de lotes | Los lotes trabados o en disputa solo se descubren consultando la base | Prometheus |
| Las disputas no se ven en las métricas | `state` siempre vale `CLOSED` en `posnet_settlement_batches_total` | `ProcessBatchClose` |
| Sin alerta para lotes sin enviar | `posnet_settlement_submissions_total{result="unavailable"}` existe, pero ninguna regla la usa | Prometheus |
| Faltan índices y hay costo creciente por aprobación | Cada evento recorre las transacciones del lote sin índice | `01-init.sql`, `Save`, `loadTransactions` |
| `batch-closed` se publica sin outbox | Si falla, Notification no se entera y nada lo reintenta | `ProcessBatchClose` |
| Un cierre sin lote no recibe respuesta | Terminal Gateway ya respondió `202`, y Settlement solo deja un `WARN` | `ProcessBatchClose` |
| Sin backoff en NATS | Los errores persistentes, como una base caída, consumen las 5 entregas de inmediato. Los conflictos de versión sí tienen reintento interno | `nak` |
| Sin migraciones reales | Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `st_schema.sql` difieren | `pkg/pgutil/migrate.go` |
| gRPC placeholder y sin apagado ordenado | Abre un puerto que no ofrece nada y que ignora `GRPC_PORT` | `st_grpc_server.go`, `st_wire.go` |
| `/readyz` no verifica NATS | Una instancia sin NATS sigue figurando como lista | `st_http_handler.go` |
| Errores de estado como `500` | `force-close` sobre un lote cerrado o un ID inválido devuelven `500` en lugar de `400` | `st_http_handler.go` |
| Código sin uso | `port.BatchService` y `port.AdminService` no se implementan; tampoco se usan `mdrPercent`, `BatchTxOffline` ni los eventos de dominio internos | `st_port_inbound.go`, `st_batch_handler.go`, `st_settlement_batch.go` |

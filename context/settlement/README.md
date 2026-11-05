# Settlement Context

Gestiona el **cierre de lote diario (EOD)** por terminal. Agrupa todas las transacciones aprobadas en un batch abierto, lo cierra al final del día, detecta discrepancias y genera el archivo de remesa para enviarlo al procesador (Visa/Mastercard).

## Dominio

### `SettlementBatch` (Aggregate Root)

Lote diario por terminal. Garantía de unicidad: un solo lote abierto por terminal por día.

```
OPEN → PENDING_CLOSE → CLOSED → SUBMITTED → SETTLED
                                          → DISPUTED
```

**Campos clave:** `id`, `terminalID`, `merchantID`, `date`, `state`, `summary`

### `BatchTransaction` (Entity)

Transacción individual dentro del lote:

| Tipo | Descripción |
|---|---|
| `PURCHASE` | Venta aprobada |
| `REVERSAL` | Reversa de una compra |
| `OFFLINE` | Transacción capturada sin conexión |

### `BatchSummary` (Value Object)

Totales calculados al cierre:

| Campo | Descripción |
|---|---|
| `purchaseCount` | Cantidad de compras |
| `purchaseAmount` | Total de compras |
| `reversalCount` | Cantidad de reversas |
| `reversalAmount` | Total de reversas |
| `netAmount` | `purchaseAmount - reversalAmount` |

## Arquitectura

```
application/
  register_approval.go       → agrega transacción aprobada al lote abierto
  register_reversal.go       → resta reversa del lote
  process_batch_close.go     → calcula totales, detecta discrepancias, cierra lote
  get_batch.go               → consulta estado del lote (admin)
  list_batches_by_merchant.go→ reporte diario por comercio
  force_close.go             → cierre manual por soporte
domain/
  settlement_batch.go        → aggregate root + state machine
  batch_transaction.go       → entity
  batch_summary.go           → value object
  ports.go                   → interfaces (Repository, EventPublisher, SettlementProcessor)
infrastructure/
  postgres/                  → persistencia con unicidad diaria por terminal (sqlc)
  nats/                      → subscriber + publisher JetStream
config/
  config.go                  → carga de env vars
```

## Use Cases

| Use Case | Descripción |
|---|---|
| `RegisterApproval` | Agrega compra al lote; crea el lote si no existe |
| `RegisterReversal` | Resta el monto de la reversa del lote activo |
| `ProcessBatchClose` | Calcula totales, detecta discrepancias, cierra el lote |
| `GetBatch` | Consulta estado y detalle de un lote |
| `ListBatchesByMerchant` | Reporte diario de lotes por comercio |
| `ForceClose` | Cierre manual de soporte ante problemas operativos |

## Eventos NATS

**Publica:**

| Subject | Cuándo |
|---|---|
| `posnet.settlement.batch-closed.v1` | Lote cerrado exitosamente |
| `posnet.settlement.completed.v1` | Remesa enviada al procesador |

**Suscribe:**

| Subject | Qué hace |
|---|---|
| `posnet.auth.approved.v1` | Registra la compra en el lote abierto |
| `posnet.auth.reversal-completed.v1` | Resta la reversa del lote |
| `posnet.transaction.batch-close.v1` | Inicia el cierre del lote (EOD por terminal) |

## Configuración

| Variable | Default | Descripción |
|---|---|---|
| `GRPC_PORT` | `9093` | Puerto gRPC |
| `HTTP_PORT` | `8083` | Puerto health/admin |
| `POSTGRES_DSN` | — | Cadena de conexión PostgreSQL |
| `NATS_URL` | — | URL NATS JetStream |
| `SETTLEMENT_MDR_PERCENT` | `2.5` | Porcentaje de comisión por defecto |
| `SETTLEMENT_BATCH_CLOSE_HOUR` | `3` | Hora UTC del cierre automático (00:00 en Argentina); `0` lo desactiva |
| `SETTLEMENT_PROCESSOR` | `none` | Procesador externo: `none` o `mock` |
| `SETTLEMENT_BUSINESS_TIMEZONE` | `America/Argentina/Buenos_Aires` | Zona horaria del día de negocio |
| `SETTLEMENT_ADMIN_API_KEY` | — | Token de las operaciones de escritura (mínimo 32 caracteres). Vacío, quedan deshabilitadas |
| `SETTLEMENT_SUBMIT_RETRIES` | `3` | Reintentos de envío al procesador |
| `SETTLEMENT_SUBMIT_TIMEOUT` | `30s` | Timeout de envío |

## Correr localmente

```bash
go run ./context/settlement/...
```

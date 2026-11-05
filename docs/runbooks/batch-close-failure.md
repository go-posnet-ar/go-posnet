# Runbook: Fallos en el cierre de lote (Settlement)

## Antes de empezar — qué se puede resolver hoy

El ciclo del lote tiene salidas por API para cada estado, pero **no hay un procesador real**:

| `SETTLEMENT_PROCESSOR` | Dónde | Qué pasa con un lote correcto |
|---|---|---|
| `none` | Default del código | Queda en `CLOSED`, sin enviar |
| `mock` | Docker Compose | Pasa a `SUBMITTED`, **sin enviar nada** |

La liquidación (`SETTLED`) la confirma un operador con `settle`.

Revisado contra el código el 2026-11-05. Detalle técnico en
[SETTLEMENT.md](../technical/SETTLEMENT.md). Puerto HTTP en Docker Compose: `8084`.

| Síntoma | Escenario |
|---|---|
| Lote en `DISPUTED` | [A](#escenario-a--lote-en-disputed) |
| Cierre sin efecto, log `no open batch found` | [B](#escenario-b--cierre-sin-lote-abierto) |
| Lote en `CLOSED` que no avanza | [C](#escenario-c--lote-en-closed-sin-enviar) |
| Aprobaciones sin lote, log `reload after conflict` | [D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Reversa que no se descontó | [E](#escenario-e--reversas-sin-descontar) |
| Lote `OPEN` de días anteriores | [F](#escenario-f--lotes-abiertos-de-días-anteriores) |
| Lote en `SUBMITTED` que no se liquida | [G](#escenario-g--confirmar-la-liquidación) |

## Contexto del flujo

Terminal Gateway publica `posnet.transaction.batch-close.v1`, y `settlement-batch-consumer`
(`POSNET_TRANSACTIONS`, 5 entregas) lo procesa en `BatchHandler.ProcessBatchClose`:

1. Busca el lote `OPEN` del terminal para `batch_date`. Si no hay ninguno, registra un `WARN` y confirma.
2. Calcula los totales del backend y los compara con los del terminal.
3. Marca el evento y **guarda el lote en `CLOSED`**.
4. Publica `settlement.batch-closed`. Si falla, solo queda un log.
5. Con discrepancias: guarda `DISPUTED` y termina.
6. Sin discrepancias: lo envía al procesador. Sin procesador, queda en `CLOSED`.

```
                                 reopen
        ┌─────────────────────────────────────────────┐
        ▼                                             │
      OPEN ──► CLOSED ──► SUBMITTED ──► SETTLED    DISPUTED
                 │  ▲                                ▲  │
                 │  └────────── resolve-dispute ─────┼──┘
                 └──────── con discrepancias ────────┘
```

`PENDING_CLOSE` existe, pero no se guarda nunca: el cierre lo atraviesa en un solo paso.

**Restricciones que explican varios escenarios:**

- Hay **un solo lote por terminal y fecha, en cualquier estado** (`UNIQUE (terminal_id, batch_date)`).
- Las fechas son **UTC**, y Terminal Gateway siempre publica la fecha UTC actual.

### Modificar un lote a mano

Los pasos con `UPDATE` o `DELETE` cambian datos financieros y **requieren la aprobación de quien
corresponda**. Antes de ejecutarlos, guardá una copia de las filas en el ticket del incidente, y usá
siempre una transacción:

```sql
BEGIN;
-- UPDATE o DELETE, con RETURNING para ver qué cambió
-- Si el resultado es el esperado: COMMIT;  si no: ROLLBACK;
```

---

## Escenario A — Lote en `DISPUTED`

**Síntoma:** `GET /batches/{id}` devuelve `"State": "DISPUTED"`. En logs: `batch has discrepancies —
marking as disputed` o `resubmit blocked by discrepancies`.

**Causa:** la cantidad o el monto del terminal no coinciden con el backend. Las más comunes:

| Causa | Cómo reconocerla |
|---|---|
| El terminal descontó una reversa y el backend no | [Escenario E](#escenario-e--reversas-sin-descontar) |
| Aprobaciones que no llegaron al lote | [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Ventas de otra fecha UTC | Cierres cerca de las 21:00 en UTC-3: [Escenario F](#escenario-f--lotes-abiertos-de-días-anteriores) |
| Totales enviados incorrectamente por el terminal | El backend coincide con `pn_authorization.transactions` |
| El lote pasó por `force-close` | Discrepancias falsas: el cierre forzado compara contra 0 |

Una transacción no puede contarse dos veces: `transaction_id` es único en la tabla.

**Diagnóstico:**

```sql
-- Totales del backend
SELECT id, state, batch_date, total_count, total_amount, purchase_count, purchase_amount,
       reversal_count, reversal_amount, discrepancies
FROM settlement.settlement_batches WHERE id = '<batch_id>';

-- Transacciones del lote
SELECT transaction_id, amount_cents, tx_type, included_at
FROM settlement.batch_transactions WHERE batch_id = '<batch_id>' ORDER BY included_at;

-- Lo que Authorization registró para ese terminal y fecha UTC
SELECT state, count(*), sum(amount_cents)
FROM pn_authorization.transactions
WHERE terminal_id = '<terminal_id>'
  AND (COALESCE(authorized_at, created_at) AT TIME ZONE 'UTC')::date = '<batch_date>'
GROUP BY state;
```

Lo que informó el terminal no se guarda: buscalo en el evento original o en el sistema del terminal.
`discrepancies` no mide dinero: es la diferencia de cantidades, o `1` si solo difiere el monto.

**Resolución:** elegí según quién tenga razón. Ambas exigen `operator_id` y `reason`, que quedan en los
logs: dejá también el motivo en el ticket.

| Situación | Operación | Qué pasa después |
|---|---|---|
| **El backend es correcto** | `resolve-dispute` | Queda en `CLOSED`, con los totales del backend y sin discrepancias. Enviarlo con `resubmit` (Escenario C) |
| **Faltan transacciones o el terminal debe reenviar sus totales** | `reopen` | Queda en `OPEN` y vuelve a aceptar transacciones. Republicar lo que falte (Escenario D) y pedir un nuevo cierre |

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/resolve-dispute \
  -d '{"operator_id":"<tu_usuario>","reason":"<motivo o ticket>"}'

curl -X POST http://<host>:8084/batches/<batch_id>/reopen \
  -d '{"operator_id":"<tu_usuario>","reason":"<motivo o ticket>"}'
```

**Atención con `reopen` en lotes de otro día:** un nuevo cierre del terminal no llega a ese lote, porque
Terminal Gateway siempre publica la fecha UTC actual. Lo cerrará el job automático en su próxima corrida
del día siguiente, **sin conciliar**. Si el backend ya es correcto, usá `resolve-dispute`.

Ninguna de las dos publica un nuevo `batch-closed`: si los totales cambian, avisale al comercio por otro
medio.

---

## Escenario B — Cierre sin lote abierto

**Síntoma:** el terminal pidió el cierre y no cambió nada. Log: `no open batch found for terminal —
nothing to close`. El mensaje se confirma, sin reintento, y el cliente ya había recibido `202`.

**Causas típicas:**

- El lote ya estaba cerrado: un reintento del terminal o de un reprocesamiento.
- No hubo transacciones en esa fecha UTC.
- El cierre ocurrió con otra fecha UTC que la de las ventas: el lote de las ventas sigue `OPEN`
  ([Escenario F](#escenario-f--lotes-abiertos-de-días-anteriores)).

**Diagnóstico:**

```sql
SELECT id, state, batch_date, closed_at FROM settlement.settlement_batches
WHERE terminal_id = '<terminal_id>' ORDER BY batch_date DESC LIMIT 5;
```

| Resultado | Qué significa |
|---|---|
| Lote cerrado o posterior en esa fecha | El cierre ya se había procesado: no hay nada que hacer |
| Lote `OPEN` de otra fecha | Escenario F |
| Sin lote en esa fecha, pero con ventas aprobadas | Faltan eventos: Escenario D |

---

## Escenario C — Lote en `CLOSED` sin enviar

**Síntoma:** el lote sigue en `CLOSED`, con `submitted_at` vacío.

```sql
SELECT id, state, discrepancies, closed_at, submitted_at
FROM settlement.settlement_batches WHERE id = '<batch_id>';
```

| Log o causa | Qué significa |
|---|---|
| `no settlement processor configured` | `SETTLEMENT_PROCESSOR=none`. También aumenta `posnet_settlement_submissions_total{result="unavailable"}` |
| `submitBatch: processor submit` | Falló el procesador. La reentrega no reintenta el envío, porque ya no hay un lote `OPEN` |
| `discrepancies` mayor que 0 | Viene de `force-close`, o falló el guardado de `DISPUTED` durante el cierre |
| Lote sin transacciones | Lote vacío cerrado por el job. No requiere acción |

**Resolución:**

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/resubmit -d '{"operator_id":"<tu_usuario>"}'
```

| Respuesta | Qué significa | Qué hacer |
|---|---|---|
| `200` | Enviado; pasa a `SUBMITTED` | Continuar con el Escenario G |
| `503 PROCESSOR_UNAVAILABLE` | No hay procesador configurado | El lote queda en `CLOSED`, sin riesgo. **No lo fuerces:** esperá a tener un procesador, o activá `mock` solo en desarrollo |
| `400`, "moved to DISPUTED" | Tenía discrepancias y **no se envió** | Escenario A |
| `400`, otro estado | El lote no está en `CLOSED` | Verificá su estado |
| `500` | Falló el procesador o la base de datos | Revisá los logs y reintentá |

**Riesgo cuando exista un procesador real:** si el procesador acepta y falla el guardado, el lote queda en
`CLOSED`, y `resubmit` lo enviaría dos veces. Antes de reintentar un `500`, confirmá con el procesador si
lo recibió.

---

## Escenario D — Aprobaciones fuera de todo lote

**Síntoma:** transacciones `APPROVED` sin fila en `batch_transactions`. Logs de Settlement: `transient
error` con `reload after conflict`, hasta agotar 5 entregas.

**Causas:**

| Causa | Cómo reconocerla |
|---|---|
| Venta posterior al cierre, en la misma fecha UTC | `reload after conflict`; el lote de esa fecha no está `OPEN` |
| `auth.approved` no se publicó | `failed to publish approval event` en Authorization; el evento no está en el stream |
| Settlement no pudo procesar el evento | Otros errores repetidos en `settlement-auth-consumer` |

**Diagnóstico:**

```sql
SELECT t.id, t.terminal_id, t.amount_cents, t.authorized_at, b.id AS batch_id, b.state AS batch_state
FROM pn_authorization.transactions t
LEFT JOIN settlement.batch_transactions bt ON bt.transaction_id = t.id
LEFT JOIN settlement.settlement_batches b
       ON b.terminal_id = t.terminal_id
      AND b.batch_date = (t.authorized_at AT TIME ZONE 'UTC')::date
WHERE t.state = 'APPROVED' AND bt.transaction_id IS NULL
ORDER BY t.authorized_at DESC;
```

**Resolución:**

1. **Dejá el lote de esa fecha en `OPEN`:**

   | Estado del lote | Qué hacer |
   |---|---|
   | `OPEN` o sin lote | Nada: seguí al paso 2 |
   | `DISPUTED` | `reopen` (Escenario A) |
   | `CLOSED` | Con aprobación, reabrirlo con el SQL de abajo: el endpoint solo acepta `DISPUTED` |
   | `SUBMITTED` o `SETTLED` | **No lo reabras:** el lote ya se envió. Coordiná una corrección con el equipo |

   ```sql
   BEGIN;
   UPDATE settlement.settlement_batches
   SET state = 'OPEN', closed_at = NULL, discrepancies = 0,
       total_count = NULL, total_amount = NULL,
       purchase_count = NULL, purchase_amount = NULL,
       reversal_count = NULL, reversal_amount = NULL
   WHERE id = '<batch_id>' AND state = 'CLOSED'
   RETURNING id, state;
   -- COMMIT;  o ROLLBACK;
   ```

2. **Republicá el `auth.approved` original** con [nats-dlq-reprocess.md](nats-dlq-reprocess.md). No debería
   existir una fila en `settlement.processed_events`, porque la transacción se deshace al fallar.
3. **Si el evento no está en el stream,** no hay un procedimiento seguro: coordinalo con el equipo.
4. **Cerrá de nuevo el lote:** si es de hoy, con un nuevo pedido del terminal. Si es de otro día, lo cierra
   el job automático al día siguiente.

Mientras esto no se corrija en el código, seguir vendiendo después de cerrar el lote el mismo día UTC
vuelve a producir el problema. Evitá también `force-close` en lotes de terminales activos. El job
automático no lo provoca, porque solo cierra lotes de días anteriores.

---

## Escenario E — Reversas sin descontar

**Síntoma:** una venta anulada sigue sumando en el lote. La métrica `posnet_settlement_reversals_total`
aumenta y no hay errores en los logs.

**Causa:** **ocurre con todas las reversas.** La fila `REVERSAL` usa el `transaction_id` de la compra,
y `ON CONFLICT (transaction_id) DO NOTHING` la descarta. Reprocesar no sirve: se descartaría de nuevo.

**Diagnóstico:**

```sql
SELECT bt.batch_id, b.state AS batch_state, bt.transaction_id, bt.amount_cents, t.state AS tx_state
FROM settlement.batch_transactions bt
JOIN settlement.settlement_batches b ON b.id = bt.batch_id
JOIN pn_authorization.transactions t ON t.id = bt.transaction_id
WHERE t.state = 'REVERSED' AND bt.tx_type = 'PURCHASE'
ORDER BY b.batch_date DESC;
```

**Resolución:** ninguna automática. Con el lote en `OPEN`, y con aprobación, se puede quitar la compra
para que los totales coincidan con el monto neto:

```sql
BEGIN;
DELETE FROM settlement.batch_transactions
WHERE transaction_id = '<transaction_id>' AND tx_type = 'PURCHASE'
  AND batch_id IN (SELECT id FROM settlement.settlement_batches WHERE state = 'OPEN')
RETURNING *;
-- COMMIT;  o ROLLBACK;
```

Esto borra el rastro de la compra en el lote, así que guardá la fila en el ticket. Si el terminal cuenta
las reversas en su `terminal_count`, confirmá cómo cuadrar la cantidad antes de cerrar.

**Hacelo antes del cierre automático:** el job cierra y envía los lotes de días anteriores **sin revisar
reversas**. Si el lote ya está en `SUBMITTED` o `SETTLED`, coordiná una corrección con el equipo.

Una reversa en otra fecha UTC también crea un lote `OPEN` vacío, que después cierra el job.

---

## Escenario F — Lotes abiertos de días anteriores

**Síntoma:** lotes `OPEN` con `batch_date` pasada. Lo normal es que no persistan: el job automático los
cierra.

**Cómo funciona el job:** corre desde las **03:00 UTC (00:00 en Argentina)** y al arrancar el servicio.
Cierra los lotes de días UTC anteriores con los totales del backend, **sin conciliar con el terminal**,
publica `batch-closed` y los envía al procesador. Log: `batch closed automatically without terminal
totals`.

**Diagnóstico:**

```sql
SELECT b.id, b.terminal_id, b.batch_date, count(bt.id) AS tx_count, sum(bt.amount_cents) AS amount
FROM settlement.settlement_batches b
LEFT JOIN settlement.batch_transactions bt ON bt.batch_id = b.id
WHERE b.state = 'OPEN' AND b.batch_date < (NOW() AT TIME ZONE 'UTC')::date
GROUP BY b.id, b.terminal_id, b.batch_date
ORDER BY b.batch_date;
```

```bash
docker logs posnet-settlement 2>&1 | grep -E 'automatic batch close'
```

| Si el lote sigue `OPEN` | Causa probable |
|---|---|
| Antes de las 03:00 UTC | Todavía no corrió el job de hoy |
| `SETTLEMENT_BATCH_CLOSE_HOUR=0` | El job está desactivado |
| `automatic batch close failed` | Error de cierre: el job reintenta cada minuto |
| Se reabrió hoy con `reopen` | El job ya lo marcó en la corrida de hoy: lo cierra al día siguiente |

**Si no querés esperar:** `POST /batches/{id}/force-close`, con sus limitaciones (ver abajo).

---

## Escenario G — Confirmar la liquidación

**Síntoma:** lote en `SUBMITTED`. **Nada lo avanza solo:** no hay un procesador que confirme la
liquidación. Con `mock`, además, el lote nunca se envió.

Una vez confirmada por fuera del sistema:

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/settle -d '{"operator_id":"<tu_usuario>"}'
```

Responde `200` y pasa a `SETTLED`, que es un estado final. Sobre otro estado responde `400`.

Al liquidar el último lote con transacciones del comercio en esa fecha, se publica
`settlement.completed`, con el bruto, el neto y el MDR. **Hoy ningún servicio lo consume,** así que el
comercio no recibe ese aviso.

```sql
-- Lotes del comercio y fecha que faltan liquidar
SELECT id, terminal_id, state FROM settlement.settlement_batches
WHERE merchant_id = '<merchant_id>' AND batch_date = '<batch_date>' AND state <> 'SETTLED';
```

Los lotes vacíos no impiden que se publique el evento.

Si falla la publicación, queda `failed to publish SettlementCompleted` en el log, y nada la reintenta.

---

## Referencia — endpoints

| Endpoint | Sobre qué estado | Resultado |
|---|---|---|
| `GET /batches/{id}` | Cualquiera | Estado y totales. Un lote `OPEN` muestra totales en 0. Los campos están en PascalCase (`State`, `TotalCount`) |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Cualquiera | Lotes del comercio; `date` es obligatorio |
| `POST /batches/{id}/force-close` | Solo `OPEN`; otros, `500` | Pasa a `CLOSED` |
| `POST /batches/{id}/resubmit` | Solo `CLOSED`; otros, `400` | Pasa a `SUBMITTED`, o a `DISPUTED` si tiene discrepancias. Sin procesador, `503` |
| `POST /batches/{id}/reopen` | Solo `DISPUTED`; otros, `400` | Pasa a `OPEN` |
| `POST /batches/{id}/resolve-dispute` | Solo `DISPUTED`; otros, `400` | Pasa a `CLOSED`, con los totales del backend |
| `POST /batches/{id}/settle` | Solo `SUBMITTED`; otros, `400` | Pasa a `SETTLED` |

Todos los `POST` exigen `operator_id`; `reopen` y `resolve-dispute`, también `reason`. **Ninguno tiene
autenticación**, y esos datos solo quedan en los logs.

**Limitaciones de `force-close`:**

- Compara contra 0: **todo lote con transacciones queda con discrepancias falsas**. En el primer
  `resubmit` pasa a `DISPUTED`, y sale con `resolve-dispute`.
- No publica `batch-closed`, así que Notification no se entera.
- No guarda ni registra `operator_id`: anotá en el ticket quién lo ejecutó y por qué.
- Bloquea el lote de esa fecha: las aprobaciones posteriores del terminal caen en el Escenario D.

### Lotes que necesitan atención

```sql
SELECT id, terminal_id, merchant_id, batch_date, state, discrepancies, closed_at, submitted_at
FROM settlement.settlement_batches
WHERE state IN ('DISPUTED', 'CLOSED', 'SUBMITTED')
   OR (state = 'OPEN' AND batch_date < (NOW() AT TIME ZONE 'UTC')::date)
ORDER BY batch_date DESC;
```

# Runbook: Fallos en el cierre de lote (Settlement)

## Antes de empezar — qué se puede resolver hoy

El ciclo del lote tiene salidas por API para cada estado, pero **no hay un procesador real**:

| `SETTLEMENT_PROCESSOR` | Dónde | Qué pasa con un lote correcto |
|---|---|---|
| `none` | Default del código | Queda en `CLOSED`, sin enviar |
| `mock` | Docker Compose | Pasa a `SUBMITTED`, **sin enviar nada** |

La liquidación (`SETTLED`) la confirma un operador con `settle`.

**Las operaciones de escritura exigen token.** Los `curl` de este runbook asumen esta variable, con el mismo
valor que `SETTLEMENT_ADMIN_API_KEY` en el servicio:

```bash
export AUTH="Authorization: Bearer <token>"
```

| Respuesta | Qué significa |
|---|---|
| `401 UNAUTHORIZED` | Falta el token o es incorrecto. Queda `rejected admin request` en el log |
| `503 ADMIN_API_DISABLED` | El servicio no tiene `SETTLEMENT_ADMIN_API_KEY`. Hay que configurarla y reiniciarlo |

**Si Settlement falla justo después de un despliegue con errores sobre `version`, `settlement.outbox`, `settlement.batch_audit_log` o
`settlement.reversal_reviews`,**
falta aplicar las [migraciones](../technical/SETTLEMENT.md#aplicar-la-migración).

Revisado contra el código el 2026-11-05. Detalle técnico en
[SETTLEMENT.md](../technical/SETTLEMENT.md). Puerto HTTP en Docker Compose: `8084`.

| Síntoma | Escenario |
|---|---|
| Lote en `DISPUTED` | [A](#escenario-a--lote-en-disputed) |
| Cierre sin efecto, log `no open batch found` | [B](#escenario-b--cierre-sin-lote-abierto) |
| Lote en `CLOSED` que no avanza | [C](#escenario-c--lote-en-closed-sin-enviar) |
| Aprobaciones sin lote | [D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Reversa anterior a la corrección que no se descontó | [E](#escenario-e--reversas-anteriores-sin-descontar) |
| Lote `OPEN` de días anteriores | [F](#escenario-f--lotes-abiertos-de-días-anteriores) |
| Lote en `SUBMITTED` que no se liquida | [G](#escenario-g--confirmar-la-liquidación) |

## Contexto del flujo

Terminal Gateway publica `posnet.transaction.batch-close.v1`, y `settlement-batch-consumer`
(`POSNET_TRANSACTIONS`, 5 entregas) lo procesa en `BatchHandler.ProcessBatchClose`:

1. Busca el lote `OPEN` del terminal para `batch_date`. Si no hay ninguno, registra un `WARN` y confirma.
2. Calcula los totales del backend y los compara con los del terminal.
3. En una transacción, marca el evento, **guarda el lote en `CLOSED`, o en `DISPUTED` si hay discrepancias**,
   y encola `settlement.batch-closed` en el outbox. Si algo falla, el lote sigue `OPEN` y se reintenta.
4. Con discrepancias, termina.
5. Sin discrepancias: lo envía al procesador. Sin procesador, queda en `CLOSED`.

El Relay publica el evento a NATS por separado, con reintentos.

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

- Hay **un solo lote `OPEN` por terminal, día de negocio y moneda**. Puede haber otros lotes de esa misma
  combinación en otros estados: un terminal que sigue vendiendo tras un cierre abre un lote nuevo.
- El día de negocio se calcula con `SETTLEMENT_BUSINESS_TIMEZONE` (Argentina por defecto).
- El cierre usa el `batch_date` del pedido. Sin él, usa el día de negocio del pedido.
- `terminal_count` cuenta compras y reversas, y `terminal_amount` es el monto neto.

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
marked as disputed` o `resubmit blocked by discrepancies`.

**Causa:** la cantidad o el monto del terminal no coinciden con el backend. Las más comunes:

| Causa | Cómo reconocerla |
|---|---|
| Reversa de antes de la corrección | [Escenario E](#escenario-e--reversas-anteriores-sin-descontar) |
| Aprobaciones que no llegaron al lote | [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| El terminal no reinició sus contadores | Un cierre anterior ese día, con el mismo total acumulado |
| El terminal contó solo las compras | La diferencia de cantidad coincide con las reversas del lote |
| Totales enviados incorrectamente por el terminal | El backend coincide con `pn_authorization.transactions` |
| Un `force-close` anterior a la corrección | Discrepancias falsas: comparaba contra 0. Los actuales no generan discrepancias |

Una compra o su reversa no pueden contarse dos veces: `(transaction_id, tx_type)` es único.

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
  AND (COALESCE(authorized_at, created_at) AT TIME ZONE 'America/Argentina/Buenos_Aires')::date = '<batch_date>'
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
curl -X POST http://<host>:8084/batches/<batch_id>/resolve-dispute -H "$AUTH" \
  -d '{"operator_id":"<tu_usuario>","reason":"<motivo o ticket>"}'

curl -X POST http://<host>:8084/batches/<batch_id>/reopen -H "$AUTH" \
  -d '{"operator_id":"<tu_usuario>","reason":"<motivo o ticket>"}'
```

**`reopen` responde `400` si ya hay otro lote `OPEN`** de ese terminal, día y moneda. En ese caso, usá
`resolve-dispute`.

**En lotes de otro día,** el nuevo cierre debe informar `batch_date`. Si no llega, lo cerrará el job
automático en su corrida del día siguiente, **sin conciliar**.

Ninguna de las dos publica un nuevo `batch-closed`: si los totales cambian, avisale al comercio por otro
medio.

---

## Escenario B — Cierre sin lote abierto

**Síntoma:** el terminal pidió el cierre y no cambió nada. Log: `no open batch found for terminal —
nothing to close`. El mensaje se confirma, sin reintento, y el cliente ya había recibido `202`.

**Causas típicas:**

- El lote ya estaba cerrado: un reintento del terminal o de un reprocesamiento.
- No hubo transacciones de esa moneda en esa fecha.
- Un `batch_date` distinto al de las ventas: el lote de las ventas sigue `OPEN`
  ([Escenario F](#escenario-f--lotes-abiertos-de-días-anteriores)).

**Diagnóstico:**

```sql
SELECT id, state, batch_date, currency, closed_at FROM settlement.settlement_batches
WHERE terminal_id = '<terminal_id>' ORDER BY batch_date DESC, created_at DESC LIMIT 5;
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
| `discrepancies` mayor que 0 | Viene de una versión anterior: un `force-close` antes de la corrección, o un fallo al guardar `DISPUTED` durante el cierre |
| Pasó por `force-close` | Un cierre forzado no envía el lote: hay que hacer `resubmit` |
| Lote sin transacciones | Lote vacío cerrado por el job. No requiere acción |

**Resolución:**

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/resubmit -H "$AUTH" -d '{"operator_id":"<tu_usuario>"}'
```

| Respuesta | Qué significa | Qué hacer |
|---|---|---|
| `200` | Enviado; pasa a `SUBMITTED` | Continuar con el Escenario G |
| `503 PROCESSOR_UNAVAILABLE` | No hay procesador configurado | El lote queda en `CLOSED`, sin riesgo. **No lo fuerces:** esperá a tener un procesador, o activá `mock` solo en desarrollo |
| `400`, "moved to DISPUTED" | Tenía discrepancias y **no se envió** | Escenario A |
| `400`, otro estado | El lote no está en `CLOSED` | Verificá su estado |
| `409` | Otro proceso modificó el lote | Consultá el estado y repetí |
| `500` | Falló el procesador o la base de datos | Revisá los logs y reintentá |

**Riesgo cuando exista un procesador real:** si el procesador acepta y falla el guardado, el lote queda en
`CLOSED`, y `resubmit` lo enviaría dos veces. Antes de reintentar un `500`, confirmá con el procesador si
lo recibió.

---

## Escenario D — Aprobaciones fuera de todo lote

**Síntoma:** transacciones `APPROVED` sin fila en `batch_transactions`. Una venta posterior al cierre ya no
causa esto: abre un lote nuevo.

**Causas:**

| Causa | Cómo reconocerla |
|---|---|
| `auth.approved` no se publicó | `failed to publish approval event` en Authorization; el evento no está en el stream |
| Settlement no pudo procesar el evento | `transient error` repetido en `settlement-auth-consumer`, hasta agotar 5 entregas |
| Moneda inválida o monto no positivo | `permanent error` con `invalid currency` o `amount_cents`: se descartó |

**Diagnóstico:**

```sql
SELECT t.id, t.terminal_id, t.amount_cents, t.currency, t.authorized_at
FROM pn_authorization.transactions t
LEFT JOIN settlement.batch_transactions bt ON bt.transaction_id = t.id AND bt.tx_type = 'PURCHASE'
WHERE t.state IN ('APPROVED', 'REVERSED') AND bt.transaction_id IS NULL
ORDER BY t.authorized_at DESC;
```

**Resolución:**

1. **Republicá el `auth.approved` original** con [nats-dlq-reprocess.md](nats-dlq-reprocess.md). La venta entra
   en el lote `OPEN` de su día y moneda, o abre uno nuevo: no hace falta reabrir ningún lote. No debería
   existir una fila en `settlement.processed_events`, porque la transacción se deshace al fallar.
2. **Si el evento no está en el stream,** no hay un procedimiento seguro: coordinalo con el equipo.
3. **El lote nuevo se cierra** con un nuevo pedido del terminal, con su `batch_date`, o con el job automático
   al día siguiente.

---

## Escenario E — Reversas anteriores sin descontar

**Síntoma:** una venta anulada **antes de esta corrección** sigue sumando. Las reversas nuevas se guardan con
su monto real.

**Causa:** antes, la fila `REVERSAL` se descartaba sin error. Su evento quedó en
`settlement.processed_events`, de modo que republicarlo no tiene efecto.

**Diagnóstico:**

```sql
-- Transacciones revertidas sin fila de reversa
SELECT p.batch_id, b.state AS batch_state, p.transaction_id, p.amount_cents, t.state AS tx_state
FROM settlement.batch_transactions p
JOIN settlement.settlement_batches b ON b.id = p.batch_id
JOIN pn_authorization.transactions t ON t.id = p.transaction_id
LEFT JOIN settlement.batch_transactions r
       ON r.transaction_id = p.transaction_id AND r.tx_type = 'REVERSAL'
WHERE t.state = 'REVERSED' AND p.tx_type = 'PURCHASE' AND r.transaction_id IS NULL
ORDER BY b.batch_date DESC;
```

**Resolución:** para registrarla, con aprobación, borrá la marca del evento y republicá el
`auth.reversal-completed` original con [nats-dlq-reprocess.md](nats-dlq-reprocess.md). La reversa entra en
el lote abierto del momento.

```sql
BEGIN;
DELETE FROM settlement.processed_events WHERE event_id = '<event_id>' RETURNING *;
-- COMMIT;  o ROLLBACK;
```

Si la compra ya estaba en un lote `SUBMITTED` o `SETTLED`, el descuento recién impacta en un lote posterior:
confirmá con el equipo que ese sea el tratamiento correcto.

---

## Escenario F — Lotes abiertos de días anteriores

**Síntoma:** lotes `OPEN` con `batch_date` pasada. Lo normal es que no persistan: el job automático los
cierra.

**Cómo funciona el job:** corre desde las **03:00 UTC (00:00 en Argentina)** y al arrancar el servicio.
Cierra los lotes de días de negocio anteriores con los totales del backend, **sin conciliar con el terminal**,
encola `batch-closed` y los envía al procesador. Log: `batch closed automatically without terminal
totals`.

**Diagnóstico:**

```sql
SELECT b.id, b.terminal_id, b.batch_date, count(bt.id) AS tx_count, sum(bt.amount_cents) AS amount
FROM settlement.settlement_batches b
LEFT JOIN settlement.batch_transactions bt ON bt.batch_id = b.id
WHERE b.state = 'OPEN' AND b.batch_date < (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
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

**Si no querés esperar:** `POST /batches/{id}/force-close` (ver abajo). Después hay que enviarlo con `resubmit`.

---

## Escenario G — Confirmar la liquidación

**Síntoma:** lote en `SUBMITTED`. **Nada lo avanza solo:** no hay un procesador que confirme la
liquidación. Con `mock`, además, el lote nunca se envió.

Una vez confirmada por fuera del sistema:

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/settle -H "$AUTH" -d '{"operator_id":"<tu_usuario>"}'
```

Responde `200` y pasa a `SETTLED`, que es un estado final. Sobre otro estado responde `400`.

Al liquidar el último lote con transacciones del comercio en esa fecha, se encola
`settlement.completed`, con el bruto, el neto y el MDR. **Hoy ningún servicio lo consume,** así que el
comercio no recibe ese aviso.

```sql
-- Lotes del comercio y fecha que faltan liquidar
SELECT id, terminal_id, state FROM settlement.settlement_batches
WHERE merchant_id = '<merchant_id>' AND batch_date = '<batch_date>' AND state <> 'SETTLED';
```

Los lotes vacíos no impiden que se publique el evento.

El evento se guarda junto con la liquidación. Si no se puede guardar, `settle` responde `500`, el lote sigue
en `SUBMITTED` y se puede repetir. Con NATS caído, el evento espera en el outbox y sale solo.

---

## Escenario H — Reversas en revisión

**Síntoma:** alerta `ReversasEnRevision`, o `reversal registered for review` en los logs.

**Qué significa:** la reversa ya se descontó, con el monto del evento, pero no coincide con su compra. **El lote
sigue su ciclo normal:** si el monto está mal, el total enviado también.

```sql
-- Revisiones abiertas, con la compra tal como está ahora
SELECT r.transaction_id, r.reason, r.created_at, r.batch_id, b.state AS batch_state,
       r.merchant_id, r.reversal_amount, r.reversal_currency,
       t.amount_cents AS purchase_amount, t.currency AS purchase_currency
FROM settlement.reversal_reviews r
JOIN settlement.settlement_batches b ON b.id = r.batch_id
LEFT JOIN settlement.batch_transactions t ON t.transaction_id = r.transaction_id AND t.tx_type = 'PURCHASE'
WHERE r.resolved_at IS NULL
ORDER BY r.created_at;
```

| `reason` | Qué revisar |
|---|---|
| `PURCHASE_NOT_FOUND`, con `purchase_amount` completo y coincidente | **Falso positivo:** reversa y compra se procesaron a la vez. Cerrar la revisión |
| `PURCHASE_NOT_FOUND`, sin compra | Falta la venta en Settlement: [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote). Si llega y coincide, la revisión se cierra sola |
| `AMOUNT_MISMATCH`, `CURRENCY_MISMATCH` | Comparar con la transacción en Authorization: hoy siempre revierte el monto completo, así que apunta a un error |
| `MERCHANT_MISMATCH` | **Puede descontarle a un comercio una venta de otro.** Escalar antes de enviar el lote |

Si el lote de `batch_id` aún no se envió y el monto es incorrecto, corregirlo es una decisión financiera: ver
[Modificar un lote a mano](#modificar-un-lote-a-mano). Después, cerrar la revisión:

```sql
UPDATE settlement.reversal_reviews
SET resolved_at = now(), resolved_by = '<operador>', resolution = '<qué se hizo, ticket>'
WHERE transaction_id = '<transaction_id>' AND resolved_at IS NULL
RETURNING transaction_id, reason, resolved_by;
```

Cerrar la revisión no cambia ningún monto.

---

## Referencia — endpoints

| Endpoint | Sobre qué estado | Resultado |
|---|---|---|
| `GET /batches/{id}` | Cualquiera | Estado y totales. Un lote `OPEN` muestra totales en 0. Los campos están en PascalCase (`State`, `TotalCount`) |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Cualquiera | Lotes del comercio, que pueden ser varios por terminal; `date` es obligatorio |
| `POST /batches/{id}/force-close` | Solo `OPEN`; otros, `400` | Pasa a `CLOSED` |
| `POST /batches/{id}/resubmit` | Solo `CLOSED`; otros, `400` | Pasa a `SUBMITTED`, o a `DISPUTED` si tiene discrepancias. Sin procesador, `503` |
| `POST /batches/{id}/reopen` | Solo `DISPUTED`; otros, `400` | Pasa a `OPEN` |
| `POST /batches/{id}/resolve-dispute` | Solo `DISPUTED`; otros, `400` | Pasa a `CLOSED`, con los totales del backend |
| `POST /batches/{id}/settle` | Solo `SUBMITTED`; otros, `400` | Pasa a `SETTLED` |

Todos los `POST` exigen el token y `operator_id`; `reopen` y `resolve-dispute`, también `reason`. Esos datos
quedan en la [auditoría](../technical/SETTLEMENT.md#auditoría-de-operadores), pero `operator_id` lo declara
quien llama: **anotá también en el ticket quién hizo la operación**. Un `500` puede indicar que falta la
migración de auditoría: el lote no cambió. Un `409` indica que otro proceso modificó el lote: hay
que consultarlo y repetir. Un `{id}` que no sea un UUID responde `400 INVALID_ID`.

**Si `resubmit` responde `500`, buscá `accepted by processor but not saved` en el log antes de repetirlo:** el
procesador pudo haber aceptado el lote, y repetir lo enviaría dos veces.

```sql
-- Quién hizo qué sobre un lote
SELECT created_at, action, operator_id, reason, from_state, to_state
FROM settlement.batch_audit_log WHERE batch_id = '<batch_id>' ORDER BY created_at;
```

**Sobre `force-close`:**

- Cierra con los totales del backend, **sin conciliar**: asumís que son correctos.
- Encola `batch-closed`, salvo que el lote esté vacío.
- No envía el lote: hay que hacer `resubmit`.
- Las ventas posteriores del terminal abren un lote nuevo, que hay que volver a cerrar.

### Lotes que necesitan atención

```sql
SELECT id, terminal_id, merchant_id, batch_date, state, discrepancies, closed_at, submitted_at
FROM settlement.settlement_batches
WHERE state IN ('DISPUTED', 'CLOSED', 'SUBMITTED')
   OR (state = 'OPEN' AND batch_date < (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date)
ORDER BY batch_date DESC;
```

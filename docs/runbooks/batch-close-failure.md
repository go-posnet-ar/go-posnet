# Runbook: Fallos en el cierre de lote (Settlement)

## Antes de empezar — qué se puede resolver hoy

**El ciclo del lote termina en `CLOSED` o `DISPUTED`.** No hay procesador externo, así que ningún lote
llega a `SUBMITTED` ni a `SETTLED`. Este runbook sirve para diagnosticar, proteger los datos y dejar los
lotes en condiciones de seguir cuando exista el procesador. No esperes "destrabar" un lote hasta
`SETTLED`.

Revisado contra el código el 2026-11-05. Detalle técnico en
[SETTLEMENT.md](../technical/SETTLEMENT.md). Puerto HTTP en Docker Compose: `8084`.

| Síntoma | Escenario |
|---|---|
| Lote en `DISPUTED` | [A](#escenario-a--lote-en-disputed) |
| Cierre sin efecto, log `no open batch found` | [B](#escenario-b--cierre-sin-lote-abierto) |
| Settlement se reinició; lote en `CLOSED` | [C](#escenario-c--caída-del-servicio-al-cerrar-un-lote-correcto) |
| Aprobaciones sin lote, log `reload after conflict` | [D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Reversa que no se descontó | [E](#escenario-e--reversas-sin-descontar) |
| Lote `OPEN` de días anteriores | [F](#escenario-f--lotes-abiertos-de-días-anteriores) |

## Contexto del flujo

Terminal Gateway publica `posnet.transaction.batch-close.v1`, y `settlement-batch-consumer`
(`POSNET_TRANSACTIONS`, 5 entregas) lo procesa en `BatchHandler.ProcessBatchClose`:

1. Busca el lote `OPEN` del terminal para `batch_date`. Si no hay ninguno, registra un `WARN` y confirma.
2. Calcula los totales del backend y los compara con los del terminal.
3. Marca el evento y **guarda el lote en `CLOSED`**.
4. Publica `settlement.batch-closed`. Si falla, solo queda un log.
5. Con discrepancias: guarda `DISPUTED` y termina.
6. Sin discrepancias: llama al procesador, que es `nil`, y **el proceso cae** (Escenario C).

```
OPEN ──► CLOSED ──► DISPUTED
            │
            └──► panic (sin discrepancias)
```

`PENDING_CLOSE` existe, pero no se guarda nunca: el cierre lo atraviesa en un solo paso. `SUBMITTED` y
`SETTLED` no se alcanzan.

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
marking as disputed`.

**Causa:** la cantidad o el monto del terminal no coinciden con el backend. Las más comunes:

| Causa | Cómo reconocerla |
|---|---|
| El terminal descontó una reversa y el backend no | [Escenario E](#escenario-e--reversas-sin-descontar) |
| Aprobaciones que no llegaron al lote | [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Ventas de otra fecha UTC | Cierres cerca de las 21:00 en UTC-3: [Escenario F](#escenario-f--lotes-abiertos-de-días-anteriores) |
| Totales enviados incorrectamente por el terminal | El backend coincide con `pn_authorization.transactions` |

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

**Resolución:** **no hay ningún endpoint que saque un lote de `DISPUTED`.** `force-close` responde `500`
y `resubmit` responde `400`.

1. Documentá la diferencia y su causa.
2. Si la causa está corregida o los datos se pueden corregir, y hace falta volver a cerrar, reabrí el lote
   con aprobación:

   ```sql
   BEGIN;
   UPDATE settlement.settlement_batches
   SET state = 'OPEN', closed_at = NULL, discrepancies = 0,
       total_count = NULL, total_amount = NULL,
       purchase_count = NULL, purchase_amount = NULL,
       reversal_count = NULL, reversal_amount = NULL
   WHERE id = '<batch_id>' AND state IN ('DISPUTED', 'CLOSED')
   RETURNING id, state;
   -- COMMIT;  o ROLLBACK;
   ```

3. Un nuevo cierre con los totales correctos **hace caer el servicio** mientras no exista el procesador
   (Escenario C). Si no hay necesidad de cerrar ahora, dejá el lote `OPEN` o en `DISPUTED`, y registralo.

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
| Lote `CLOSED` o `DISPUTED` en esa fecha | El cierre ya se había procesado: no hay nada que hacer |
| Lote `OPEN` de otra fecha | Escenario F |
| Sin lote en esa fecha, pero con ventas aprobadas | Faltan eventos: Escenario D |

---

## Escenario C — Caída del servicio al cerrar un lote correcto

**Síntoma:** `posnet-settlement` se reinició, y el lote quedó en `CLOSED` con `discrepancies = 0`.

```bash
docker inspect -f '{{.RestartCount}} {{.State.StartedAt}}' posnet-settlement
docker logs posnet-settlement 2>&1 | grep -A15 '^panic:'
```

Vas a ver `invalid memory address or nil pointer dereference`, con `submitBatch` en el stack.

**Causa:** el procesador es `nil` en `cmd/settlement/st_wire.go`, a pesar del comentario que dice que es
un no-op. Como el panic ocurre en un handler de NATS, nada lo recupera. **Es el comportamiento de todo
cierre sin discrepancias.**

Ya al reiniciar:

- El lote está guardado en `CLOSED`, y `batch-closed` ya se publicó.
- La reentrega no encuentra un lote `OPEN`, de modo que se confirma sin hacer nada. El servicio no queda
  en un bucle de caídas.
- Los mensajes que estaban en curso en otros consumidores se reentregan, y la idempotencia los protege.

**Confirmá el diagnóstico:**

```sql
SELECT id, state, discrepancies, closed_at, submitted_at
FROM settlement.settlement_batches WHERE id = '<batch_id>';
```

**Resolución:** **no hay ninguna hoy, y no hace falta intervenir el lote.** Sus totales son correctos y
coinciden con los del terminal; queda en `CLOSED` hasta que exista el procesador.

- **No uses `resubmit`:** siempre responde `500`, porque llama al mismo procesador `nil` (en este caso, el
  middleware HTTP captura el panic). No hace daño, pero tampoco avanza.
- **No uses `force-close`:** exige `OPEN`.
- Para evitar nuevas caídas hay que corregir el código. Mientras tanto, cada cierre correcto provoca un
  reinicio breve.

Cuando exista el procesador, `resubmit` será la vía para reintentar un lote `CLOSED`. Antes de usarlo,
verificá que `discrepancies` sea 0: el endpoint no lo controla.

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

1. **Si el lote de esa fecha no está `OPEN`,** el reprocesamiento volverá a fallar. Para incluir la venta,
   hay que reabrirlo como en el [Escenario A](#escenario-a--lote-en-disputed), con aprobación. Evaluá
   antes el impacto: al reabrir un lote `CLOSED` correcto, su nuevo cierre con totales correctos provoca la
   caída del Escenario C. Si no es urgente, registrá las transacciones y esperá la corrección del código.
2. **Republicá el `auth.approved` original** con [nats-dlq-reprocess.md](nats-dlq-reprocess.md). No debería
   existir una fila en `settlement.processed_events`, porque la transacción se deshace al fallar.
3. **Si el evento no está en el stream,** no hay un procedimiento seguro: coordinalo con el equipo.

Mientras esto no se corrija en el código, seguir vendiendo después de cerrar el lote el mismo día UTC
vuelve a producir el problema. Evitá también `force-close` en lotes de terminales activos.

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

Una reversa en otra fecha UTC también crea un lote `OPEN` vacío: ver el Escenario F.

---

## Escenario F — Lotes abiertos de días anteriores

**Síntoma:** lotes `OPEN` con `batch_date` pasada. **No hay cierre automático:** el job de las 23:00 UTC
solo escribe un log, y `SETTLEMENT_BATCH_CLOSE_HOUR` no tiene efecto.

**Causas:**

- El terminal no pidió el cierre.
- El terminal cerró en otra fecha UTC. Para uno en UTC-3, las ventas posteriores a las 21:00 caen en el
  lote siguiente, y un cierre posterior a esa hora no alcanza el lote del día.
- Una reversa llegó en otra fecha y creó un lote vacío.

**Diagnóstico:**

```sql
SELECT b.id, b.terminal_id, b.batch_date, count(bt.id) AS tx_count, sum(bt.amount_cents) AS amount
FROM settlement.settlement_batches b
LEFT JOIN settlement.batch_transactions bt ON bt.batch_id = b.id
WHERE b.state = 'OPEN' AND b.batch_date < (NOW() AT TIME ZONE 'UTC')::date
GROUP BY b.id, b.terminal_id, b.batch_date
ORDER BY b.batch_date;
```

**Resolución:** `POST /batches/{id}/force-close` puede cerrarlos, con las limitaciones de abajo. Un lote
`OPEN` con transacciones no se pierde: conserva sus datos hasta que se decida qué hacer.

---

## Referencia — endpoints

| Endpoint | Sobre qué estado | Resultado real |
|---|---|---|
| `GET /batches/{id}` | Cualquiera | Estado y totales. Un lote `OPEN` muestra totales en 0. Los campos están en PascalCase (`State`, `TotalCount`) |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Cualquiera | Lotes del comercio; `date` es obligatorio |
| `POST /batches/{id}/force-close` | Solo `OPEN`; otros, `500` | Pasa a `CLOSED` |
| `POST /batches/{id}/resubmit` | Solo `CLOSED`; otros, `400` | Hoy siempre `500` |

Ambos `POST` exigen `{"operator_id": "<usuario>"}` y **no tienen autenticación**.

**Limitaciones de `force-close`:**

- Compara contra 0: **todo lote con transacciones queda con discrepancias falsas**, aunque esté `CLOSED`.
- No publica `batch-closed`, así que Notification no se entera.
- No guarda ni registra `operator_id`: anotá en el ticket quién lo ejecutó y por qué.
- Bloquea el lote de esa fecha: las aprobaciones posteriores del terminal caen en el Escenario D.

### Lotes que necesitan atención

```sql
SELECT id, terminal_id, merchant_id, batch_date, state, discrepancies, closed_at, submitted_at
FROM settlement.settlement_batches
WHERE state IN ('DISPUTED', 'CLOSED')
   OR (state = 'OPEN' AND batch_date < (NOW() AT TIME ZONE 'UTC')::date)
ORDER BY batch_date DESC;
```

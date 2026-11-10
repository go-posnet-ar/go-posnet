# Runbook: Fallos en el cierre de lote (Settlement)

## Antes de empezar — qué se puede resolver hoy

El ciclo del lote tiene salidas por API para cada estado, pero **no hay un procesador real**:

| `SETTLEMENT_PROCESSOR` | Dónde | Qué pasa con un lote correcto |
|---|---|---|
| `none` | Default del código | Queda en `CLOSED`, sin enviar |
| `mock` | Docker Compose | Pasa a `SUBMITTED`, **sin enviar nada** |

La liquidación (`SETTLED`) la confirma un operador con `settle`.

**Toda la API exige token, salvo `/healthz`, `/readyz` y `/metrics`.** Los `curl` de este runbook asumen esta variable,
con [tu token](../technical/SETTLEMENT.md#tokens-y-roles):

```bash
export AUTH="Authorization: Bearer <token>"
```

| Respuesta | Qué significa |
|---|---|
| `401 UNAUTHORIZED` | Falta el token, es incorrecto o venció. Queda `rejected admin request` en el log |
| `403 FORBIDDEN` | Tu rol no alcanza: `force-close`, `resubmit`, `reopen` y el reproceso de la DLQ piden `operator`; lo demás que modifica, `supervisor` |
| `403 OPERATOR_MISMATCH` | El `operator_id` del body no es el de tu token. Omitilo |
| `429 TOO_MANY_ATTEMPTS` | Demasiados tokens incorrectos desde tu IP. Esperá lo que indica `Retry-After`, hasta 5 minutos |
| `503 ADMIN_API_DISABLED` | El servicio no tiene tokens configurados. Hay que definir `SETTLEMENT_ADMIN_TOKENS_FILE` y reiniciarlo |

**Si Settlement falla justo después de un despliegue con errores sobre `version`, `settlement.outbox`, `settlement.batch_audit_log` o
`settlement.reversal_reviews` o `settlement.merchant_timezones`,**
falta aplicar las [migraciones](../technical/SETTLEMENT.md#aplicar-la-migración). `/readyz` lo confirma: responde `503`
con `database schema incomplete`, y Docker muestra el contenedor `unhealthy`.

Revisado contra el código el 2026-11-10. Detalle técnico en
[SETTLEMENT.md](../technical/SETTLEMENT.md). Puerto HTTP en Docker Compose: `8084`.

| Síntoma | Escenario |
|---|---|
| Lote en `DISPUTED` | [A](#escenario-a--lote-en-disputed) |
| Cierre sin efecto, logs `no open batch for the terminal` o `terminal reported sales for a day without any batch`, alerta `CierreSinLote` | [B](#escenario-b--cierre-sin-lote-abierto) |
| Lote en `CLOSED` que no avanza | [C](#escenario-c--lote-en-closed-sin-enviar) |
| Aprobaciones sin lote | [D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| Reversa anterior a la corrección que no se descontó | [E](#escenario-e--reversas-anteriores-sin-descontar) |
| Lote `OPEN` de días anteriores | [F](#escenario-f--lotes-abiertos-de-días-anteriores) |
| Lote en `SUBMITTED` que no se liquida | [G](#escenario-g--confirmar-la-liquidación) |
| Eventos en la DLQ, alerta `EventosEnDLQSettlement` | [I](#escenario-i--eventos-en-la-dlq) |

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
- El día de negocio se calcula con la zona del comercio o, si no tiene una, con `SETTLEMENT_BUSINESS_TIMEZONE`
  (Argentina por defecto). Se consulta con `GET /merchants/{merchant_id}/timezone`.
- El cierre usa el `batch_date` del pedido. Sin él, usa el día de negocio del pedido. Se descarta un `batch_date`
  futuro, más antiguo que `SETTLEMENT_CLOSE_MAX_AGE_DAYS`, o anterior a ayer de un lote que nadie reabrió: en el
  log, `permanent error` y `batch_date ...`, y la métrica `posnet_settlement_batch_close_rejected_total{reason}`.
  Para que el terminal vuelva a cerrar un lote de hace días, primero hay que reabrirlo con `reopen`.
- Un lote con una reversa en revisión no se envía: queda en `DISPUTED`.
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
marked as disputed`, `batch held — reversals under review` o `resubmit blocked — batch moved to DISPUTED`.

**Si `Discrepancies` es 0, el lote está retenido por una reversa en revisión:** ir al
[Escenario H](#escenario-h--reversas-en-revisión).

**Causa:** la cantidad o el monto del terminal no coinciden con el backend. Las más comunes:

Una venta que el terminal contó pero llegó después del cierre, por un reintento o desde la DLQ, **se agrega sola** al
lote en disputa (log `late sale added to its disputed batch`, auditoría `LATE_SALE`). Si con ella coincide, el lote pasa
a `CLOSED` y se envía. Esperá unos 12 minutos antes de intervenir en una disputa recién cerrada, y revisá la DLQ.

| Causa | Cómo reconocerla |
|---|---|
| Reversa de antes de la corrección | [Escenario E](#escenario-e--reversas-anteriores-sin-descontar) |
| Aprobaciones que no llegaron al lote | [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote) |
| El terminal no reinició sus contadores, y un lote anterior de ese día está en `DISPUTED` | El acumulado exacto se concilia solo, salvo que incluya un lote en disputa, que no cuenta. Resolver primero ese lote |
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

**Síntoma:** el terminal pidió el cierre y no cambió nada. El mensaje se confirma, sin reintento, y el cliente ya
había recibido `202`. El log y `posnet_settlement_batch_close_rejected_total{reason}` dicen por qué:

| Log | `reason` | Causa típica |
|---|---|---|
| `no open batch for the terminal — the day's batches are already closed` (`WARN`) | `already_closed` | Un reintento del terminal o de un reprocesamiento: el cierre ya se había procesado |
| `terminal reported sales for a day without any batch` (`ERROR`, alerta `CierreSinLote`) | `no_batch` | El terminal informó ventas que Settlement no tiene: se están reintentando, están en la [DLQ](#escenario-i--eventos-en-la-dlq), o no llegaron |
| `no open batch for the terminal and no sales reported` (`INFO`) | — | Un día sin ventas en esa moneda. No hay nada que hacer |

Un `batch_date` distinto al de las ventas también da `no_batch`: el lote de las ventas sigue `OPEN`
([Escenario F](#escenario-f--lotes-abiertos-de-días-anteriores)).

**Con `no_batch`, los totales del terminal están en el log** (`terminal_count`, `terminal_amount`): no se guardan en
ningún otro lado. Anotalos. Si las ventas llegan después, abren un lote nuevo que el job cierra sin conciliar, porque
no hay un lote en disputa que las espere.

**Diagnóstico:**

```sql
SELECT id, state, batch_date, currency, closed_at FROM settlement.settlement_batches
WHERE terminal_id = '<terminal_id>' ORDER BY batch_date DESC, created_at DESC LIMIT 5;
```

| Resultado | Qué significa |
|---|---|
| Lote cerrado o posterior en esa fecha | El cierre ya se había procesado: no hay nada que hacer |
| Lote `OPEN` de otra fecha | Escenario F |
| Sin lote en esa fecha, pero con ventas aprobadas | Faltan eventos: revisá la DLQ (Escenario I) y después el Escenario D |

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
| Settlement no pudo procesar el evento | `transient error` repetido en `settlement-auth-consumer`, hasta agotar 5 entregas. Si después aparece `moved to the dead letter queue`, está en la DLQ: [Escenario I](#escenario-i--eventos-en-la-dlq) |
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

1. **Si está en la DLQ, reprocesalo desde ahí** ([Escenario I](#escenario-i--eventos-en-la-dlq)): no le llega a los
   otros consumidores. Si no, **republicá el `auth.approved` original** con [nats-dlq-reprocess.md](nats-dlq-reprocess.md). La venta entra
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
`settlement.processed_events`, de modo que republicarlo no tiene efecto, y puede ya no estar en el stream.

**Diagnóstico:** el comando lista las ventas `REVERSED` en Authorization sin reversa en Settlement, con el lote y
el estado de su compra. **Sin `-apply`, no cambia nada.** Necesita leer `pn_authorization.transactions`.

```bash
POSTGRES_DSN=... go run ./cmd/settlement-recover-reversals
```

**Resolución:** registrar las reversas es una decisión financiera y **requiere aprobación**. Revisá antes la lista:
si la compra ya está en un lote `SUBMITTED` o `SETTLED`, confirmá con el equipo que el tratamiento sea el correcto.

```bash
POSTGRES_DSN=... go run ./cmd/settlement-recover-reversals -apply -operator <tu_usuario> -reason "<ticket>"
```

Usá la misma `SETTLEMENT_BUSINESS_TIMEZONE` que el servicio, si no es la de Argentina. Cada reversa entra por el flujo
normal, **en el día de negocio de la compra**, y no en el lote de hoy, cuyo cierre no la espera:

| Lote de ese día | Resultado |
|---|---|
| Hay uno `OPEN` | La reversa entra ahí, y el lote queda con el monto neto |
| Ya no hay uno `OPEN` | Abre un lote nuevo de esa fecha, que **el job automático cierra y envía sin conciliar**, con total negativo. Dentro de `SETTLEMENT_CLOSE_MAX_AGE_DAYS`, también se puede cerrar con un pedido del terminal |

Se puede repetir: lo ya registrado deja de aparecer. Procesa hasta 500 por ejecución, o lo que indique `-limit`.
Cada reversa queda en la auditoría como `RECOVER_REVERSAL`. Si el comando informa `reversal registered but not
audited`, la reversa está registrada y falta asentarla a mano en el ticket.

**Cuándo queda hecho:** cuando no queda ninguna reversa sin registrar, tras un `-apply` o en una ejecución que no
encuentra nada, el comando lo deja en `settlement.maintenance_runs` (`Recorded in settlement.maintenance_runs`).
Hasta entonces, `posnet_settlement_reversal_recovery_pending` vale 1 y salta la alerta `ReversasSinRecuperar`. Si
quedan más que `-limit`, avisa `More reversals remain`: volvé a ejecutarlo. Una base creada con `01-init.sql` ya
viene registrada, porque no puede tener reversas anteriores.

```sql
SELECT name, completed_at, completed_by, details FROM settlement.maintenance_runs;
```

---

## Escenario F — Lotes abiertos de días anteriores

**Síntoma:** lotes `OPEN` con `batch_date` pasada. Lo normal es que no persistan: el job automático los
cierra.

**Cómo funciona el job:** revisa cada 15 minutos, todo el día, y al arrancar el servicio. Cierra los lotes de días
de negocio anteriores una vez pasada la **medianoche de su comercio más `SETTLEMENT_BATCH_CLOSE_GRACE`** (30 min por
defecto), con los totales del backend, **sin conciliar con el terminal**,
encola `batch-closed` y los envía al procesador. Log: `batch closed automatically without terminal
totals`.

**Diagnóstico:**

```sql
SELECT b.id, b.terminal_id, b.batch_date, count(bt.id) AS tx_count, sum(bt.amount_cents) AS amount
FROM settlement.settlement_batches b
LEFT JOIN settlement.batch_transactions bt ON bt.batch_id = b.id
LEFT JOIN settlement.merchant_timezones m ON m.merchant_id = b.merchant_id
WHERE b.state = 'OPEN'
  AND b.batch_date < (NOW() AT TIME ZONE COALESCE(m.timezone, 'America/Argentina/Buenos_Aires'))::date
GROUP BY b.id, b.terminal_id, b.batch_date
ORDER BY b.batch_date;
```

```bash
docker logs posnet-settlement 2>&1 | grep -E 'automatic batch close'
```

| Si el lote sigue `OPEN` | Causa probable |
|---|---|
| `automatic batch close finished` sin este lote | Todavía no pasó la medianoche más el plazo en la zona de su comercio. La alerta tampoco lo cuenta hasta entonces |
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

**Qué significa:** la reversa se descontó con el monto del evento, pero no coincide con su compra. **El lote que
tiene la reversa no se envía** mientras la revisión esté abierta: el cierre lo deja en `DISPUTED` y `resolve-dispute`
responde `400`.

```bash
curl -H "$AUTH" http://<host>:8084/reversal-reviews
```

| `reason` | Qué revisar | `action` habitual |
|---|---|---|
| `PURCHASE_NOT_FOUND` | Falta la venta en Settlement: [Escenario D](#escenario-d--aprobaciones-fuera-de-todo-lote). Si llega y coincide, la revisión se cierra sola | Esperar la compra. Si la venta nunca existió, `EXCLUDE` |
| `AMOUNT_MISMATCH` | Comparar con la transacción en Authorization: hoy siempre revierte el monto completo, así que apunta a un error | `USE_PURCHASE_AMOUNT` |
| `CURRENCY_MISMATCH` | Igual que el anterior | `EXCLUDE` |
| `MERCHANT_MISMATCH` | **Puede descontarle a un comercio una venta de otro.** Escalar antes de corregir | `EXCLUDE` |

`ACCEPT` deja la reversa como está. Corregir un monto es una decisión financiera: **requiere aprobación**.

```bash
curl -X POST http://<host>:8084/reversal-reviews/<transaction_id>/resolve -H "$AUTH" \
  -d '{"operator_id":"<tu_usuario>","resolution":"<qué se hizo, ticket>","action":"USE_PURCHASE_AMOUNT"}'
```

`USE_PURCHASE_AMOUNT` y `EXCLUDE` corrigen la reversa en un lote `OPEN` o `DISPUTED`. Con un lote en `CLOSED`, un
`resubmit` lo pasa a `DISPUTED`. **Con uno `SUBMITTED` o `SETTLED`, el dinero ya salió:** el lote no cambia, y la
corrección entra como un `ADJUSTMENT` en el lote abierto del mismo terminal, que la liquida (log `sent batch corrected
with an adjustment`). Revisá el monto antes: positivo le devuelve al comercio, negativo le descuenta.

**Si la revisión ya se cerró con `ACCEPT`, o la cerró el sistema, y su lote se envió,** se puede volver a resolver con
`USE_PURCHASE_AMOUNT` o `EXCLUDE` para corregirla: genera el ajuste igual. Se puede hacer una sola vez, y requiere la
misma aprobación que cualquier corrección.

```sql
SELECT b.id, b.state, t.amount_cents
FROM settlement.batch_transactions t JOIN settlement.settlement_batches b ON b.id = t.batch_id
WHERE t.transaction_id = '<transaction_id>' AND t.tx_type = 'ADJUSTMENT';
```

**Si `EXCLUDE` fue por `PURCHASE_NOT_FOUND` y la compra llega después,** la reversa vuelve sola si coincide (log
`excluded reversal restored`). Si no coincide, la revisión se reabre en el lote de la compra (log `excluded reversal
does not match`): ahí `ACCEPT` la deja excluida y `USE_PURCHASE_AMOUNT` la agrega con el monto de la compra.

**Al cerrar la última revisión, el lote se libera solo si concilia:** pasa a `CLOSED` y se envía (log `held batch
released`, auditoría `AUTO_RELEASE`). Si no concilia (log `still does not reconcile`, por ejemplo porque el terminal
había contado la reversa que excluiste) o se cerró antes de `000007` sin aplicar `000010` (log `closed before its
reconciliation was recorded`), sigue en `DISPUTED`. Si la liberación falló (log `could not release the held
batch`), el job de cierre automático la reintenta en su barrido siguiente, cada 15 minutos (log `retrying the release
of a held batch`): no hace falta intervenir. Si no concilia:

```bash
curl -X POST http://<host>:8084/batches/<batch_id>/resolve-dispute -H "$AUTH" \
  -d '{"operator_id":"<tu_usuario>","reason":"<ticket>"}'
curl -X POST http://<host>:8084/batches/<batch_id>/resubmit -H "$AUTH" -d '{"operator_id":"<tu_usuario>"}'
```

`resolve-dispute` recalcula los totales con la corrección. Si el terminal puede volver a cerrar, usá `reopen`.

---

## Escenario I — Eventos en la DLQ

**Síntoma:** alerta `EventosEnDLQSettlement`, `posnet_settlement_dead_letters` mayor que 0, log `deliveries exhausted —
event moved to the dead letter queue`.

**Causa:** una venta, reversa o cierre falló las 5 entregas, durante unos 12 minutos, con un error transitorio: casi
siempre la base caída. No se perdió: está en `POSNET_DLQ`, bajo `posnet.dlq.settlement.<subject>`, con el error.
Mientras tanto, falta en los lotes.

**Diagnóstico:**

```bash
curl -s http://<host>:8084/dead-letters -H "$AUTH" | jq '.dead_letters[] | {sequence, subject, event_id, error, failed_at}'
```

**Resolución:**

1. **Confirmá que la causa se resolvió:** `/readyz` en `200`.
2. **Reprocesá cada evento,** del más antiguo al más nuevo. Una reversa antes que su compra queda en revisión
   hasta que llegue la compra ([Escenario H](#escenario-h--reversas-en-revisión)):

   ```bash
   curl -X POST http://<host>:8084/dead-letters/<sequence>/replay -H "$AUTH" -d '{"operator_id":"<tu_usuario>"}'
   ```

   | Respuesta | Qué significa |
   |---|---|
   | `200 replayed` | Se procesó y se borró de la DLQ. Uno ya registrado también da `200`: la idempotencia lo descarta |
   | `422 REPLAY_REJECTED` | Error permanente, como un dato inválido: reintentarlo no lo arregla. Corregí la causa o descartalo |
   | `503 REPLAY_FAILED` | Sigue fallando, y queda en la DLQ. Revisá la causa y repetí |
   | `404` | Ya no está: lo reprocesó o descartó otra persona |

   Reprocesar usa el mismo código que el consumidor, en Settlement: **no le llega a Terminal Gateway ni a
   Notification**, a diferencia de republicar con `nats pub`. Pide el rol `operator`.

3. **Un cierre reprocesado tarde** cierra con los totales del terminal si el lote sigue `OPEN`. Si el job ya lo cerró,
   da `already_closed` y no hace nada ([Escenario B](#escenario-b--cierre-sin-lote-abierto)).
4. **Descartar** pierde el evento: solo con un motivo, y con el rol `supervisor`:

   ```bash
   curl -X POST http://<host>:8084/dead-letters/<sequence>/discard -H "$AUTH" \
     -d '{"operator_id":"<tu_usuario>","reason":"<ticket>"}'
   ```

El reproceso y el descarte quedan solo en los logs (`dead letter replayed by operator`, `dead letter discarded by
operator`), con el operador: `batch_audit_log` registra cambios de lotes.

**Si en vez de `moved to the dead letter queue` el log dice `dead letter could not be published`,** el evento no llegó a
la DLQ y salta `EventoSettlementPerdido`: seguí [nats-dlq-reprocess.md](nats-dlq-reprocess.md).

---

## Referencia — endpoints

| Endpoint | Sobre qué estado | Resultado |
|---|---|---|
| `GET /batches/{id}` | Cualquiera | Estado y totales. Un lote `OPEN` muestra totales en 0. Los campos están en PascalCase (`State`, `TotalCount`) |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Cualquiera | Lotes del comercio, que pueden ser varios por terminal; `date` es obligatorio |
| `POST /batches/{id}/force-close` | Solo `OPEN`; otros, `400` | Pasa a `CLOSED` |
| `POST /batches/{id}/resubmit` | Solo `CLOSED`; otros, `400` | Pasa a `SUBMITTED`, o a `DISPUTED` si tiene discrepancias o una reversa en revisión. Sin procesador, `503` |
| `POST /batches/{id}/reopen` | Solo `DISPUTED`; otros, `400` | Pasa a `OPEN` |
| `POST /batches/{id}/resolve-dispute` | Solo `DISPUTED`, sin reversas en revisión; otros, `400` | Pasa a `CLOSED`, con los totales del backend |
| `POST /batches/{id}/settle` | Solo `SUBMITTED`; otros, `400` | Pasa a `SETTLED` |
| `GET /reversal-reviews` | — | Revisiones abiertas, hasta 200 |
| `POST /reversal-reviews/{transaction_id}/resolve` | Revisión abierta; otras, `400` | Cierra la revisión y corrige el lote si corresponde. Inexistente, `404` |
| `GET /merchants/{merchant_id}/timezone` | — | Zona vigente del comercio |
| `PUT /merchants/{merchant_id}/timezone` | — | Define la zona del comercio. Rige para las operaciones siguientes |
| `GET /dead-letters` | — | Eventos en la DLQ, hasta 100 (`?limit=` hasta 500) |
| `POST /dead-letters/{sequence}/replay` | — | Procesa el evento y lo borra de la DLQ ([Escenario I](#escenario-i--eventos-en-la-dlq)) |
| `POST /dead-letters/{sequence}/discard` | — | Borra el evento sin procesarlo; exige `reason` |

Todos los `POST` y `PUT` exigen un operador, que sale del token o de `operator_id`; `reopen` y `resolve-dispute`, también `reason`. Esos datos
quedan en la [auditoría](../technical/SETTLEMENT.md#auditoría-de-operadores). Con un token del archivo, el operador es
el nombre del token. **Con `SETTLEMENT_ADMIN_API_KEY`, lo declara quien llama: anotá también en el ticket quién hizo la
operación.** Un `500` puede indicar que falta la
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
   OR (state = 'OPEN' AND batch_date < (NOW() AT TIME ZONE COALESCE(
         (SELECT timezone FROM settlement.merchant_timezones m WHERE m.merchant_id = settlement_batches.merchant_id),
         'America/Argentina/Buenos_Aires'))::date)
ORDER BY batch_date DESC;
```

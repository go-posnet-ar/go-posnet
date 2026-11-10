# Runbook: Reprocesamiento de eventos

## Antes de empezar — cómo funciona hoy

**Settlement usa `POSNET_DLQ`:** un evento que agota sus 5 entregas queda en `posnet.dlq.settlement.<subject>`, y se
reprocesa con su API, sin republicar nada: seguí el
[Escenario I de batch-close-failure.md](batch-close-failure.md#escenario-i--eventos-en-la-dlq). Este runbook sigue
sirviendo para Settlement solo si el log dice `dead letter could not be published`: el evento no llegó a la DLQ.

**Los otros BC no publican en `POSNET_DLQ`.** Un mensaje que agota `MaxDeliver` no se
mueve: JetStream deja de entregárselo a ese consumidor, y el mensaje sigue en su stream de origen.

Por eso, "reprocesar" significa **encontrar el mensaje original y volver a publicarlo**. Es un
procedimiento manual, y tiene tres riesgos que conviene entender antes de ejecutarlo:

1. **Le llega a todos los consumidores del subject,** no solo al que falló.
2. **Una fila en `processed_events` no garantiza que el trabajo se haya hecho,** y mientras exista, el
   consumidor descarta el evento republicado.
3. **Un evento que se perdió antes de publicarse no está en el stream:** no hay nada que recuperar.

Hay dos formas de que un mensaje deje de reintentarse:

| Motivo | Qué hizo el subscriber | ¿Reprocesar? |
|---|---|---|
| Mensaje inválido, `ValidationError` o `ConflictError` | `msg.Term()`, sin reintentos | Solo después de corregir la causa |
| Otro error, repetido hasta agotar `MaxDeliver` | `msg.Nak()` en cada intento, sin backoff | Sí, una vez resuelta la causa |

Los reintentos son inmediatos, así que una caída de pocos segundos ya puede agotar las entregas. **Settlement
es la excepción:** espera 5 s, 30 s, 2 min y 10 min entre entregas, así que resiste una caída de unos 12
minutos, y en el último intento pasa el evento a la DLQ.

Los comandos se probaron con el CLI `nats` `v0.4.0`. Requieren también `jq`.

---

## 1. Identificar el evento

### Por logs

Buscá el `event_id` en el servicio dueño del consumidor:

```bash
docker logs posnet-<servicio> 2>&1 | grep '<event_id>'
```

| Mensaje | Significado |
|---|---|
| `permanent error — terminating` | Se descartó con `Term` |
| `transient error — nacking for retry` | Una línea por intento, hasta `MaxDeliver` |
| `failed to publish` | **El evento nunca llegó al stream:** no se puede republicar (ver [sección 6](#6-si-el-evento-no-está-en-el-stream)) |

### Por el consumidor

```bash
nats consumer info <STREAM> <DURABLE> --json \
  | jq '{num_pending, num_redelivered, num_ack_pending, max_deliver: .config.max_deliver}'
```

`num_ack_pending` en 0 y `num_redelivered` mayor que 0 sugiere que hubo mensajes que agotaron sus
entregas. Estos valores no identifican qué mensaje fue: para eso están los logs. La lista de consumidores
está en
[incident-response.md](incident-response.md#31-estado-de-un-consumidor).

---

## 2. Recuperar el mensaje original

El CLI no busca por `event_id`, así que hay que recorrer el stream. Este script empieza por los más
recientes y guarda el mensaje y su subject:

```bash
STREAM=POSNET_AUTH        # Stream de origen
EVENT_ID=<event_id>
OUT=/tmp/msg-$EVENT_ID

STATE=$(nats stream info "$STREAM" --json)
LAST=$(jq '.state.last_seq' <<<"$STATE")
FIRST=$(jq '.state.first_seq' <<<"$STATE")

for ((seq = LAST; seq >= FIRST; seq--)); do
  msg=$(nats stream get "$STREAM" "$seq" --json 2>/dev/null) || continue
  if [ "$(jq -r '.data | @base64d | fromjson | .event_id' <<<"$msg")" = "$EVENT_ID" ]; then
    jq -r '.subject' <<<"$msg" > "$OUT.subject"
    jq -r '.data | @base64d' <<<"$msg" > "$OUT.json"
    echo "encontrado en seq=$seq"
    break
  fi
done
```

Hace una llamada por mensaje, así que puede tardar en un stream grande. Si no tenés el `event_id`,
cambiá la condición para buscar por transacción, con `.data.transaction_id`.

En una terminal interactiva, también podés explorar con
`nats stream view <STREAM> --subject '<subject>' --since 1h`.

Confirmá el contenido antes de seguir:

```bash
cat "$OUT.subject"
jq . "$OUT.json"
jq -e --arg id "$EVENT_ID" '.event_id == $id' "$OUT.json"
```

---

## 3. Confirmar la causa y entender `processed_events`

**No republiques todavía.**

Si la causa era transitoria, confirmá que se haya resuelto: `readyz` del servicio en `200` y NATS
accesible (ver [incident-response.md](incident-response.md)). Si fue un error de validación, el problema
está en el productor, y el mismo payload volverá a fallar.

Después buscá el evento en el schema del servicio **que debe procesarlo**:

```sql
-- Schemas: terminal_gateway, pn_authorization, fraud_detection, settlement, notification
SELECT * FROM settlement.processed_events WHERE event_id = '<event_id>';
```

Mientras exista esa fila, el consumidor descarta el evento republicado. **Qué significa la fila depende
del consumidor:**

| Consumidor | Qué significa la fila | Si existe, ¿qué hacer? |
|---|---|---|
| `auth-txn-receiver` | La transacción se guardó. El pedido de fraude pudo perderse | Si sigue en `FRAUD_CHECKING` sin caso de fraude: borrar la fila y republicar |
| `auth-fraud-score-consumer` | **Se marcó antes de trabajar:** no prueba nada | Si sigue en `FRAUD_CHECKING`: borrar la fila y republicar (ver la advertencia) |
| `auth-reversal-processor` | **Se marcó antes de trabajar:** no prueba nada | Si sigue en `APPROVED`: borrar la fila y republicar (ver la advertencia) |
| `fraud-check-consumer` | El caso se guardó. El score pudo perderse | Si la transacción sigue en `FRAUD_CHECKING`: borrar la fila y republicar. Se evalúa de nuevo: el score publicado puede diferir del caso guardado |
| `gateway-auth-consumer` | El resultado se aplicó a la sesión | Nada |
| `settlement-auth-consumer` | La compra entró al lote | Nada |
| `settlement-reversal-consumer` | La reversa se registró. **Antes de la corrección, se descartaba igual** | Nada, salvo en reversas antiguas: ver el [Escenario E](batch-close-failure.md#escenario-e--reversas-anteriores-sin-descontar) |
| `settlement-batch-consumer` | El lote se cerró | Nada: seguí [batch-close-failure.md](batch-close-failure.md) |
| `notify-*` | Las notificaciones se crearon | No borrar: crearía duplicados. Ver [Notification](../technical/NOTIFICATION.md#reenviar-una-notificación-dead) |

Sin fila, en general se puede republicar directamente.

Para ver el estado de la transacción:

```sql
SELECT id, state, created_at, authorized_at, rejected_at
FROM pn_authorization.transactions WHERE id = '<transaction_id>';
```

Para borrar una fila, cuando la tabla lo indique:

```sql
DELETE FROM pn_authorization.processed_events WHERE event_id = '<event_id>' RETURNING *;
```

> **Advertencia: score y reversa.** Reprocesar estos eventos vuelve a llamar al adquirente. Hoy es un
> mock y no tiene consecuencias. **Con un adquirente real, primero hay que confirmar con él si la
> operación anterior se ejecutó**: `FRAUD_CHECKING` también es el estado que queda si el proceso cayó
> durante la llamada, y reprocesar podría cobrar o revertir dos veces.

---

## 4. Republicar

```bash
nats pub "$(cat "$OUT.subject")" --force-stdin \
  -H "Nats-Msg-Id:reprocess-$EVENT_ID-$(date +%s)" \
  < "$OUT.json"
```

Dos detalles importantes:

- **`--force-stdin` es obligatorio.** Sin él, `nats pub` ignora el archivo y publica un mensaje de **0
  bytes**, que los consumidores descartan como inválido.
- **`Nats-Msg-Id` debe ser nuevo.** Si reutilizás el `event_id` dentro de la ventana de deduplicación
  (2 minutos por defecto), JetStream descarta el mensaje, **y el CLI igual muestra `Published`**. La
  idempotencia de los consumidores usa el `event_id` que está dentro del JSON, que no cambia.

Confirmá que el mensaje entró y qué se publicó:

```bash
STATE=$(nats stream info "$STREAM" --json)
jq '.state.messages' <<<"$STATE"
nats stream get "$STREAM" "$(jq '.state.last_seq' <<<"$STATE")" --json \
  | jq -r '.data | @base64d | fromjson | {event_id, event_type}'
```

### A quién le llega

| Subject | Consumidores | Efecto en los que ya lo procesaron |
|---|---|---|
| `posnet.transaction.received.v1` | Authorization | Se descarta por idempotencia |
| `posnet.fraud.check-requested.v1` | Fraud Detection | Se descarta por idempotencia |
| `posnet.fraud.score-calculated.v1` | Authorization | Se descarta por idempotencia |
| `posnet.transaction.reversal-requested.v1` | Authorization | Se descarta por idempotencia |
| `posnet.auth.approved.v1` | Terminal Gateway, Settlement, Notification | Settlement y Notification lo descartan. **Terminal Gateway falla con una transición inválida y reintenta 3 veces** |
| `posnet.auth.rejected.v1` | Terminal Gateway, Notification | Igual que el anterior |
| `posnet.auth.reversal-completed.v1` | Settlement; Terminal Gateway lo ignora | Se descarta por idempotencia |
| `posnet.transaction.batch-close.v1` | Settlement | Si el lote ya cerró, no hace nada |
| `posnet.settlement.batch-closed.v1` | Notification | Se descarta por idempotencia |

Los errores de Terminal Gateway son esperables y no causan daño, pero aumentan
`posnet_nats_messages_failed_total`, que forma parte del SLO de éxito de transacción.

El mensaje republicado no conserva el contexto de traza original.

### Confirmar el resultado

Buscá en los logs del servicio el mensaje de éxito, y verificá el efecto real: el estado de la
transacción, el lote o la notificación. **Una fila en `processed_events` no alcanza como confirmación.**

```bash
rm -f "$OUT.subject" "$OUT.json"
```

---

## 5. Si no se puede corregir la causa

Si el evento nunca podrá procesarse tal cual, porque referencia una entidad que ya no existe o porque el
dominio no lo acepta, no insistas. Documentá el `event_id` y el motivo, y coordiná con el equipo una
corrección de datos.

Un caso típico es una aprobación tardía para una sesión que el job de limpieza de Terminal Gateway ya
borró: `gateway-auth-consumer` no puede aplicarla y agota sus entregas. Settlement y Notification sí
pueden procesarla.

---

## 6. Si el evento no está en el stream

Las publicaciones directas no usan outbox: si fallaron, el servicio registró `failed to publish`, siguió
adelante, y el evento no existe. Esto aplica a todos los eventos, salvo `transaction.received` y los de
Settlement (`settlement.batch-closed` y `settlement.completed`), que esperan en el outbox hasta publicarse.

| Evento perdido | Cómo se ve | Recuperación |
|---|---|---|
| `fraud.check-requested` | Transacción en `FRAUD_CHECKING`, sin caso de fraude | Borrar la fila en `pn_authorization` y republicar el `transaction.received` original |
| `fraud.score-calculated` | Transacción en `FRAUD_CHECKING`, con caso de fraude | Borrar la fila en `fraud_detection` y republicar el `check-requested` original |
| `auth.approved`, `auth.rejected` | Transacción en estado final, sin efecto en los demás servicios | **No hay procedimiento seguro.** Reconstruir el evento a mano es delicado: coordinalo con el equipo |
| `settlement.batch-closed` anterior al outbox | Lote cerrado, sin notificación | Avisar al comercio por otro medio |

Al republicar `transaction.received`, el `Save` de Authorization reescribe la transacción como
`FRAUD_CHECKING`, y su estado anterior se pierde. **Hacelo solo si sigue en `FRAUD_CHECKING`.**

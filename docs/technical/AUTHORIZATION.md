# Authorization — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/authorization/README.md) o de los
> comentarios del código, vale lo que está acá.

## 1. Resumen

Authorization orquesta la Saga de autorización de cada pago: recibe la transacción desde
[Terminal Gateway](TERMINAL-GATEWAY.md), consulta la blocklist, pide la evaluación de fraude, llama al
adquirente y publica el resultado al resto del sistema.

La Saga funciona de punta a punta y **puede hablar [ISO 8583](#iso-8583) por TCP con un host, hoy un simulador**. Los datos
de la tarjeta los resuelve un stub con tarjetas de prueba, hasta que exista `card-management-service`. El rechazo por
fraude sigue sin alcanzarse en el flujo QR con los pesos por defecto (ver
[Fraud Detection](FRAUD-DETECTION.md#45-alcance-real-de-reject)).

| Capacidad | Estado | Detalle |
|---|---|---|
| Inicio de la Saga y coordinación con Fraud Detection | Implementado | Por eventos NATS |
| Máquina de estados de `Transaction` | Implementado | Ver [§4.2](#42-máquina-de-estados) |
| Publicación de resultados | Implementado | Por outbox, al menos una vez |
| Reversas | Implementado | Contra el mock |
| Consulta de estado | Implementado | HTTP y gRPC, sin autenticación |
| Métricas, trazas, alertas y SLO | Implementado | Ver [§10](#10-observabilidad) |
| Adquirente por ISO 8583 sobre TCP | Implementado | `0100`, `0400` y `0800`, contra un [simulador](#iso-8583). Sin TLS |
| Circuit breaker del adquirente | Implementado | Con el host sin responder, rechaza con `91` al instante. Ver [Circuit breaker](#circuit-breaker) |
| Datos de la tarjeta | **Stub** | Tarjetas de prueba. No se puede usar en producción |
| Blocklist de tarjetas | Implementado | Con `card_token` en `/pay` y un `41` del host |
| Timeout de fraude | Implementado | Sin score a tiempo, rechaza con `FRAUD_TIMEOUT`. Ver [§5.2](#52-decisión-de-fraude) |
| Conciliación de `INDETERMINATE` | **Parcial** | Las revierte y rechaza. No consulta el resultado al host |
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
| `emvDataBase64`, `iso8583Raw` | Datos opacos del terminal; hoy llegan vacíos. El mensaje al host no se guarda acá: lleva el PAN |
| `receivedAt`, `authorizedAt`, `rejectedAt` | Marcas de tiempo |

Redes aceptadas: `VISA`, `MASTERCARD`, `AMEX`, `CABAL`, `NARANJA` y `UNKNOWN`.

### 4.2 Máquina de estados

```
RECEIVED ──────► FRAUD_CHECKING ──────► PROCESSING ──────► APPROVED ──► REVERSED
    │                  │                    │  │
    │ blocklist        │ fraude: REJECT     │  └─ rechazo ─► REJECTED
    │                  │ o sin score        │
    ▼                  ▼                    │
 REJECTED           REJECTED                └─ error ────► INDETERMINATE ──► REVERSED | APPROVED | REJECTED
```

`APPROVED` admite una sola transición, a `REVERSED`. `REJECTED` y `REVERSED` son finales.
`INDETERMINATE` tampoco es final: el [conciliador](#56-conciliación) la revierte en el adquirente y la pasa a `REJECTED`.

**No todos los estados llegan a la base de datos:**

| Estado | ¿Se persiste? | Cuándo |
|---|---|---|
| `RECEIVED` | No | La transacción se guarda recién después de pasar a `FRAUD_CHECKING` |
| `FRAUD_CHECKING` | Sí | Primer estado visible en una consulta |
| `PROCESSING` | Sí | Antes de llamar al adquirente, junto con el evento de score. `processing_started_at` indica desde cuándo |
| `APPROVED`, `REJECTED`, `REVERSED` | Sí | — |
| `INDETERMINATE` | Sí | Si no se puede guardar, sigue en `PROCESSING` y el conciliador la toma igual |

Una transacción en `FRAUD_CHECKING` está esperando el score: si sigue así más de `FRAUD_CHECK_TIMEOUT`, se rechaza. Una en
`PROCESSING`, esperando al adquirente: si sigue
así más de `ACQUIRER_STUCK_AFTER`, quedó trabada.

Las transiciones de la Saga se guardan con `SaveIfState`, **solo si la fila sigue en el estado esperado**. De dos
procesos que intentan la misma transición, gana uno: dos scores de una venta no la autorizan dos veces, y una
respuesta tardía del adquirente no pisa lo que ya resolvió el conciliador.

### 4.3 Rechazos

| Fuente | Código | Cuándo | Ordena retener la tarjeta |
|---|---|---|---|
| `FRAUD` | `FRAUD_REJECTED` | El motor devolvió `REJECT` | No |
| `FRAUD` | `FRAUD_TIMEOUT` | Fraud Detection no respondió dentro de `FRAUD_CHECK_TIMEOUT` | No |
| `BLOCKLIST` | `CARD_BLOCKED` | La tarjeta está en la blocklist | Sí |
| `ACQUIRER` | ISO 8583 (`05`, `51`, etc.) | El emisor rechazó | Solo `04`, `41` y `43` |
| `VALIDATION` | `UNKNOWN_RESPONSE` | El adquirente respondió sin código | No |
| `VALIDATION` | `INVALID_DATA` | `transaction.received` trae datos inválidos. Se registra aparte: ver [§5.5](#55-qué-pasa-cuando-falla-cada-paso) | No |
| `TIMEOUT` | `TIMEOUT` | El adquirente no respondió, y el [conciliador](#56-conciliación) la revirtió | No |

Se publican con `is_retryable=true` los códigos `91` y `96`, y `TIMEOUT` y `FRAUD_TIMEOUT`: en estos dos, reintentar no
cobra dos veces, porque la venta se revirtió o nunca llegó al adquirente. La retención se decide por la
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
│                      │ TX: marca evento procesado     │                  │
│                      │ guarda en PROCESSING           │                  │
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
la transacción sigue esperando, hasta el timeout.

**Sin score a tiempo, se rechaza.** Cada segundo, o cada quinto de `FRAUD_CHECK_TIMEOUT` si es menor, Authorization busca
las transacciones en `FRAUD_CHECKING` recibidas hace más de ese timeout, 5 s por defecto, y las pasa a `REJECTED` con
`FRAUD_TIMEOUT`, junto con su `auth.rejected` en el outbox. El cliente espera, como mucho, el timeout más un segundo.

- **No se sigue sin el score:** aprobar sin evaluar el riesgo dejaría pasar justo lo que el motor frena. Con Fraud Detection
  caído, se rechazan todas las ventas, y avisa la alerta `FraudCheckSinRespuesta`.
- **No llama al adquirente:** no hay nada que revertir, y el rechazo es reintentable.
- **Un score que llega después se ignora,** como cualquier otro que la transacción ya no espera. Si llega a la vez que el
  rechazo, `SaveIfState` deja que gane uno solo.
- **Con varias instancias,** cada una corre la búsqueda por su cuenta, y `SaveIfState` evita que dos rechacen la misma.

El timeout se mide desde la recepción, así que incluye los dos saltos por NATS y el relay del outbox, unos cientos de
milisegundos. Fraud Detection tiene su propio límite por evaluación, `ENGINE_EVAL_TIMEOUT`, que debe ser bastante menor.

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

Al recibir `reversal-requested`, en una sola transacción de base marca el evento, bloquea la fila, **valida el estado** y
guarda el pedido en `reversal_requested_at`. Recién entonces, si corresponde, llama al adquirente:

| Estado | Resultado |
|---|---|
| `APPROVED` | Llama a `acquirer.Reverse`, pasa a `REVERSED` y publica `reversal-completed` |
| `FRAUD_CHECKING`, `PROCESSING` | La venta sigue en curso, y no se llama al adquirente. Si termina aprobada, el conciliador la revierte. Si termina rechazada, no hay nada más que hacer |
| `INDETERMINATE` | No hace nada: el conciliador ya la revierte |
| `REJECTED`, `REVERSED` | No hay nada que revertir. Queda un `WARN` y `posnet_reversals_ignored_total` |

**Un pedido de reversa no se pierde.** Si el adquirente falla o el proceso cae, la transacción sigue en `APPROVED` con el
pedido guardado, y el [conciliador](#56-conciliación) la reintenta hasta completarla. Mientras tanto, `GET
/transactions/{id}` informa `ReversalRequestedAt`. Si no se puede guardar el pedido, nada queda marcado y el evento se
reintenta.

La transacción sigue vigente hasta pasar a `REVERSED`: recién entonces se publica `reversal-completed`, y Settlement la
descuenta. **Terminal Gateway no se entera:** ya respondió `202` e ignora este evento.

### 5.5 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| `transaction.received` con datos inválidos | Lo registra en `invalid_transactions`, encola `auth.rejected` con `INVALID_DATA` en la misma transacción y `Term` | **Sí**: la sesión pasa a rechazada |
| Ídem, sin un `transaction_id` válido o con uno que ya existe | `Term`; no se publica nada | No hace falta: no hay sesión a la que avisar, o vale la transacción original |
| Ídem, y falla la consulta, el registro o el encolado del rechazo | No queda ninguno de los dos: `Nak` y reintento. Al agotar las entregas, se pierde | **Sí**, si se recupera antes |
| Error de base de datos antes de marcar el evento | `Nak` y reintento | **Sí** |
| Falla el alta, el encolado de `fraud.check-requested` o el commit | Nada queda guardado, ni el evento marcado: `Nak` y reintento | **Sí** |
| Otro `transaction.received` con un `transaction_id` que ya existe | Se ignora con un `WARN` y se marca el evento: la transacción original no cambia | No hace falta |
| Fraud Detection nunca responde | Pasado `FRAUD_CHECK_TIMEOUT`, se rechaza con `FRAUD_TIMEOUT` | **Sí**: la sesión pasa a rechazada |
| Falla el rechazo por timeout de fraude | Sigue en `FRAUD_CHECKING`; la corrida siguiente lo reintenta | **Sí** |
| Error de base de datos al aplicar el score, incluido el encolado de un rechazo por fraude | Nada queda guardado: `Nak` y reintento, sin haber llamado al adquirente | **Sí** |
| Llega otro score de una transacción que ya no lo espera | Se ignora con un `WARN` y se confirma | No hace falta |
| Caída durante la llamada al adquirente | Queda en `PROCESSING`; la toma el conciliador | **Sí**: la revierte y la rechaza |
| Error del adquirente | Pasa a `INDETERMINATE`; la toma el conciliador | **Sí**: la revierte y la rechaza |
| El host deja de responder | Con varias llamadas seguidas sin respuesta, el [circuit breaker](#circuit-breaker) se abre y rechaza con `91` al instante | **Sí**: se cierra cuando el host vuelve a responder |
| Falla el guardado o el encolado del resultado tras responder el adquirente | Queda en `PROCESSING`; la toma el conciliador | **Sí**: la revierte, aunque el emisor la haya aprobado |
| Falla el guardado o el encolado tras completar una reversa | Sigue en `APPROVED`, con la reversa pedida; el conciliador la vuelve a pedir | **Sí** |
| La reversa del conciliador falla | Sigue en `INDETERMINATE`; alerta `TransaccionesSinResolver` | **Sí**, cuando el adquirente responda |
| NATS no disponible al publicar | Los eventos esperan en el outbox; alerta `OutboxAuthorizationTrabado` | **Sí**, al volver NATS |

El rechazo por `INVALID_DATA` solo lleva los campos que pasaron la validación; los demás van vacíos o en cero. Un
`card_last4` o `card_network` inválido tampoco se loguea ni se guarda, porque podría traer un PAN completo.

No entran en `transactions`, porque no cumplen sus restricciones y Fraud Detection lee esa tabla. `GET /transactions/{id}` y
`GetTransactionStatus` las informan como `REJECTED`. Un `404` significa que el ID nunca llegó, que no se pudo registrar o
que su registro [ya venció](#retención-de-transacciones-inválidas).
El motivo exacto solo está en `validation_error`, porque esas APIs no tienen autenticación.

Notification descarta el evento si el comercio no es válido o el monto no es positivo, con un `ERROR` y
`posnet_nats_messages_failed_total`. Terminal Gateway no depende de eso: solo necesita el `transaction_id`. En el flujo de
`/pay` no ocurre, porque esos campos salen de la sesión.

Terminal Gateway ya valida los datos de tarjeta en `/pay`, así que este rechazo es una segunda barrera: se alcanza con un
evento de otro productor, o si las reglas de los dos contextos dejan de coincidir. Para probarlo, publicar el evento directamente en
`posnet.transaction.received.v1`.

En los casos sin resultado publicado, Terminal Gateway mantiene la sesión en `PROCESSING` hasta
que su job de limpieza la borra.

### Llamadas al adquirente

No ocupan el handler de NATS. La transacción ya está guardada, así que el mensaje se confirma enseguida, y la llamada corre
en un worker, con hasta `ACQUIRER_MAX_CONCURRENT` simultáneas y un timeout de `ACQUIRER_TIMEOUT_SECONDS` cada una, que no
depende del `AckWait` de NATS. Un adquirente lento no frena la cola hasta que se ocupan todos los workers. Ahí, el consumo
espera, en lugar de acumular llamadas sin límite.

### Circuit breaker

Si el host está conectado pero no responde, cada llamada espera su timeout entero, ocupa un worker y termina en
`INDETERMINATE`, con una reversa. Para cortarlo, las llamadas pasan por un circuit breaker:

| Estado | Qué hace | Cuándo cambia |
|---|---|---|
| Cerrado | Las llamadas pasan | Con `ACQUIRER_BREAKER_FAILURES` llamadas seguidas sin respuesta, 5 por defecto, se abre |
| Abierto | Rechaza las ventas con `91` al instante, sin llamar al host. Las reversas fallan y el conciliador las reintenta | Pasado `ACQUIRER_BREAKER_COOLDOWN`, 30 s por defecto, pasa a medio abierto |
| Medio abierto | Deja pasar una sola llamada de prueba, y rechaza las demás mientras tanto | Si el host responde, se cierra; si no, se abre otro cooldown |

- **Qué cuenta como falla:** solo un error ambiguo, el de una llamada que salió y no recibió respuesta. Una respuesta del
  emisor, aprobada o rechazada, reinicia la cuenta. Un envío imposible, como sin conexión o con una tarjeta desconocida, ya
  se rechaza al instante y no cuenta.
- **El `91` es reintentable,** y no hay nada que revertir: la venta no salió. Lo mismo que [sin conexión](#iso-8583).
- **Con varias instancias,** cada una tiene su breaker y lo abre por su cuenta.
- **Lo informan** `posnet_acquirer_circuit_open` y la alerta `CircuitoAdquirenteAbierto`. Los logs son `acquirer circuit
  opened`, `half-open` y `closed`.
- **`ACQUIRER_BREAKER_FAILURES=0` lo desactiva.**

### ISO 8583

Con `ACQUIRER_MODE=iso8583`, activado en Docker Compose, `iso8583.Client` implementa `AcquirerGateway`, con ISO 8583:1987 en
ASCII y un largo de 2 bytes delante de cada mensaje. Usa una sola conexión TCP, multiplexada: las respuestas se
correlacionan por tipo de mensaje y RRN (DE-37). Reconecta sola, con una espera creciente de hasta 30 s, y prueba la
conexión con un `0800` cada 30 s.

| Mensaje | Uso | Campos |
|---|---|---|
| `0100`/`0110` | Autorización | DE-2, 3, 4, 7, 11, 12, 13, 14, 22, 25, 32, 37, 41, 42, 49 y 55 si hay datos EMV. Responde DE-38 y 39 |
| `0400`/`0410` | Reversa | Los mismos, más DE-38 si se aprobó y DE-90 con los datos originales. DE-39 `00` o `25` (el host no la conoce) son un éxito |
| `0800`/`0810` | Eco | DE-7, 11 y 70 (`301`) |

El RRN y DE-7 se derivan de la transacción, así que la reversa los repite sin guardar nada. DE-41 y DE-42 son un hash del
terminal y del comercio, porque sus UUID no entran en esos campos. **Un host real va a pedir su propio formato:** el dialecto
se ajusta en `messages.go`.

**El PAN solo existe en memoria,** dentro del adaptador y mientras arma un mensaje. No se guarda, no viaja por eventos, y
`CardData` lo oculta en los logs. `CardDataProvider` lo resuelve a partir de `card_token`, el mismo token de la blocklist,
que llega en `POST /api/sessions/{id}/pay`. Hoy lo implementa un stub, que usa una tarjeta de prueba fija si el pago llega
sin token. **Para conectar `card-management-service`, alcanza con otra implementación de ese puerto.** Tendrá que rechazar
los pagos sin token, y conviene comparar los últimos 4 dígitos con los de la transacción.

**Qué pasa según el error:**

| Situación | ¿Salió el mensaje? | Resultado |
|---|---|---|
| Sin conexión o sin escribir nada | No | `REJECTED` con `91`, reintentable. Sin reversa |
| Tarjeta desconocida para el proveedor | No | `REJECTED` con `14` |
| Otro error del proveedor | No | `REJECTED` con `91`. Sin reversa |
| Vence el timeout, o se corta esperando la respuesta | No se sabe | `INDETERMINATE`, y el conciliador revierte |
| Respuesta después del timeout | Sí | Se ignora con un `WARN`, y vale lo que resolvió el conciliador |

### 5.6 Conciliación

Sin una respuesta guardada, no se puede saber si el emisor aprobó la operación. Cada `ACQUIRER_RECONCILE_EVERY` (30 s) y
al arrancar, `ReconcileUnresolved` toma hasta 50 transacciones, de la más antigua a la más nueva: las
`INDETERMINATE` y las que siguen en `PROCESSING` después de `ACQUIRER_STUCK_AFTER` (2 min).

1. Si está en `PROCESSING`, la pasa a `INDETERMINATE`. Desde ahí, una respuesta tardía ya no se puede guardar.
2. **Envía la reversa al adquirente,** la práctica de ISO 8583 ante un timeout. Si falla, reintenta en la corrida
   siguiente, sin frenar a las demás.
3. La pasa a `REJECTED`, con código y origen `TIMEOUT`, y publica `auth.rejected`, con `is_retryable` en `true`: el
   cliente recibe una respuesta y puede volver a intentar sin riesgo de un cobro doble.

**No publica `reversal-completed`:** la venta nunca se informó como aprobada, así que Settlement no tiene nada que
descontar, y abriría una revisión `PURCHASE_NOT_FOUND`.

**También reintenta los pedidos de reversa** de transacciones en `APPROVED`, una vez pasado `ACQUIRER_TIMEOUT_SECONDS`:
antes, la primera reversa puede seguir en curso. Estas sí publican `reversal-completed`, porque Settlement ya registró la
venta.

Varias instancias pueden tomar la misma transacción. La reversa repetida no debe tener efecto, y solo una instancia
informa el resultado. **El adaptador real del adquirente debe responder éxito** si el host no conoce la operación o si
ya estaba revertida: una caída antes de enviar la autorización también termina en reversa.

## 6. API HTTP

Puerto `HTTP_PORT` (8080 por defecto). **No hay autenticación.**

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; verifica Postgres, las tablas y columnas requeridas y la conexión con NATS. `reason`: `database unavailable`, `database schema incomplete` o `nats unavailable`, y el log nombra lo que falta | `200` | `503` |
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
  "RejectedAt": "",
  "ReversalRequestedAt": ""
}
```

`ReversalRequestedAt` indica un pedido de reversa sin completar.

Los errores tienen la forma `{"error": "CODIGO", "message": "..."}`.

## 7. API gRPC

Contrato: [`authorization.proto`](../../pkg/proto/authorization/v1/authorization.proto). El puerto es `GRPC_PORT`
(9090), sin TLS ni autenticación. Hoy ningún otro servicio del repositorio lo consume. Si el puerto está ocupado, el
servicio no arranca. Al apagar, espera hasta 10 s las llamadas en curso.

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

Todos se publican por outbox: se guardan en `pn_authorization.outbox`, en la misma transacción que el cambio de estado
que los origina, así que no hay un resultado guardado sin su evento ni un evento sin su resultado. Un Relay revisa la
tabla cada 200 ms, publica hasta 100 eventos por ciclo y borra los enviados. Eso suma hasta 200 ms a cada resultado.
Puede correr en varias instancias: cada fila la toma una sola, con `FOR UPDATE SKIP LOCKED`.

**La entrega es al menos una vez.** Si el Relay publica y no llega a borrar la fila, reenvía el evento con el mismo
`Nats-Msg-Id`, el `event_id`: JetStream lo descarta dentro de su ventana de deduplicación, de 2 minutos por defecto. Pasada
esa ventana, llega duplicado, y el consumidor debe descartarlo por `event_id`.

**El orden no está garantizado con varias instancias.** Cada Relay publica sus filas por `created_at`, pero dos instancias
pueden tomar eventos de la misma transacción en lotes distintos y publicarlos en otro orden. Solo afecta a una reversa
completada muy poco después de su aprobación: Settlement la deja [en revisión](SETTLEMENT.md#revisiones-de-reversas) hasta que llegue la compra.

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
| `transactions` | Transacciones | PK `id`; índice parcial en `card_token`; índices por terminal y fecha, y de aprobadas por comercio y fecha, para el historial de Fraud Detection |
| `processed_events` | Idempotencia de eventos | PK `event_id` |
| `outbox` | Eventos pendientes de publicar | PK `id`; índice `created_at` |
| `blocked_cards` | Blocklist | PK `card_token`; sin FK, para sobrevivir a purgas de transacciones |
| `invalid_transactions` | Rechazos por datos inválidos. Solo valores validados, nunca el payload original | PK `transaction_id` |

### Migraciones

Las migraciones se aplican a mano. **Al arrancar, el servicio verifica las tablas y columnas que necesita y, si falta alguna,
no arranca:** termina con un error que nombra lo que falta. No llega a consumir eventos, así que esperan en NATS hasta que
se aplique la migración, en lugar de fallar hasta agotar sus reintentos y perderse. En Docker Compose, el contenedor se
reinicia hasta que la base esté completa.

### Aplicar la migración de conciliación

**Una base existente necesita `000002` y `000003` antes de desplegar esta versión del servicio.** Sin ellas, el servicio no
arranca. Se pueden aplicar con el servicio anterior en marcha:

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000002_processing_started_at.up.sql
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000003_reversal_requested_at.up.sql
```

Son idempotentes. **Al arrancar, el conciliador revierte en el adquirente las `INDETERMINATE` que ya existan:** si hay un
adquirente real conectado, revisalas antes. `000002_processing_started_at.down.sql` la revierte: falla sin cambiar nada si
hay transacciones en `PROCESSING` o `INDETERMINATE`, y exige volver antes a la versión anterior del servicio.
`000003_reversal_requested_at.down.sql` exige lo mismo, y falla si hay reversas pendientes.

### Aplicar la migración de transacciones inválidas

**Una base existente necesita `000004` antes de desplegar esta versión.** Sin ella, el servicio no arranca. Se puede aplicar
con el servicio anterior en marcha:

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000004_invalid_transactions.up.sql
```

Es idempotente. `000004_invalid_transactions.down.sql` borra ese registro, que no se puede reconstruir, y exige volver antes
a la versión anterior del servicio.

### Retención de transacciones inválidas

`invalid_transactions` sirve para soporte e incidentes de integración, no para analítica. Al arrancar y cada hora, el
servicio borra lo que supera `INVALID_TRANSACTIONS_RETENTION`, 90 días por defecto. Si la purga falla, queda un `ERROR` en el
log y reintenta en la corrida siguiente. Con varias instancias, cada una la corre por su cuenta, sin efectos entre ellas.

Después de ese plazo, `GET /transactions/{id}` vuelve a responder `404`. Para conservar los datos más tiempo, hay que
exportarlos antes o aumentar la variable.

### Aplicar los índices de historial

**Una base existente necesita `000005`** para que las consultas de historial de Fraud Detection no recorran la tabla entera.
Sin ella, el servicio arranca igual, porque no es una columna requerida, pero cada evaluación de fraude es más lenta: con
200.000 transacciones, la consulta de ventas recientes de un terminal pasa de 0,2 ms a 58 ms.

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000005_history_indexes.up.sql
```

Crea los índices con `CONCURRENTLY`, así que se aplica con el servicio en marcha, sin bloquear las escrituras, y es
idempotente. Si se interrumpe a mitad, un índice puede quedar inválido y la siguiente ejecución lo saltearía: hay que
borrarlo con `DROP INDEX CONCURRENTLY` y volver a aplicar el archivo. `000005_history_indexes.down.sql` los borra.

### Aplicar la migración de blocklist

**Una base creada antes de este cambio necesita la migración manualmente.** Sin la columna
`card_token`, el servicio no arranca:

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000001_card_blocklist.up.sql
```

El script es idempotente. Existe `000001_card_blocklist.down.sql` para deshacerlo, pero **destruye
datos que no se pueden reconstruir**: la blocklist y los `card_token` de las transacciones. En un
entorno con datos reales, exportarlos antes de ejecutarlo.

### Aplicar la migración del outbox

**Una base existente necesita `000006` antes de desplegar esta versión.** Sin ella, el servicio no arranca. Se puede aplicar
con el servicio anterior en marcha:

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000006_outbox.up.sql
```

Es idempotente. `000006_outbox.down.sql` la revierte: exige volver antes a la versión anterior del servicio, y falla sin
cambiar nada si quedan eventos sin publicar, porque borrarlos perdería resultados de ventas.

### Aplicar el índice del timeout de fraude

**Una base existente necesita `000007`** para que la búsqueda de transacciones sin score, que corre cada segundo, no recorra
la tabla entera. Sin ella, el servicio arranca igual, pero cada búsqueda es más lenta.

```bash
psql "$POSTGRES_DSN" -f migrations/pn_authorization/000007_fraud_checking_index.up.sql
```

Crea el índice con `CONCURRENTLY`, como [`000005`](#aplicar-los-índices-de-historial): se aplica con el servicio en marcha y
es idempotente, y si se interrumpe hay que borrarlo y volver a aplicarlo. `000007_fraud_checking_index.down.sql` lo borra.

### Garantías actuales

- **Publicación:** *at-least-once*, por [outbox](#publicados). Cada evento se confirma junto con su cambio de estado, y
  si NATS falla, espera en la tabla hasta que se pueda publicar. Un duplicado se descarta por `Nats-Msg-Id` o, pasada la
  ventana de JetStream, por `event_id` en el consumidor.
- **Idempotencia:** el `event_id` se registra en `processed_events`. En `AuthorizeTransaction`, ese registro se confirma
  junto con el alta, que no modifica una transacción existente. En `ApplyFraudScore` y `ProcessReversal`, se confirma
  junto con el nuevo estado o el pedido, y **antes** de llamar al adquirente: un fallo previo se
  reintenta, y una reentrega no vuelve a llamarlo. Lo que falle después lo resuelve el
  [conciliador](#56-conciliación).

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene el conciliador, la purga y HTTP (hasta 10 s), drena y cierra NATS, espera las llamadas al
adquirente en curso, hasta su timeout más 5 s, detiene el Relay y cierra el pool de Postgres. Lo que no termine a tiempo lo
retoma el conciliador, y los eventos que queden en el outbox salen al arrancar otra instancia. El servidor gRPC espera hasta 10 s las llamadas en curso.

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
| `posnet_authorization_unresolved_transactions` | Gauge | `state`: `INDETERMINATE`, `PROCESSING` trabadas o `REVERSAL_PENDING`. Consulta la base en cada scrape |
| `posnet_authorization_outbox_stuck` | Gauge | Eventos en el outbox desde hace más de 1 minuto. Se calcula en la misma consulta |
| `posnet_acquirer_connected` | Gauge | Con `iso8583`: 1 conectado, 0 caído |
| `posnet_acquirer_circuit_open` | Gauge | 1 con el [circuit breaker](#circuit-breaker) abierto o probando, 0 cerrado |
| `posnet_reversals_ignored_total` | Counter | `state`: pedidos de reversa sin nada que revertir |

`posnet_saga_duration_seconds` solo registra sagas que terminan en aprobación o rechazo: las trabadas no aparecen, ni
las rechazadas por `INVALID_DATA`, que no llegan a empezar.
Una reversa del conciliador cuenta como rechazo, con `source="TIMEOUT"`.

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
| `AdquirenteDesconectado` | Warning | Sin conexión con el host durante 2 min: las ventas se rechazan con `91` |
| `CircuitoAdquirenteAbierto` | Warning | El circuit breaker sigue abierto 1 min: el host no responde y las ventas se rechazan con `91` |
| `FraudCheckSinRespuesta` | Critical | Ventas rechazadas con `FRAUD_TIMEOUT` en los últimos 5 min, durante 1 min: Fraud Detection no responde a tiempo. `increase` no cuenta el primer rechazo desde que arrancó el servicio, así que un rechazo aislado no la dispara |
| `TransaccionesSinResolver` | Warning | Transacciones sin resultado conocido o reversas sin completar durante 5 min: las reversas están fallando |
| `OutboxAuthorizationTrabado` | Warning | Eventos en el outbox hace más de 1 minuto, durante 5 min: no llegan a NATS. Se publican solos al resolver la causa |
| `LatenciaSagaAlta` | Warning | p95 de la Saga por encima de 3 s durante 5 min |
| `DuplicadosNATSAltos` | Warning | Duplicados por encima de 0,05/s durante 5 min |

`ErrorRateAlta` incluye rechazos de negocio válidos, como fondos insuficientes.

**Dashboards de Grafana:** `posnet-saga-overview`, `posnet-bc-detail` y `posnet-slo`.
**Runbook:** [incident-response](../runbooks/incident-response.md).

### Trazas y logs

Cada comando abre un span (`command.AuthorizeTransaction`, `command.ApplyFraudScore`,
`command.callAcquirer`, etc.). Para seguir un pago en los logs, buscar por `transaction_id`. Los logs
más relevantes son `acquirer error` y `outbox relay`, que informa los ciclos del Relay que no pudieron publicar.

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
| `GRPC_PORT` | `9090` | Sí. Si está ocupado, el servicio no arranca |
| `FRAUD_CHECK_TIMEOUT` | `5s` | Sí; cuánto se espera el score de Fraud Detection antes de rechazar con `FRAUD_TIMEOUT`. Con un valor inválido o menor a `1s`, el servicio no arranca |
| `INVALID_TRANSACTIONS_RETENTION` | `2160h` (90 días) | Sí; cuánto se conservan las [transacciones inválidas](#retención-de-transacciones-inválidas), y `0` desactiva la purga. Va en horas: con un valor como `30d`, o negativo, el servicio no arranca |
| `ACQUIRER_RECONCILE_EVERY` | `30s` | Sí; cada cuánto corre el conciliador, y `0` lo desactiva. Docker Compose usa `2s`. Si es negativo, el servicio no arranca |
| `ACQUIRER_STUCK_AFTER` | `2m` | Sí; Docker Compose usa `10s`. Cuánto puede seguir una transacción en `PROCESSING`. Si no supera `ACQUIRER_TIMEOUT_SECONDS`, el servicio no arranca: revertiría llamadas en curso |
| `ACQUIRER_MODE` | `mock` | Sí; `mock` o `iso8583`. Docker Compose usa `iso8583`. Con otro valor, o con `iso8583` y `ENVIRONMENT=production`, el servicio no arranca |
| `ACQUIRER_HOST`, `ACQUIRER_PORT` | `localhost`, `9100` | Con `iso8583`. Docker Compose apunta a `iso-host:9101` |
| `ACQUIRER_INSTITUTION_ID` | `000001` | Con `iso8583`; es el DE-32 |
| `ACQUIRER_TIMEOUT_SECONDS` | `30` | Sí; timeout de cada llamada al adquirente. Docker Compose usa `3` |
| `ACQUIRER_MAX_CONCURRENT` | `20` | Sí; llamadas simultáneas al adquirente. Si es menor que 1, el servicio no arranca |
| `ACQUIRER_BREAKER_FAILURES` | `5` | Sí; llamadas seguidas sin respuesta que abren el [circuit breaker](#circuit-breaker). `0` lo desactiva; negativo, el servicio no arranca |
| `ACQUIRER_BREAKER_COOLDOWN` | `30s` | Sí; cuánto rechaza el breaker abierto antes de probar con una llamada. Con el breaker activo, debe ser positivo |
| `ACQUIRER_MOCK_SCENARIOS` | `false` | Solo con `ACQUIRER_MODE=mock`; el mock responde [según el monto](#probar-los-fallos-del-adquirente). Con `ENVIRONMENT=production`, el servicio no arranca |
| `ACQUIRER_TLS_CERT`, `ACQUIRER_TLS_KEY`, `ACQUIRER_TLS_CA` | Vacías | **No**: el cliente no usa TLS |
| `MIGRATIONS_DIR` | `migrations/authorization` | **No**: `Migrate` no hace nada, y esa carpeta no existe |

La variable del README `ACQUIRER_TIMEOUT` no existe. El servicio `acquirer-mock` de Compose no se usa: el host de desarrollo
es `iso-host`, y `MockAcquirerGateway` corre dentro del proceso.

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

### Probar los fallos del adquirente

El simulador `iso-host` responde según el monto. Con `ACQUIRER_MODE=mock` y `ACQUIRER_MOCK_SCENARIOS=true`, el mock hace lo
mismo sin conexión. Con cualquier otro monto, aprueban. La colección de Postman los usa en la carpeta "Adquirente".

| `amount_cents` | El host | Resultado |
|---|---|---|
| `770051` | Responde `51` | `REJECTED`, de origen `ACQUIRER` |
| `770041` | Responde `41` | `REJECTED`, ordena retener la tarjeta y, si llegó `card_token`, la bloquea |
| `770091` | Cierra la conexión | `INDETERMINATE`, y el conciliador la rechaza con `TIMEOUT` |
| `770098` | No responde | Igual que el anterior, unos segundos después. Con `ACQUIRER_BREAKER_FAILURES` seguidas, abre el [circuit breaker](#circuit-breaker) |
| `770092` | Aprueba, y responde `96` a las dos primeras reversas | `APPROVED`, y `REVERSED` cuando el conciliador reintenta |

El simulador solo acepta tarjetas de prueba, y responde `14` a cualquier otra: una tarjeta real no pasa por acá. También
responde `54` a una vencida. Para que el host esté caído: `docker compose stop iso-host`.

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

Con el simulador, estas fallas solo ocurren con [los montos de prueba](#probar-los-fallos-del-adquirente).

| Brecha | Consecuencia | Dónde |
|---|---|---|
| El conciliador no consulta el resultado | Revierte toda operación sin respuesta, también las que el emisor aprobó: el cliente ve un rechazo y puede reintentar. El protocolo del host puede ofrecer algo mejor | `ReconcileUnresolved` |
| Respuesta tardía al cliente | Sin respuesta del adquirente, el cliente espera hasta `ACQUIRER_STUCK_AFTER` más una corrida. Si Terminal Gateway ya borró la sesión, el rechazo no encuentra a quién avisar | `ReconcileUnresolved`, `DeleteExpired` |
| Terminal Gateway no conoce el resultado de una reversa | Ya respondió `202` e ignora `reversal-completed`: el terminal no sabe si se completó ni si se ignoró. Se consulta con `GET /transactions/{id}`. Resolverlo requiere cambiar Terminal Gateway | `tg_nats_subscriber.go` |
| Una reversa que nunca se completa no tiene límite | Se reintenta cada corrida, sin un máximo ni una espera creciente, hasta que el host responda. Solo avisa la alerta | `ReconcileUnresolved` |
| Workers ocupados hasta que abre el breaker | Con el host colgado, las primeras `ACQUIRER_BREAKER_FAILURES` llamadas, y las que estén en curso, esperan su timeout y terminan en `INDETERMINATE` antes de que el [circuit breaker](#circuit-breaker) corte | `runAcquirerCall`, `breaker.Gateway` |

### Saga

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Notification no consume `reversal-completed` | El contrato del evento prevé un comprobante de anulación, pero el subscriber de Notification no se suscribe, así que no se genera | `nt_nats_subscriber.go` |

### Blocklist y datos de tarjeta

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Solo tarjetas propias | El PAN lo resolverá `card-management-service`, que solo conoce las tarjetas del emisor propio. Una tarjeta de otro banco se rechaza con `14`. Procesarlas requiere otra implementación de `CardDataProvider` | `CardDataProvider` |
| Sin TLS hacia el host | El mensaje lleva el PAN en claro por TCP. Solo se puede usar con tarjetas de prueba | `iso8583.Client` |
| Dialecto propio | Los campos siguen ISO 8583:1987, pero un host real va a pedir los suyos, además de certificación | `messages.go` |
| El stub no usa `card_token` para nada real | Devuelve una tarjeta de prueba: no valida que el token exista ni que coincidan los últimos 4 dígitos | `carddata.Stub` |
| Sin caso de uso para desbloquear | Un bloqueo es permanente, salvo que se borre la fila a mano | `BlockedCardRepository` |
| `iso8583_raw` se guarda tal como llega | El mensaje al host no se guarda ahí, pero si un terminal lo enviara con el PAN, quedaría en la base. Definir qué se guarda antes de recibir mensajes de terminales | `TransactionRepo.InsertTx` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Sin migraciones automáticas | Cada migración se aplica a mano. Si falta una columna requerida, el servicio no arranca hasta aplicarla (ver [§9](#migraciones)); un índice faltante solo lo hace más lento | `pkg/pgutil/migrate.go` |
| Acoplamiento con Fraud Detection | Lee las tablas de este contexto directamente, en contra de lo que indica la documentación de arquitectura | `fd_postgres_repository.go` |
| gRPC sin TLS ni autenticación | Cualquiera con acceso de red al puerto puede consultar transacciones. Hoy ningún servicio lo usa | `server.go` |
| API HTTP sin autenticación | Cualquiera con acceso de red y un ID puede leer el estado y el monto de una transacción | `router.go` |
| Eventos de dominio sin usar | `Transaction` registra eventos internos (`TransactionApproved`, etc.) que nadie lee: el outbox arma los eventos de integración desde la transacción | `transaction.go` |
| Orden de eventos con varias instancias | Los Relays de dos instancias pueden publicar eventos de una misma transacción en otro orden (ver [§8](#publicados)) | `outbox.Relay` |

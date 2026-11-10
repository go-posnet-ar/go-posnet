# Settlement — Documentación técnica

> Revisado el 2026-11-06. Este documento describe lo que el código hace
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

**Una base anterior a esta versión necesita las
[migraciones](#aplicar-la-migración) antes del despliegue:** sin ellas, el servicio falla al leer y guardar
lotes, o al cerrarlos y liquidarlos.

| Capacidad | Estado | Detalle |
|---|---|---|
| Apertura de lotes y registro de compras | Implementado | Un lote abierto por terminal, día de negocio y moneda |
| Registro de reversas | Implementado | Con su monto real, en el lote abierto del momento |
| Cierre y detección de discrepancias | Implementado | Por pedido del terminal |
| Cierre automático | Implementado | Lotes de días de negocio anteriores, sin conciliación |
| Resolución de disputas | Implementado | `reopen` y `resolve-dispute` |
| Liquidación y comisión (MDR) | Implementado | Confirmación manual con `settle` |
| Publicación de `batch-closed` y `settlement.completed` | Implementado | Por outbox, con reintentos. Nadie consume `settlement.completed` |
| Consulta de lotes | Implementado | HTTP, sin autenticación |
| Envío al procesador | **Simulado** | `none` no envía; `mock` acepta sin enviar nada |
| Cierre forzado por soporte | Implementado | Con los totales del backend, sin conciliación |
| Autenticación de escrituras | Implementado | Un token compartido; las lecturas siguen abiertas |
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
| `OPEN` → `DISPUTED` | Al cerrar con discrepancias: el lote se guarda directamente en `DISPUTED` |
| `CLOSED` → `DISPUTED` | En un `resubmit`, si el lote tiene discrepancias de una versión anterior |
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
3. Busca el lote `OPEN` del terminal para ese día y moneda o lo crea, en una transacción `SERIALIZABLE` aparte.
4. Agrega la transacción, guarda el lote y [compara la reversa](#52-reversa) si había una esperando esta compra,
   en la transacción del paso 2.

Si algo falla, nada de los pasos 2 y 4 se confirma, y el evento se reintenta.

### 5.2 Reversa

Al recibir `auth.reversal-completed`, se agrega una fila `REVERSAL` con el `transaction_id` de la compra y el
monto del evento. La fila va al **lote abierto del momento**, no necesariamente al de la compra:

| Situación | Resultado |
|---|---|
| La compra está en el lote abierto | Ese lote queda con el monto neto |
| La compra está en un lote ya cerrado | La reversa entra en un lote nuevo, que puede dar un **total negativo** |

**La reversa se registra siempre, con el monto del evento, y se compara con su compra.** Si no coinciden, queda
además en `reversal_reviews` y salta la alerta `ReversasEnRevision`. No se rechaza: una reversa `Term` se perdería.

| `reason` | Situación |
|---|---|
| `PURCHASE_NOT_FOUND` | La compra no está en Settlement |
| `MERCHANT_MISMATCH` | La compra es de otro comercio |
| `CURRENCY_MISMATCH` | La moneda es otra. La reversa entra en un lote de la moneda del evento |
| `AMOUNT_MISMATCH` | El monto es otro. Authorization siempre revierte el monto completo: no hay reversas parciales |

Si hay más de una diferencia, el motivo es el primero de esa lista, de arriba abajo.

**La reversa puede llegar antes que la compra,** porque van por consumidores distintos y una compra puede
demorarse en reintentos. Al llegar la compra, si coincide, la revisión se cierra sola (`resolved_by = 'system'`).
Si no coincide, cambia el motivo y sigue abierta.

Un bloqueo por `transaction_id` serializa la compra y la reversa de una misma transacción: si se procesan a la
vez, la segunda ve lo que confirmó la primera y no queda un falso `PURCHASE_NOT_FOUND`.

**Una revisión abierta frena el envío del lote que tiene la reversa.** El cierre del terminal y el automático lo
dejan en `DISPUTED`, aunque los totales coincidan. `resubmit` lo pasa a `DISPUTED` y `resolve-dispute` responde
`400`: primero hay que cerrar la revisión. La revisión se cierra con
[`POST /reversal-reviews/{transaction_id}/resolve`](#revisiones-de-reversas).

**Al cerrarse la última revisión de un lote `DISPUTED`, el lote se vuelve a conciliar,** con sus transacciones
actuales, haya cerrado la revisión el sistema o un operador. El cierre guarda contra qué se concilió:

| El lote se cerró | Se compara con | Si coincide | Si no |
|---|---|---|---|
| Por el terminal | Los totales que informó el terminal | Pasa a `CLOSED` y se envía | Sigue `DISPUTED`, con las discrepancias recalculadas |
| Por el job, `force-close` o `resolve-dispute` | Nada: no hay totales del terminal | Pasa a `CLOSED` y se envía | — |
| Antes de `000007` | No se sabe | No se libera: `resolve-dispute` o `reopen` | — |

La liberación queda en la auditoría como `AUTO_RELEASE`, con `operator_id` `system`. Corre después de cerrar la
revisión, en su propia transacción: si falla, la revisión ya está cerrada y el lote sigue `DISPUTED`, con un log
`could not release the held batch`. La alerta `LotesEnDisputa` lo muestra, y se resuelve con `resolve-dispute`.

**Una reversa excluida vuelve si llega su compra.** `EXCLUDE` sobre una revisión `PURCHASE_NOT_FOUND` saca la
reversa porque la compra no estaba. Si la compra llega después y coincide, la reversa se agrega al lote de la compra,
que queda con el monto neto, y la revisión queda resuelta por `system` con la acción `RESTORE`. Si no coincide, la
revisión se reabre con el motivo nuevo en el lote de la compra, que queda retenido hasta que un operador decida.

**Las reversas anteriores a esta corrección se recuperan con un comando,** que hay que ejecutar en cada entorno:
se descartaron, y su evento ya figura como procesado. Ver el
[Escenario E](../runbooks/batch-close-failure.md#escenario-e--reversas-anteriores-sin-descontar). Cuando no queda
ninguna, el comando lo registra en `settlement.maintenance_runs`; hasta entonces salta la alerta
`ReversasSinRecuperar`. Una base creada con `01-init.sql` ya viene registrada.

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
Calcula totales, discrepancias y revisiones abiertas
   │
   ▼
TX: marca el evento ─ guarda el lote (CLOSED o DISPUTED) ─ encola batch-closed en el outbox
   │
   ├─ discrepancies > 0 o revisión abierta ─► DISPUTED, fin
   │
   └─ sin discrepancias ni revisiones ─► envía al procesador
                              ├─ none ─► queda CLOSED, WARN y métrica
                              └─ mock ─► guarda SUBMITTED
```

Las tres escrituras son atómicas: si alguna falla, el lote sigue `OPEN` y el mensaje se reintenta. El
[Relay](#publicados) envía el evento a NATS después, por separado. Sin procesador, el mensaje se confirma: reintentarlo no cambiaría nada. El lote sigue en `CLOSED` hasta
un `resubmit`. Con cualquier otro error del procesador, el mensaje se reintenta, pero la reentrega ya no
encuentra un lote `OPEN` y no hace nada: la vía para reintentar también es `resubmit`.

**Si el terminal no reinicia sus contadores,** informa el acumulado del día. Si coincide exactamente con este lote
más los lotes ya cerrados de ese terminal, día y moneda, se concilia solo lo pendiente y no hay disputa. Un lote en
`DISPUTED` no cuenta para ese acumulado, porque sus totales no están conciliados. Cualquier otra diferencia da
`DISPUTED`.

### 5.5 Día de negocio

Los lotes se agrupan por día de negocio, en la zona del comercio. Sin una zona propia, usa
`SETTLEMENT_BUSINESS_TIMEZONE` (`America/Argentina/Buenos_Aires` por defecto). Una venta de las 22:30 en Argentina
pertenece a ese día, aunque en UTC ya sea el siguiente.

La zona propia se define con [`PUT /merchants/{merchant_id}/timezone`](#zona-horaria-de-un-comercio) y rige para las
operaciones siguientes. **Los lotes `OPEN` del comercio fechados hoy en la zona anterior pasan a hoy en la nueva,** en
la misma transacción, para que la venta siguiente no abra otro lote. Los de días anteriores conservan su fecha y los
cierra el job. Si un terminal ya tiene un lote `OPEN` en la fecha nueva, el cambio responde `409` y no cambia nada.

El cierre usa el `batch_date` que informa el terminal. Sin él, usa el día de negocio del pedido. Terminal
Gateway rechaza una moneda o una fecha inválidas.

El día que se puede cerrar, medido desde el día de negocio del pedido:

| `batch_date` | Resultado |
|---|---|
| Hoy o ayer | Se cierra. Ayer es el caso del terminal que cierra después de medianoche, antes que el job |
| Anterior a ayer, de un lote que un operador reabrió con `reopen` | Se cierra: es la [vuelta a cerrar](#57-disputas) de una disputa |
| Anterior a ayer, de un lote que nadie reabrió | Se rechaza (`not_reopened`): el terminal se equivocó de día, y sus totales darían una disputa falsa |
| Futuro (`future`) o más antiguo que `SETTLEMENT_CLOSE_MAX_AGE_DAYS` (`too_old`, 7 días por defecto) | Se rechaza |

Un rechazo descarta el evento sin reintentos, con un log `permanent error` y la métrica
`posnet_settlement_batch_close_rejected_total{reason}`. El terminal no recibe respuesta: Terminal Gateway ya
respondió `202`. Si no hay un lote `OPEN` para esa fecha, no se rechaza: se confirma sin hacer nada, como siempre.

Las fechas de los lotes creados antes del día de negocio siguen en UTC.

### 5.6 Cierre automático

El job revisa **cada 15 minutos, todo el día**, y al arrancar, por si el servicio estaba caído. Cada lote vence a
la **medianoche de su comercio más `SETTLEMENT_BATCH_CLOSE_GRACE`** (30 min por defecto): es el plazo para que el
terminal lo cierre él y concilie. Con `SETTLEMENT_BATCH_CLOSE_HOUR=0` queda desactivado.

1. Busca los lotes `OPEN` de **días de negocio anteriores** que ya vencieron, en la zona de cada comercio. No
   toca el lote del día, porque sigue recibiendo ventas.
2. Los cierra con los totales del backend: **no hay conciliación**, porque el terminal no informó los
   suyos, y por eso tampoco hay discrepancias.
3. Encola `batch-closed` en la misma transacción y envía el lote al procesador. Un lote vacío solo se cierra.
   **Con una revisión de reversa abierta, queda en `DISPUTED`, sin enviarse.**

Un evento sintético en `processed_events`, determinístico por lote y día, evita que dos instancias cierren
el mismo lote. Si falla el cierre de un lote, el job sigue con los demás y reintenta en el minuto
siguiente.

### 5.7 Disputas

| Operación | Efecto | Cuándo usarla |
|---|---|---|
| `reopen` | `DISPUTED` → `OPEN`. Descarta los totales y vuelve a aceptar transacciones | Faltan transacciones, o el terminal puede volver a cerrar con los totales correctos |
| `resolve-dispute` | `DISPUTED` → `CLOSED`. Recalcula con las transacciones actuales y deja las discrepancias en 0 | El backend es correcto y los totales del terminal no |

**Para volver a cerrar un lote de otro día,** el pedido debe informar su `batch_date`, dentro de
`SETTLEMENT_CLOSE_MAX_AGE_DAYS`. Un lote anterior a ayer solo se puede cerrar así si se reabrió con `reopen`. Si no
llega ningún pedido, lo cierra el job automático al día siguiente, sin conciliar.

**`reopen` falla si ya hay otro lote `OPEN`** de ese terminal, día y moneda, porque solo puede haber uno. En
ese caso, usar `resolve-dispute`.

Después de `resolve-dispute`, hay que enviar el lote con `resubmit`. Ninguna de las dos operaciones publica
un nuevo evento: si los totales cambian, Notification no se entera.

### 5.8 Liquidación

`settle` confirma un lote `SUBMITTED` y lo pasa a `SETTLED`. La confirmación es manual, porque no hay un
procesador que la informe.

Cuando se liquidó el último lote con transacciones de un comercio en esa fecha, se publica
`settlement.completed`, un evento por moneda. Se encola en la misma transacción que la liquidación: si no se
puede encolar, `settle` falla, el lote sigue `SUBMITTED` y se puede repetir.

```
neto = bruto − redondeo(bruto × MDR / 100)
```

La verificación usa un advisory lock por comercio y fecha: si se liquidan en paralelo los dos últimos lotes,
el segundo ve lo que confirmó el primero, y sale un solo evento.

Si después un terminal del mismo comercio abre otro lote para esa fecha, al liquidarlo se publica otro
evento con el total acumulado.

### 5.9 Qué pasa cuando falla cada paso

| Fallo | Resultado | ¿Se recupera solo? |
|---|---|---|
| IDs inválidos | `Term`; no se reintenta | No |
| Error de base de datos al registrar | `Nak` y reintento con espera (ver [§8](#8-eventos-nats)) | **Sí**, si se resuelve en unos 12 minutos |
| Se agotan las entregas | El evento no se reintenta; log `deliveries exhausted` y alerta `EventoSettlementPerdido` | No: hay que [reprocesarlo](../runbooks/nats-dlq-reprocess.md) |
| Conflicto con otro guardado del mismo lote | Reintento interno, y después `Nak` | **Sí** (ver [§5.10](#510-concurrencia)) |
| Moneda inválida o monto no positivo | `Term`; no se reintenta | No |
| Se pierde `auth.approved` en Authorization | La transacción falta en el lote; el cierre da `DISPUTED` | No |
| Falla el guardado del cierre o de su evento | `Nak`; el lote sigue `OPEN` y el cierre se reintenta | **Sí** |
| NATS no disponible al publicar | Los eventos esperan en el outbox; alerta `OutboxSettlementTrabado` | **Sí**, al volver NATS |
| Cierre sin procesador configurado | Queda `CLOSED` | Con `resubmit` |
| El procesador falla | Queda `CLOSED` | Con `resubmit` |
| El terminal nunca pide el cierre | El job lo cierra sin conciliar | **Sí** |
| Falla el cierre automático de un lote | Reintenta en el minuto siguiente | **Sí** |
| Falla el guardado de `settle` o de su evento | `500`; el lote sigue `SUBMITTED` | Con operador: repetir `settle` |

### 5.10 Concurrencia

Cada lote tiene una `version`. `Save` solo actualiza si la versión de la base es la que se cargó: si otro
proceso la cambió, nada se guarda y devuelve `ErrStaleBatch`. Así, una venta simultánea no puede devolver a
`OPEN` un lote recién cerrado, y un cierre no calcula totales sin esa venta.

Las ventas, reversas y cierres **reintentan internamente** hasta 8 veces, con espera aleatoria, volviendo a
cargar el lote. Si el conflicto persiste, el error llega a NATS como transitorio. Las operaciones de
administración responden `409`, y hay que repetirlas.

## 6. API HTTP

Puerto `HTTP_PORT`: 8083 por defecto en el código y **8084 en Docker Compose**.

**Todo exige un token, enviado como `Authorization: Bearer <token>`, salvo `GET /healthz`, `/readyz` y `/metrics`,**
que usan Docker y Prometheus y no exponen datos de comercios. La lista de rutas públicas es exacta: una ruta nueva
queda protegida por defecto.

| Respuesta | Situación |
|---|---|
| `401 UNAUTHORIZED` | Sin token, con uno incorrecto o con uno vencido |
| `403 FORBIDDEN` | El rol del token no alcanza |
| `403 OPERATOR_MISMATCH` | El `operator_id` del body no es el del token |
| `429 TOO_MANY_ATTEMPTS` | Demasiados tokens incorrectos desde esa IP. Indica la espera en `Retry-After` |
| `503 ADMIN_API_DISABLED` | No hay ningún token configurado: la API queda deshabilitada, no abierta |

### Tokens y roles

Cada persona o sistema tiene su token, en el archivo de `SETTLEMENT_ADMIN_TOKENS_FILE`. **El archivo guarda el SHA-256,
no el token:**

```json
[
  { "name": "soporte-juan", "role": "operator", "sha256": "<64 caracteres hex>", "expires_at": "2027-06-30T00:00:00Z" },
  { "name": "tesoreria-ana", "role": "supervisor", "sha256": "<64 caracteres hex>" }
]
```

```bash
TOKEN=$(openssl rand -hex 32)      # Se le entrega a su dueño, y no se guarda
printf %s "$TOKEN" | sha256sum     # Va en el archivo
```

| Rol | Puede |
|---|---|
| `reader` | Consultar lotes, revisiones y zonas |
| `operator` | Además, `force-close`, `resubmit` y `reopen` |
| `supervisor` | Además, `resolve-dispute`, `settle`, cerrar revisiones de reversas y definir zonas. Una ruta nueva exige este rol hasta que se indique otro |

**Con un token del archivo, el operador de la auditoría es el `name` del token.** `operator_id` se puede omitir. Si
se envía otro, responde `403` y no cambia nada.

`expires_at` es opcional. El servicio **no arranca** si el archivo no existe o es inválido.

**Rotar o dar de baja un token no requiere reiniciar:** el archivo se relee cada 30 segundos. Para rotar sin
cortes, agregá el token nuevo con otro `name`, cambiá el cliente y quitá el viejo. Si el archivo pasa a ser inválido o
no se puede leer, se conservan los tokens vigentes y queda el log `admin tokens file not reloaded`: **una baja no rige
hasta corregirlo**. En Docker, montá el directorio y no el archivo: un archivo montado solo no ve los reemplazos.

**`SETTLEMENT_ADMIN_API_KEY` sigue funcionando,** con el rol `supervisor`, pero no identifica a nadie: `operator_id`
sigue siendo lo que declara quien llama. El servicio lo advierte al arrancar. Conviene quitarla al crear los tokens.

### Límite de intentos

Con **10 tokens incorrectos en un minuto desde una IP, esa IP queda bloqueada 5 minutos**, incluso con un token
válido. Un pedido sin token no cuenta. Las rutas públicas no se bloquean. Un acceso correcto borra los fallos.

Detrás de un proxy, todos los pedidos llegan con la IP del proxy: un solo cliente bloquearía a los demás. Hay que
declararlo en `SETTLEMENT_TRUSTED_PROXIES` para que se use `X-Forwarded-For`, que en cualquier otro caso se ignora.

### TLS

Con `SETTLEMENT_TLS_CERT` y `SETTLEMENT_TLS_KEY`, el puerto `HTTP_PORT` sirve solo HTTPS, desde TLS 1.2. Un certificado
nuevo requiere reiniciar. Hay que cambiar además lo que consulta ese puerto:

| Cliente | Cambio |
|---|---|
| `healthcheck` de Docker Compose | `wget --no-check-certificate -q --spider https://localhost:8084/readyz` |
| Prometheus | `scheme: https` en `prometheus.yml`, con la CA del certificado |

Si TLS lo termina un proxy, hay que declararlo con `SETTLEMENT_TLS_EXTERNAL=true` y limitar el acceso directo al
puerto. **Con `ENVIRONMENT=production`, el servicio no arranca sin una de las dos opciones.** En cualquier otro
entorno, arranca y lo advierte en el log. Docker Compose no usa TLS: es solo para desarrollo.

### Rutas

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; verifica Postgres, las tablas requeridas y la conexión a NATS | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `GET /batches/{id}` | Consultar un lote | `200` | `404`, `500` |
| `GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD` | Lotes de un comercio; `date` es obligatorio | `200` | `400`, `500` |
| `POST /batches/{id}/force-close` | Forzar el cierre | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/resubmit` | Enviar un lote `CLOSED` | `200` | `400`, `404`, `409`, `500`, `503` |
| `POST /batches/{id}/reopen` | Reabrir un lote `DISPUTED` | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/resolve-dispute` | Aceptar los totales del backend | `200` | `400`, `404`, `409`, `500` |
| `POST /batches/{id}/settle` | Confirmar la liquidación | `200` | `400`, `404`, `409`, `500` |
| `GET /reversal-reviews` | Revisiones abiertas, hasta 200, de la más antigua a la más nueva | `200` | `500` |
| `POST /reversal-reviews/{transaction_id}/resolve` | Cerrar una revisión y corregir el lote si corresponde | `200` | `400`, `404`, `409`, `500` |
| `GET /merchants/{merchant_id}/timezone` | Zona vigente del comercio | `200` | `400`, `500` |
| `PUT /merchants/{merchant_id}/timezone` | Definir la zona del comercio | `200` | `400`, `500` |

Todas las rutas no públicas pueden responder además `401`, `403`, `429` o `503`, como se explicó arriba. Un `{id}` que no sea un UUID responde `400` con `INVALID_ID`. Los errores tienen la forma
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

Responde `{"status": "closed"}`. El lote queda en `CLOSED` con los **totales del backend**, sin discrepancias y
**sin conciliar**, porque no hay totales del terminal. Después:

- Encola `batch-closed` en la misma transacción, salvo que el lote esté vacío.
- No envía el lote: hay que usar `resubmit`.
- Registra la operación en la [auditoría](#auditoría-de-operadores) y en los logs.

Solo funciona sobre lotes `OPEN`: en cualquier otro estado responde `400`. Los lotes cerrados así
antes de esta corrección conservan sus discrepancias falsas: en el primer `resubmit` pasan a `DISPUTED`, y
salen con `resolve-dispute`.

### Enviar al procesador

Lleva el mismo body y responde `{"status": "submitted"}`.

| Situación | Respuesta |
|---|---|
| El lote no está en `CLOSED` | `400` |
| El lote tiene discrepancias o una reversa en revisión | `400`, y **el lote pasa a `DISPUTED`**, sin enviarse |
| Sin procesador configurado | `503` con `PROCESSOR_UNAVAILABLE`; el lote queda en `CLOSED` |
| Falla el procesador | `500` |

### Resolver disputas

`reopen` y `resolve-dispute` exigen `operator_id` y `reason`, y responden `{"status": "reopened"}` o
`{"status": "closed"}`. Con un lote que no esté en `DISPUTED`, responden `400`. `reopen` también responde
`400` si ya hay otro lote `OPEN` de ese terminal, día y moneda, y `resolve-dispute` si el lote tiene reversas en
revisión.

```json
{ "operator_id": "soporte-juan", "reason": "Conciliado con el comercio, ticket 123" }
```

Ambos datos quedan en la [auditoría](#auditoría-de-operadores) y en los logs.

### Revisiones de reversas

`GET /reversal-reviews` responde `{"reviews": [...], "count": n}`, con la reversa, la compra (`null` si no está) y
el estado actual del lote.

`POST /reversal-reviews/{transaction_id}/resolve` responde `{"status": "resolved"}`:

```json
{ "operator_id": "soporte-juan", "resolution": "Conciliado con Authorization, ticket 123", "action": "USE_PURCHASE_AMOUNT" }
```

| `action` | Efecto | Se puede usar |
|---|---|---|
| `ACCEPT` | Deja la reversa como está | Siempre |
| `USE_PURCHASE_AMOUNT` | Cambia el monto de la reversa por el de la compra | Con `AMOUNT_MISMATCH`, si el lote está en `OPEN` o `DISPUTED` |
| `EXCLUDE` | Quita la reversa del lote | Si el lote está en `OPEN` o `DISPUTED` |

Fuera de esos casos, o si la revisión ya está cerrada, responde `400` y no cambia nada. La corrección, el cierre de
la revisión y la [auditoría](#auditoría-de-operadores) son atómicos. `resolved_by` es el operador, y la acción queda
en `reversal_reviews.action`. Si era la última revisión de un lote `DISPUTED`, después
[se vuelve a conciliar](#52-reversa) y, si coincide, se libera y se envía.

**Una revisión reabierta de una reversa excluida** (su compra llegó y no coincide) tiene la reversa fuera de todo
lote: `ACCEPT` la deja excluida, `USE_PURCHASE_AMOUNT` la vuelve a agregar con el monto de la compra, y `EXCLUDE`
responde `400`.

### Zona horaria de un comercio

`PUT /merchants/{merchant_id}/timezone` responde `{"status": "updated"}`. Una zona que no reconozcan el servicio o
Postgres responde `400`. Si mover el lote de hoy chocaría con otro lote `OPEN` de un terminal en la fecha nueva,
responde `409` (`OPEN_BATCH_CONFLICT`) y no cambia nada: hay que cerrar antes ese lote. Ver
[§5.5](#55-día-de-negocio):

```json
{ "timezone": "America/Mexico_City", "operator_id": "soporte-juan" }
```

`GET` responde `{"merchant_id": "...", "timezone": "...", "source": "merchant"}`, o `"source": "default"` si el
comercio usa la zona general. Quién hizo el cambio queda en `merchant_timezones.updated_by` y en los logs. Cada lote
movido queda además en `batch_audit_log`, con la acción `REDATE` y las dos fechas en `reason`.

### Confirmar la liquidación

`settle` exige `operator_id` y responde `{"status": "settled"}`. Con un lote que no esté en `SUBMITTED`,
responde `400`. El `operator_id` queda en la [auditoría](#auditoría-de-operadores) y en los logs.

## 7. gRPC

Hay un servidor en `GRPC_PORT` (9093 por defecto) sin servicios registrados, y el contexto no tiene un
`.proto`. Es un placeholder: cualquier llamada devuelve `Unimplemented`.

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
| Cualquier otro error | `Nak` con espera | Reentrega a los 5 s, 30 s, 2 min y 10 min |

Las esperas dan unos 12 minutos para que se recupere, por ejemplo, una base caída. Los conflictos de versión
no llegan a este punto: se reintentan antes, como explica [§5.10](#510-concurrencia).

Un mensaje que agota las entregas no pasa a una DLQ: queda el log `deliveries exhausted` y salta la alerta
`EventoSettlementPerdido`. Para recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md).

### Publicados

| Subject | Stream | ¿Se publica? | Consumidor |
|---|---|---|---|
| `posnet.settlement.batch-closed.v1` | `POSNET_SETTLEMENT` | Sí, por outbox | Notification |
| `posnet.settlement.completed.v1` | `POSNET_SETTLEMENT` | Sí, por outbox | Ninguno: Notification no lo consume |

Los eventos se guardan en `settlement.outbox`, en la misma transacción que el lote. Un Relay revisa la tabla
cada segundo, publica hasta 50 eventos por ciclo y borra los enviados. Puede correr en varias instancias:
cada fila la toma una sola, con `FOR UPDATE SKIP LOCKED`.

**La entrega es al menos una vez.** Si el Relay publica y no llega a borrar la fila, reenvía el evento con el
mismo `Nats-Msg-Id`: JetStream lo descarta dentro de su ventana de deduplicación, de 2 minutos por defecto.
Pasada esa ventana, llega duplicado, y el consumidor debe descartarlo por `event_id`, como hace Notification.

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
hace nada, [`migrations/settlement/`](../../migrations/settlement/) se aplica a mano y
[`st_schema.sql`](../../context/settlement/infrastructure/postgres/st_schema.sql) no se ejecuta.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `settlement_batches` | Lotes | PK `id`; único `(terminal_id, batch_date, currency)` solo para `OPEN` |
| `batch_transactions` | Transacciones del lote | PK `id`; único `(transaction_id, tx_type)`; FK `batch_id` |
| `processed_events` | Idempotencia | PK `event_id` |
| `outbox` | Eventos pendientes de publicar | PK `id`; índice `created_at` |
| `reversal_reviews` | Reversas que no coinciden con su compra | PK `transaction_id`; FK `batch_id`; índice `created_at` de las abiertas |
| `batch_audit_log` | Cambios de estado hechos por operadores | PK `id`; FK `batch_id`; índice `(batch_id, created_at)` |
| `merchant_timezones` | Zona de negocio de los comercios que no usan la general | PK `merchant_id` |
| `maintenance_runs` | Tareas de mantenimiento ya ejecutadas en el entorno, como la recuperación de reversas | PK `name` |

| Índice | Para qué sirve |
|---|---|
| `idx_batch_transactions_batch` | Cargar las transacciones de un lote, en cada venta |
| `idx_settlement_batches_merchant_date` | Listar los lotes de un comercio y comprobar la liquidación diaria |
| `idx_settlement_batches_state` | Buscar lotes pendientes, como en el cierre automático |

Con 2.000 lotes y 400.000 transacciones, cargar un lote pasó de recorrer toda la tabla en 97 ms a usar el
índice en 7 ms, en una medición puntual.

### Aplicar la migración

**Una base existente necesita `000001` y de `000003` a `000008` antes de desplegar esta versión del servicio.**
`01-init.sql` solo corre sobre una base vacía, y `pgutil.Migrate` no hace nada:

```bash
psql "$POSTGRES_DSN" -f migrations/settlement/000001_batch_integrity.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000002_batch_indexes.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000003_outbox.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000004_batch_audit.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000005_reversal_reviews.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000006_merchant_timezones.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000007_release_held_batches.up.sql
psql "$POSTGRES_DSN" -f migrations/settlement/000008_maintenance_runs.up.sql
```

Son idempotentes y no tocan los datos. Se probaron sobre una base con el schema anterior: el resultado
coincide con el de una base nueva.

**Con una tabla faltante, o una columna de `000007`, `/readyz` responde `503` con `database schema incomplete`,** y el
log `required tables are missing` la nombra. El `healthcheck` de Docker Compose usa `/readyz`, así que el contenedor
queda `unhealthy`. Las columnas que verifica son solo las de `000007`: no detecta si falta `000001`. **`unhealthy` no reinicia el servicio ni frena el consumo de
eventos:** mientras falte una tabla, los que la usen fallan y, al agotar los reintentos, se pierden.

**Sin `000003`, fallan los cierres y las liquidaciones,** y `/metrics` deja de informar los lotes que
requieren atención. **Sin `000004`, fallan las operaciones de administración.** **Sin `000005` o `000006`, fallan todas las ventas y reversas,**
y al agotar los reintentos sus eventos se pierden. Sin `000006` también fallan los cierres. **Sin `000007`, falla cada
guardado de un lote:** ventas, reversas, cierres y operaciones. Todas se pueden aplicar con el servicio anterior en
marcha, porque solo agregan tablas o columnas que admiten `NULL`. Los lotes cerrados antes de `000007` no tienen
referencia de conciliación y no se liberan solos. **Sin `000008`, `/metrics` deja de informar los lotes que requieren
atención,** y `/readyz` responde `503`. No inserta ninguna fila: después hay que ejecutar
`settlement-recover-reversals` (Escenario E), o la alerta `ReversasSinRecuperar` sigue activa.

`000002` crea los índices con `CONCURRENTLY`, sin bloquear las escrituras, y por eso no puede correr dentro de
una transacción: no usar `psql -1`. Si se interrumpe, puede dejar un índice inválido: hay que borrarlo y volver
a ejecutarla. Se puede aplicar después del despliegue, porque el servicio no depende de ella para funcionar.

`000001_batch_integrity.down.sql` la revierte, pero **falla sin cambiar nada** si ya hay varios lotes por
terminal y fecha o filas `REVERSAL`, porque el schema anterior no los admite. Resolver esos datos es una
decisión financiera, y el script no los borra. `000003_outbox.down.sql` también falla sin cambiar nada si
quedan eventos sin publicar: hay que esperar a que salgan. `000004_batch_audit.down.sql` falla si hay
registros de auditoría, para no perderlos. `000005_reversal_reviews.down.sql` falla si hay revisiones abiertas, y
exige volver antes a una versión del servicio que no use la tabla. `000006_merchant_timezones.down.sql` exige lo
mismo: los comercios configurados vuelven a la zona general. `000007_release_held_batches.down.sql` también: se pierde
la referencia de conciliación de los lotes y la acción de las revisiones. `000008_maintenance_runs.down.sql` también:
se pierde el registro de la recuperación.

### Auditoría de operadores

Cada cambio de estado hecho con `force-close`, `resubmit`, `reopen`, `resolve-dispute` o `settle`, y cada revisión
de reversa cerrada por un operador, deja una fila en `batch_audit_log`, **en la misma transacción que el cambio:** si no se puede auditar, responde `500` y el
lote no cambia.

| Columna | Contenido |
|---|---|
| `action` | `FORCE_CLOSE`, `RESUBMIT`, `RESUBMIT_BLOCKED`, `REOPEN`, `RESOLVE_DISPUTE` o `SETTLE`; `REVIEW_*` al cerrar una revisión; `REDATE` y `AUTO_RELEASE`, del sistema |
| `operator_id` | El `name` del token. Con `SETTLEMENT_ADMIN_API_KEY`, lo que declaró quien llama |
| `reason` | Lo que declaró quien llama. Solo existe en `reopen`, `resolve-dispute` y las revisiones |
| `from_state`, `to_state` | Estado antes y después |
| `previous_discrepancies` | Discrepancias antes del cambio |
| `total_amount` | Total después del cambio; vacío si el lote se reabrió |
| `confirmation_id` | Lo que devolvió el procesador en un `resubmit` |
| `created_at` | Fecha y hora del cambio |

```sql
SELECT created_at, action, operator_id, reason, from_state, to_state, previous_discrepancies, total_amount
FROM settlement.batch_audit_log
WHERE batch_id = '<batch_id>'
ORDER BY created_at;
```

**No se auditan** las operaciones rechazadas, porque no cambian nada, ni los cierres del terminal o del job
automático, que no los hace un operador. Sí se auditan dos cambios del sistema que siguen a un operador: el lote que
movió un cambio de zona (`REDATE`) y el que liberó el cierre de una revisión (`AUTO_RELEASE`, `operator_id` `system`). Las operaciones anteriores a `000004` solo están en los logs.

### Costo por aprobación

Cada aprobación carga todas las transacciones del lote y las vuelve a insertar con `ON CONFLICT DO
NOTHING`. El costo por evento crece con el tamaño del lote, y con él el costo acumulado del día.

### Apagado

Ante `SIGINT` o `SIGTERM`: detiene HTTP (hasta 10 s) y gRPC, espera al Relay, drena y cierra NATS, y cierra
el pool de Postgres.

## 10. Observabilidad y operación

### Métricas

| Métrica | Tipo | Labels | Significado |
|---|---|---|---|
| `posnet_settlement_auth_approved_total` | Counter | — | Aprobaciones registradas |
| `posnet_settlement_reversals_total` | Counter | — | Reversas procesadas; incluye duplicados que no agregan fila |
| `posnet_settlement_batches_total` | Counter | `currency`, `state` | Cierres, por estado resultante: `CLOSED` o `DISPUTED`. Los cierres anteriores a esta versión figuran como `CLOSED` |
| `posnet_settlement_batch_duration_seconds` | Histogram | — | Duración del cierre |
| `posnet_settlement_submissions_total` | Counter | `result` | Envíos tras un cierre: `submitted`, `unavailable` o `error`. No incluye `resubmit` |
| `posnet_settlement_batches_attention` | Gauge | `reason` | Lotes que requieren atención (ver abajo). Consulta la base en cada scrape |
| `posnet_settlement_outbox_stuck` | Gauge | — | Eventos en el outbox desde hace más de 1 minuto |
| `posnet_settlement_reversals_in_review` | Gauge | — | Revisiones de reversas abiertas desde hace más de 15 minutos, para dar tiempo a una compra demorada |
| `posnet_settlement_reversal_recovery_pending` | Gauge | — | 1 mientras no se haya registrado la recuperación de reversas anteriores |
| `posnet_settlement_batch_close_rejected_total` | Counter | `reason` | Cierres del terminal rechazados por su `batch_date`: `future`, `too_old` o `not_reopened` |

También se exponen métricas del pool de Postgres y de mensajes NATS, incluida
`posnet_nats_messages_failed_total{kind="exhausted"}` para los eventos que agotaron las entregas.

| `reason` | Qué cuenta |
|---|---|
| `disputed` | Lotes en `DISPUTED` |
| `closed_unsent` | Lotes con transacciones en `CLOSED` hace más de 1 hora |
| `open_overdue` | Lotes `OPEN` de días de negocio anteriores, pasado `SETTLEMENT_BATCH_CLOSE_GRACE` desde la medianoche de su comercio: los que el job ya debería haber cerrado |

Si la consulta falla, estas series no se informan en ese scrape, y las alertas que dependen de ellas no se evalúan.

### Alertas

| Alerta | Severidad | Se activa cuando | Qué hacer |
|---|---|---|---|
| `EventoSettlementPerdido` | Crítica | Un evento agotó las entregas. Sigue activa hasta reiniciar el servicio | [Reprocesar el evento](../runbooks/nats-dlq-reprocess.md) |
| `OutboxSettlementTrabado` | Warning | Hay eventos en el outbox hace más de 1 minuto, durante 5 min | Revisar NATS y el log `outbox relay`. Se publican solos al resolver la causa |
| `ReversasEnRevision` | Warning | Hay revisiones abiertas hace más de 15 min, durante 5 min | [Escenario H](../runbooks/batch-close-failure.md#escenario-h--reversas-en-revisión) |
| `ReversasSinRecuperar` | Warning | Falta registrar la recuperación de reversas anteriores, durante 15 min | [Escenario E](../runbooks/batch-close-failure.md#escenario-e--reversas-anteriores-sin-descontar) |
| `LotesEnDisputa` | Warning | Hay lotes en disputa durante 15 min | [Escenario A](../runbooks/batch-close-failure.md#escenario-a--lote-en-disputed) |
| `LotesCerradosSinEnviar` | Warning | `closed_unsent` > 0 durante 15 min | [Escenario C](../runbooks/batch-close-failure.md#escenario-c--lote-en-closed-sin-enviar) |
| `LotesAbiertosVencidos` | Warning | `open_overdue` > 0 durante 30 min | [Escenario F](../runbooks/batch-close-failure.md#escenario-f--lotes-abiertos-de-días-anteriores) |
| `EnvioAlProcesadorFallando` | Warning | Hubo envíos `unavailable` o `error` en la última hora. No detecta el primer envío tras un reinicio, pero sí lo hace `LotesCerradosSinEnviar` | [Escenario C](../runbooks/batch-close-failure.md#escenario-c--lote-en-closed-sin-enviar) |

**Las alertas no avisan a nadie todavía:** Alertmanager las envía a `webhook-mock`. **Dashboard:**
`posnet-bc-detail` muestra la duración del cierre.

### Para detectar problemas

```sql
-- Lotes que requieren atención
SELECT id, terminal_id, batch_date, state, discrepancies, closed_at
FROM settlement.settlement_batches
WHERE state = 'DISPUTED'
   OR (state = 'CLOSED' AND COALESCE(total_count, 0) > 0)
   OR (state = 'OPEN' AND batch_date < (NOW() AT TIME ZONE COALESCE(
         (SELECT timezone FROM settlement.merchant_timezones m WHERE m.merchant_id = settlement_batches.merchant_id),
         'America/Argentina/Buenos_Aires'))::date)
ORDER BY batch_date DESC;
```

En los logs, `modified concurrently` indica conflictos de versión, que son esperables con tráfico simultáneo,
`no open batch found` indica cierres sin efecto, `deliveries exhausted` un evento perdido y
`outbox relay: publish cycle failed` que el Relay no puede publicar y `reversal registered for review` una
reversa que no coincide con su compra. `batch closed automatically` identifica los cierres sin
conciliar, y `no settlement processor configured` los lotes que quedaron sin enviar.

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `NATS_URL` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `HTTP_PORT` | `8083` | Sí; Docker Compose usa `8084` |
| `GRPC_PORT` | `9093` | Sí |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `15`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-settlement` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `SETTLEMENT_MDR_PERCENT` | `2.5` | Sí; se usa para calcular el neto. Fuera de 0–10, el servicio no arranca |
| `SETTLEMENT_BATCH_CLOSE_GRACE` | `30m` | Sí; plazo tras la medianoche de cada comercio para el cierre automático. Fuera de 0–12h, o si no es una duración, el servicio no arranca |
| `SETTLEMENT_BATCH_CLOSE_HOUR` | `3` | Solo `0`, que desactiva el cierre automático. Ya no es una hora: con otro valor configurado, el arranque deja un `WARN`. Fuera de 0–23, el servicio no arranca |
| `SETTLEMENT_PROCESSOR` | `none` | Sí; `none` o `mock`. Docker Compose usa `mock`. Con otro valor, el servicio no arranca |
| `SETTLEMENT_BUSINESS_TIMEZONE` | `America/Argentina/Buenos_Aires` | Sí; zona IANA del día de negocio, para los comercios sin una propia. Si es inválida, el servicio no arranca |
| `SETTLEMENT_CLOSE_MAX_AGE_DAYS` | `7` | Sí; antigüedad máxima del `batch_date` de un cierre. `0` no limita. Si es negativo, el servicio no arranca |
| `SETTLEMENT_ADMIN_API_KEY` | Vacía | Sí; token único con el rol `supervisor`, que no identifica a nadie. Con menos de 32 caracteres, el servicio no arranca |
| `SETTLEMENT_ADMIN_TOKENS_FILE` | Vacía | Sí; archivo de [tokens](#tokens-y-roles). Sin esto ni la clave anterior, la API queda deshabilitada. Si no existe o es inválido, el servicio no arranca |
| `SETTLEMENT_TRUSTED_PROXIES` | Vacía | Sí; CIDR separados por coma cuyo `X-Forwarded-For` se acepta. Si alguno es inválido, el servicio no arranca |
| `SETTLEMENT_TLS_CERT`, `SETTLEMENT_TLS_KEY` | Vacías | Sí; habilitan [HTTPS](#tls). Van juntas. Si no se pueden cargar, el servicio no arranca |
| `SETTLEMENT_TLS_EXTERNAL` | `false` | Sí; declara que un proxy termina TLS |
| `SETTLEMENT_SUBMIT_RETRIES` | `3` | **No**: no hay procesador real |
| `SETTLEMENT_SUBMIT_TIMEOUT` | `30s` | **No**: no hay procesador real |
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
BUSINESS_DATE=$(TZ=America/Argentina/Buenos_Aires date +%F)
curl -s -H "$AUTH" "localhost:8084/merchants/$MERCHANT/batches?date=$BUSINESS_DATE" | jq
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
BATCH=$(curl -s -H "$AUTH" "localhost:8084/merchants/$MERCHANT/batches?date=$BUSINESS_DATE" | jq -r '.batches[0].ID')
OP='"operator_id":"prueba"'
AUTH="Authorization: Bearer $SETTLEMENT_ADMIN_API_KEY"

curl -s -X POST localhost:8084/batches/$BATCH/resolve-dispute -H "$AUTH" -d "{$OP,\"reason\":\"prueba\"}"
curl -s -X POST localhost:8084/batches/$BATCH/resubmit -H "$AUTH" -d "{$OP}"
curl -s -X POST localhost:8084/batches/$BATCH/settle -H "$AUTH" -d "{$OP}"
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
| Los lotes retenidos antes de `000007` no se liberan solos | No tienen guardado contra qué se conciliaron: siguen `DISPUTED` hasta un `resolve-dispute` o `reopen` | `SettlementBatch.Release` |
| Una liberación fallida no se reintenta | Si falla después de cerrar la revisión, el lote sigue `DISPUTED`, con el log `could not release the held batch` | `ReleaseHeldBatch` |
| Sin corrección en un lote ya enviado | `USE_PURCHASE_AMOUNT` y `EXCLUDE` no aplican a un lote `SUBMITTED` o `SETTLED`: el ajuste se hace con el procesador | `applyReviewAction` |
| Una venta simultánea al cambio de zona puede abrir otro lote | Si leyó la zona anterior justo antes del cambio, busca el lote en la fecha vieja, que ya se movió, y abre uno nuevo. La ventana es de milisegundos | `SetMerchantTimezone`, `RegisterApproval` |

### Ciclo de vida del lote

| Brecha | Consecuencia | Dónde |
|---|---|---|
| No hay procesador real | `mock` marca `SUBMITTED` sin enviar nada. Falta el adaptador y el archivo de remesa, que requieren la especificación del procesador. `SETTLEMENT_SUBMIT_RETRIES` y `SETTLEMENT_SUBMIT_TIMEOUT` siguen sin efecto | `st_mock_processor.go` |
| La confirmación de liquidación es manual | Un operador debe llamar a `settle`, sin una referencia del procesador | `SettleBatch` |
| Nadie consume `settlement.completed` | El comercio no recibe el neto liquidado: Notification no se suscribe | `nt_nats_subscriber.go` |
| Evento adicional por lotes tardíos | Un lote abierto después de la liquidación diaria genera otro evento con el total acumulado | `enqueueCompletedIfDone` |
| El cierre automático no concilia | Los lotes se envían con los totales del backend, sin comparar con el terminal | `CloseStaleBatches` |
| `force-close` no concilia | El operador asume que los totales del backend son correctos | `ForceClose` |
| Doble envío posible | Si el procesador acepta y falla el guardado o la auditoría, el lote queda `CLOSED`, y `resubmit` lo enviaría otra vez. En `resubmit` queda el log `accepted by processor but not saved` con el `confirmation_id` | `submitBatch`, `ResubmitBatch` |
| `resolve-dispute` no notifica | Si los totales cambian, Notification solo conoce los del primer cierre | `ResolveDispute` |
| La auditoría no tiene API ni protección propia | Se consulta por SQL, y quien tenga acceso de escritura a la base puede modificarla | `batch_audit_log` |
| `force-close`, `resubmit` y `settle` no piden motivo | Su auditoría queda sin `reason`. Exigirlo cambiaría el contrato de la API | `st_http_handler.go` |
| Lotes vacíos en `CLOSED` | Los lotes vacíos que cierra el job quedan en `CLOSED` para siempre y aparecen en la API. No cuentan para las alertas | `autoCloseBatch` |

### Seguridad

| Brecha | Consecuencia | Dónde |
|---|---|---|
| `SETTLEMENT_ADMIN_API_KEY` no identifica | Mientras esté habilitada, quien la tenga puede hacer todo, declarando cualquier `operator_id`. Hay que crear los tokens y quitarla en cada entorno | `TokenStore` |
| Los roles son fijos | No se puede limitar un token a un comercio ni cambiar qué permite cada rol sin tocar el código | `requiredRole` |
| Las lecturas no se auditan | El log registra la ruta, pero no quién consultó | `authMiddleware` |
| `/metrics` es público | Con acceso de red, se ven contadores y lotes por estado, sin datos de comercios. Hay que limitar el acceso al puerto | `publicPaths` |
| El límite de intentos es por instancia | Vive en memoria: se reinicia con el servicio y no se comparte entre instancias | `authLimiter` |
| Vencimientos sin aviso | Un token vencido responde `401`, sin un aviso previo ni una alerta | `TokenStore` |
| Los certificados requieren reiniciar | No se recargan solos, como los tokens | `st_wire.go` |
| Solo cubre la API de Settlement | Los otros BC no cambian, y Postgres y NATS tienen su propia configuración de acceso | `authMiddleware` |

### Operación y datos

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Las alertas no avisan a nadie | Alertmanager las envía a `webhook-mock`: hay que mirarlas en Prometheus o Alertmanager | Alertmanager |
| Costo creciente por aprobación | Cada venta carga y reinserta todas las transacciones del lote. Los índices evitan recorrer toda la tabla, pero el costo sigue creciendo con el tamaño del lote | `Save`, `loadTransactions` |
| Los eventos publicados no llevan la traza | El Relay publica sin el contexto del pedido: la traza del cierre no continúa en Notification. Se correlaciona por `correlation_id` | `pkg/outbox` |
| Una fila que no se puede publicar frena a las demás | El ciclo se revierte ante el primer fallo. Hoy solo ocurre si NATS o el stream no están disponibles | `pkg/outbox` |
| Un cierre sin lote no recibe respuesta | Terminal Gateway ya respondió `202`, y Settlement solo deja un `WARN` | `ProcessBatchClose` |
| Sin DLQ ni reintento pasados unos 12 minutos | Una caída más larga agota las 5 entregas: el evento queda sin registrar hasta que alguien lo reprocesa | `nak` |
| Un reintento puede llegar después del cierre | Si una venta está esperando su reentrega cuando el terminal cierra, el cierre da `DISPUTED` y la venta entra en un lote nuevo | `retryDelays` |
| Sin migraciones automáticas | Los cambios de schema hay que aplicarlos a mano en las bases existentes | `pkg/pgutil/migrate.go` |
| gRPC placeholder | Abre un puerto que no ofrece nada | `st_grpc_server.go` |
| Código sin uso | `port.BatchService` y `port.AdminService` no se implementan; tampoco se usan `mdrPercent`, `BatchTxOffline` ni los eventos de dominio internos | `st_port_inbound.go`, `st_batch_handler.go`, `st_settlement_batch.go` |

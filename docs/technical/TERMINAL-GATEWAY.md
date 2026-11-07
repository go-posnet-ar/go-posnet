# Terminal Gateway — Documentación técnica

> Revisado contra el commit `8acbfdf` (2026-11-05). Este documento describe lo que el código hace
> hoy. Si algo difiere del [README del contexto](../../context/terminal-gateway/README.md), vale lo
> que está acá: el README describe parte del diseño objetivo.

## 1. Resumen

Terminal Gateway (TG) es el borde del sistema: administra la sesión de pago, inicia la Saga de
autorización y recibe su resultado.

Los terminales reciben el resultado de sus ventas por un [WebSocket](#7b-websocket-de-terminales), con mensajes JSON. El
diseño original prevé además terminales físicos con mTLS y mensajes ISO 8583: **esa parte todavía no existe**. Hoy el
único terminal es el cajero de `posnet-front`, que usa HTTP para operar y el WebSocket para enterarse del resultado.

| Capacidad | Estado | Detalle |
|---|---|---|
| Ciclo de vida de `PaymentSession` | Implementado | Crear, procesar, aprobar, rechazar y cancelar |
| API HTTP y simulador QR | Implementado | Puerto 8081. Token por terminal en las operaciones; `/pay` y `/status` son públicos |
| Publicación de `TransactionReceived` | Implementado | Transactional Outbox |
| Consumo de resultados de autorización | Implementado | `posnet.auth.>` |
| Reversa y cierre de lote | Implementado | Publicación directa a NATS, sin outbox |
| Métricas, trazas y health checks | Implementado | Ver [§11](#11-observabilidad) |
| WebSocket de terminales | Implementado | `WS_PORT` (8082), mensajes JSON y token en el primer mensaje. Ver [§7b](#7b-websocket-de-terminales) |
| Notificaciones al terminal | Implementado | Sesión creada, resultado, vencimiento y comprobante, por el WebSocket |
| Terminales físicos con mTLS | **Pendiente** | El WebSocket autentica con el token del terminal |
| Parsing de ISO 8583 | **Pendiente** | El flujo QR envía `iso8583_raw` vacío |
| Entrega de comprobantes (`SendReceipt`) | Implementado | Por el WebSocket. Sin conexión, responde `delivered=false` |
| Expiración con estado `EXPIRED` | Implementado | Ver [§5.6](#56-expiración) |

Las consecuencias de estos pendientes están en [§13](#13-brechas-conocidas).

## 2. Contexto y límites

```
 Frontend QR (cajero / celular)            Terminal físico (futuro: mTLS e ISO 8583)
              │ HTTP :8081, WebSocket :8082            ┆ no implementado
              ▼                                       ┆
 ┌────────────────────────────────────────────────────┴──┐
 │                   Terminal Gateway                    │◄── gRPC :9091 ── Notification
 │  schema Postgres: terminal_gateway                    │    (SendReceipt)
 └───────┬───────────────────────────────────▲───────────┘
         │ posnet.transaction.*              │ posnet.auth.*
         ▼                                   │
 Authorization / Settlement  ──────  NATS JetStream
```

- **Es dueño de:** las sesiones de pago, el registro de terminales y el estado que ve el cajero.
- **No decide:** si un pago se aprueba (Authorization), el score de fraude (Fraud Detection) ni la
  liquidación (Settlement). Tampoco arma los comprobantes (Notification).
- **Identificador de correlación:** el ID de la sesión es el `TransactionID` de toda la Saga.

## 3. Estructura del código

El contexto sigue arquitectura hexagonal con CQRS liviano:

| Ruta | Contenido |
|---|---|
| [`cmd/terminal-gateway/tg_main.go`](../../cmd/terminal-gateway/tg_main.go) | Arranque y apagado ordenado |
| [`cmd/terminal-gateway/tg_wire.go`](../../cmd/terminal-gateway/tg_wire.go) | Wiring, relay de outbox y job de limpieza |
| [`domain/aggregate/`](../../context/terminal-gateway/domain/aggregate/) | `PaymentSession` (aggregate root) |
| [`domain/entity/`](../../context/terminal-gateway/domain/entity/) | `Terminal` |
| [`domain/valueobject/`](../../context/terminal-gateway/domain/valueobject/) | `SessionState`, `PaymentChannel` |
| [`domain/event/`](../../context/terminal-gateway/domain/event/) | Eventos de dominio internos |
| [`domain/repository/`](../../context/terminal-gateway/domain/repository/), [`domain/service/`](../../context/terminal-gateway/domain/service/) | Puertos de salida |
| [`application/port/`](../../context/terminal-gateway/application/port/) | Puertos de entrada y comandos |
| [`application/command/`](../../context/terminal-gateway/application/command/) | `SessionHandler` y métricas de negocio |
| [`application/query/`](../../context/terminal-gateway/application/query/) | `SessionQueryHandler` (solo lectura) |
| [`infrastructure/http/`](../../context/terminal-gateway/infrastructure/http/) | Router, API de operaciones y simulador QR |
| [`infrastructure/grpc/server/`](../../context/terminal-gateway/infrastructure/grpc/server/) | Servidor gRPC |
| [`infrastructure/nats/`](../../context/terminal-gateway/infrastructure/nats/) | Publisher y subscriber |
| [`infrastructure/postgres/`](../../context/terminal-gateway/infrastructure/postgres/) | Repositorios de sesiones y terminales |
| [`infrastructure/websocket/`](../../context/terminal-gateway/infrastructure/websocket/) | Servidor WebSocket y registro de conexiones (`Hub`). `MockNotifier` queda para pruebas |

Las rutas de `domain/`, `application/` e `infrastructure/` son relativas a `context/terminal-gateway/`.

## 4. Modelo de dominio

### 4.1 `PaymentSession`

Representa un cobro, desde que el cajero lo inicia hasta que hay un resultado.

| Campo | Descripción |
|---|---|
| `id` | `TransactionID` (UUID), generado al crear la sesión |
| `terminalID`, `merchantID` | Terminal y comercio |
| `amount` | Monto en centavos y moneda; debe ser positivo |
| `stan` | System Trace Audit Number, entre 1 y 999999 |
| `channel` | Canal de pago |
| `state` | Estado actual |
| `expiresAt` | Creación + 5 minutos (constante `defaultSessionTTL`) |
| `authCode` | Código de autorización, si se aprobó |
| `rejectionCode`, `rejectionReason` | Motivo del rechazo, si se rechazó |
| `createdAt`, `closedAt` | `closedAt` se completa al llegar a un estado final |

### 4.2 Máquina de estados

```
                   StartProcessing                Approve
AWAITING_PAYMENT ───────────────────► PROCESSING ─────────► APPROVED
    │       │                          │  │     Reject
    │       │ Cancel                   │  └───────────────► REJECTED
    │       └──────► CANCELLED         │
    │ Expire                           │ (sin uso)
    └──────► EXPIRED                   ▼
                                  RECONNECTING ──► PROCESSING | APPROVED | REJECTED
```

`APPROVED`, `REJECTED`, `EXPIRED` y `CANCELLED` son finales. Una sesión nace en `AWAITING_PAYMENT` y
`StartProcessing` falla si el TTL ya venció.

Partes definidas en el dominio que ningún flujo utiliza hoy:

- `IDLE` y `RECONNECTING` nunca se asignan.
- `Expire()` no se invoca: el job pasa las sesiones a `EXPIRED` con un solo `UPDATE`, sin cargarlas (ver
  [§5.6](#56-expiración)).
- La sesión registra eventos internos (`session.created`, `transaction.initiated`, etc.), pero nadie
  los lee. Lo que se publica a NATS se construye aparte en el publisher.

### 4.3 `Terminal`

| Campo | Descripción |
|---|---|
| `id`, `merchantID` | Identidad y comercio |
| `terminalCode` | Código físico único, como `TRM-0001` |
| `certificateCN` | CN del certificado mTLS; único, sin uso hasta que exista el WebSocket |
| `status` | `ACTIVE`, `BLOCKED` o `MAINTENANCE` |

Solo un terminal `ACTIVE` puede crear sesiones. No hay casos de uso para registrar o bloquear
terminales: se administran directamente en la base de datos.

### 4.4 Canales de pago

| Canal | `entry_mode` publicado |
|---|---|
| `QR`, `NFC`, `APPLE_PAY`, `GOOGLE_PAY` | `CONTACTLESS` |
| `MAGSTRIPE` | `MAGSTRIPE` |

El simulador HTTP solo crea sesiones `QR`. ISO 8583 no tiene un modo de entrada propio para QR, por
eso se mapea a `CONTACTLESS`.

## 5. Flujos

### 5.1 Pago QR

```
Cajero/Celular        Terminal Gateway             NATS            Authorization
    │ POST /api/sessions/create │                    │                    │
    ├──────────────────────────►│ valida terminal    │                    │
    │◄── 201 + qr_content ──────┤ sesión AWAITING_PAYMENT                 │
    │                           │                    │                    │
    │ POST /api/sessions/{id}/pay                    │                    │
    ├──────────────────────────►│ TX: sesión PROCESSING + fila en outbox  │
    │◄── 202 processing ────────┤                    │                    │
    │                           │ Relay (cada 500 ms)│                    │
    │                           ├─ transaction.received ─────────────────►│
    │                           │                    │        Saga        │
    │                           │◄──────── auth.approved / auth.rejected ─┤
    │                           │ sesión APPROVED / REJECTED              │
    │ GET /api/sessions/{id}/status (polling)        │                    │
    ├──────────────────────────►│                    │                    │
    │◄── state, auth_code ──────┤                    │                    │
```

El `202` indica que el pago se aceptó para procesar, no que se aprobó. El cliente conoce el resultado
por polling, porque todavía no hay canal push.

### 5.2 Rechazo con retención de tarjeta

Si el evento de rechazo trae `capture_card=true`, TG registra un log `WARN` de auditoría antes de
notificar. Los códigos que exigen retención son `04`, `41`, `43` y `CARD_BLOCKED`
([`pkg/domain/card_capture.go`](../../pkg/domain/card_capture.go)).

No se guarda en una columna propia: `RequiresCardCapture()` lo deriva del `rejection_code`. Las respuestas de
`GET /api/sessions/{id}/status` y `GET /sessions/{id}` lo incluyen en `capture_card` (`CaptureCard`), y el cajero de
`posnet-front` muestra **RETENER TARJETA**. También viaja en el mensaje `session.result` del
[WebSocket](#7b-websocket-de-terminales).

### 5.3 Cancelación

`POST /sessions/{id}/cancel` exige que el terminal del token coincida con el de la sesión (sin tokens, el de
`X-Terminal-ID`). Solo se
puede cancelar en `AWAITING_PAYMENT`; una sesión en `PROCESSING` ya no es cancelable.

### 5.4 Reversa

`POST /sessions/{id}/reversal` verifica el terminal y publica `reversal-requested`, tomando monto y
código de autorización de la sesión original. La sesión no cambia de estado, porque no existe un
estado `REVERSED`. TG ignora el evento `reversal-completed`: lo confirma con `Ack` y deja un `WARN`.

### 5.5 Cierre de lote

`POST /batch-close` publica `batch-close` con los totales informados por el terminal; Settlement hace
la conciliación. TG no persiste nada en este flujo. `currency` es obligatorio y `batch_date`, opcional: sin
él, Settlement usa el día de negocio del pedido. Una moneda o una fecha inválidas se rechazan, hoy con `500`.

### 5.6 Expiración

Cada `SESSION_CLEANUP_EVERY` (1 minuto), el job pasa a `EXPIRED` las sesiones vencidas en `AWAITING_PAYMENT`, con un solo
`UPDATE` que también fija `closed_at`, y le avisa a cada terminal con el mensaje `session.expired` del WebSocket. Si el
terminal no está conectado, el estado ya quedó guardado y lo ve al consultar.

**Una sesión en `PROCESSING` no vence:** el pago ya salió, y su resultado llega de Authorization aunque haya pasado el TTL.
Antes se borraba, y una aprobación tardía no encontraba la sesión.

| Momento | `GET .../status` | `POST .../pay` |
|---|---|---|
| Antes de vencer | Estado real | `202` |
| Vencida, antes de que corra el job | Estado anterior, `ttl_seconds: 0` | `500` |
| Después de que corrió el job | `EXPIRED` | `500` |

Las filas ya no se borran: quedan con su estado final, como las aprobadas y las rechazadas. Con varias instancias, el `UPDATE`
usa `FOR UPDATE SKIP LOCKED`, así que cada sesión la vence y la notifica una sola.

## 6. API HTTP

Puerto `HTTP_PORT` (8081 por defecto). Los errores tienen la forma `{"error": "CODIGO", "message": "..."}`. Desde afuera,
se accede por el [API Gateway](GATEWAY.md), en el puerto 8000, con los mismos paths. Para que el límite de intentos vea la
IP del cliente, el gateway tiene que estar en `TRUSTED_PROXIES`.

### Autenticación de terminales

Las operaciones de un terminal exigen `Authorization: Bearer <token>`. El terminal sale del token, no de lo que envía el
cliente:

| Endpoint | Sin token o inválido | Con el token de otro terminal |
|---|---|---|
| `POST /api/sessions/create` | `401` | `403` si `terminal_id` es otro. Sin `merchant_id`, usa el del terminal; con otro, `400` |
| `GET /sessions/{id}` | `401` | `404`, como si no existiera |
| `POST /sessions/{id}/cancel`, `POST /sessions/{id}/reversal` | `401` | Se ignora `X-Terminal-ID`. Con una sesión ajena, el caso de uso la rechaza |
| `POST /batch-close` | `401` | `403` si `terminal_id` es otro. Sin `merchant_id`, usa el del terminal; con otro, `400` |

`/api/sessions/{id}/pay` y `/api/sessions/{id}/status` no usan el token del terminal: los usa el teléfono del cliente. Usan
el [token de la sesión](#token-de-la-sesión). `/healthz`, `/readyz` y `/metrics` son públicos.

Los tokens están en el archivo de `TERMINAL_TOKENS_FILE`, que **solo guarda su SHA-256**. Un terminal puede tener varios,
para rotarlos sin cortar el servicio:

```json
[
  { "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "sha256": "<sha256 del token, en hex>" },
  { "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "sha256": "<otro>", "expires_at": "2026-12-31T23:59:59Z" }
]
```

```bash
TOKEN=$(openssl rand -hex 32)                       # entregarlo al terminal
printf %s "$TOKEN" | sha256sum | cut -d' ' -f1      # va en el archivo
```

Un archivo inválido o inexistente impide arrancar. Un archivo vacío (`[]`) deja todo cerrado. **Sin `TERMINAL_TOKENS_FILE`,
la API queda abierta como antes, y el servicio solo arranca con `ENVIRONMENT=development`:** usa `X-Terminal-ID` y el
terminal de prueba de `01-init.sql`. Así funciona hoy el Docker Compose.

El servicio relee el archivo cada 30 s: un token revocado deja de valer en ese plazo, sin reiniciar. Si el archivo queda
inválido, se conservan los tokens vigentes y queda un `ERROR` en el log.

**Límite de intentos:** con 10 tokens inválidos en un minuto desde la misma IP (en IPv6, la misma `/64`), esa IP recibe
`429` durante 5 minutos, aun con un token válido. Un pedido sin token no cuenta, y un pedido exitoso desde esa IP borra los
fallos: un terminal mal configurado no bloquea al resto de su local mientras alguno se autentique. Vive en memoria, por
instancia. Detrás de un proxy, hay que declararlo en `TRUSTED_PROXIES`: si no, todos los pedidos comparten su IP. Sin esa
variable, `X-Forwarded-For` se ignora, porque cualquiera podría declarar otra IP.

### Token de la sesión

`create` devuelve `pay_token`, el token de esa sesión, y lo agrega al fragmento de `qr_content`
(`/pay/{id}#t=<token>`). El navegador no envía el fragmento al servidor web, así que el token no queda en sus logs ni en el
`Referer`. `/pay` y `/status` lo reciben en `X-Session-Token`: así, un ID de sesión solo ya no alcanza para pagarla ni para ver
su estado.

| `X-Session-Token` | `PAY_TOKEN_REQUIRED=true` | `PAY_TOKEN_REQUIRED=false` |
|---|---|---|
| Válido | Sigue | Sigue |
| Ausente | `401 SESSION_TOKEN_REQUIRED` | Sigue, para los clientes que todavía no lo envían |
| De otra sesión o inválido | `401 INVALID_SESSION_TOKEN` | `401 INVALID_SESSION_TOKEN` |

El token es un HMAC-SHA256 del ID con `PAY_TOKEN_KEY`: no se guarda, se recalcula para verificarlo, y vale mientras exista la
sesión. Varias instancias tienen que compartir la clave. Para rotarla sin invalidar los QR ya emitidos, la anterior va en
`PAY_TOKEN_KEY_PREVIOUS` hasta que venzan sus sesiones (5 minutos).

**Fuera de `ENVIRONMENT=development`, el token se exige por defecto, y el servicio no arranca sin `PAY_TOKEN_KEY`.** En
desarrollo no se exige por defecto, para probar localmente con clientes viejos, y sin clave se usa una aleatoria: un QR deja
de valer si el servicio se reinicia.
`posnet-frontend`, la colección de Postman y Locust ya lo envían, así que en Docker Compose se puede activar con
`PAY_TOKEN_REQUIRED=true`.

```bash
openssl rand -hex 32   # una clave para PAY_TOKEN_KEY
```

### TLS

Con `HTTP_TLS_CERT` y `HTTP_TLS_KEY`, el puerto `HTTP_PORT` sirve solo HTTPS, desde TLS 1.2. Un certificado nuevo requiere
reiniciar. Si TLS lo termina un proxy, hay que declararlo con `HTTP_TLS_EXTERNAL=true` y limitar el acceso directo al
puerto. **Fuera de `ENVIRONMENT=development`, el servicio no arranca sin una de las dos opciones**, porque los tokens
viajarían sin cifrar. En desarrollo arranca sin TLS, también con tokens, y lo advierte en el log: es intencional, para
probar localmente. Docker Compose no usa TLS.

Con HTTPS hay que cambiar también lo que consulta ese puerto:

| Cliente | Cambio |
|---|---|
| `healthcheck` de Docker Compose | `wget --no-check-certificate -q --spider https://localhost:8081/healthz` |
| Prometheus | `scheme: https` en `prometheus.yml`, con la CA del certificado |
| `posnet-frontend`, Postman y Locust | `https://` en la URL de Terminal Gateway |

| Método y ruta | Uso | Éxito | Errores |
|---|---|---|---|
| `GET /healthz` | Liveness | `200` | — |
| `GET /readyz` | Readiness; solo verifica Postgres | `200` | `503` |
| `GET /metrics` | Métricas Prometheus | `200` | — |
| `POST /api/sessions/create` | Crear sesión QR | `201` | `400`, `500` |
| `POST /api/sessions/{id}/pay` | Simular el pago | `202` | `400`, `401`, `404`, `500` |
| `GET /api/sessions/{id}/status` | Consultar estado (polling) | `200` | `400`, `401`, `404`, `500` |
| `GET /sessions/{id}` | Consulta para soporte | `200` | `400`, `404`, `500` |
| `POST /sessions/{id}/cancel` | Cancelar | `200` | `500` |
| `POST /sessions/{id}/reversal` | Solicitar reversa | `202` | `500` |
| `POST /batch-close` | Solicitar cierre de lote | `202` | `400`, `500` |

En `cancel` y `reversal`, cualquier fallo del caso de uso devuelve `500`, incluso un UUID inválido, un terminal incorrecto
o una transición no permitida. En `create` y `batch-close`, un error de validación es un `400` con su motivo, y el resto, un
`500`. En `pay`, igual, salvo sus validaciones previas y el `404`.

### Crear sesión

```http
POST /api/sessions/create
Content-Type: application/json

{
  "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "amount_cents": 150000,
  "currency": "ARS"
}
```

Solo `amount_cents` es obligatorio. Si se omiten, `terminal_id` y `merchant_id` toman los valores del
ejemplo (el terminal de prueba) y `currency` toma `ARS`. El STAN se genera en el servidor.

```json
{
  "transaction_id": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "expires_at": "2026-11-05T15:05:00Z",
  "ttl_seconds": 299,
  "amount_cents": 150000,
  "currency": "ARS",
  "pay_token": "Xq3...",
  "qr_content": "http://192.168.0.2:5173/pay/7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10#t=Xq3..."
}
```

`qr_content` usa `POSNET_HOST`; si no está definida, usa la IP local detectada. El puerto del
frontend (5173) está fijo en el código.

### Simular pago

```json
{ "card_last4": "4242", "card_network": "VISA", "card_token": "<64 caracteres hex>" }
```

Ambos campos son opcionales; sus valores por defecto son `0000` y `VISA`. También se acepta
`entry_mode`, pero se ignora.

| Error | Cuándo |
|---|---|
| `400 INVALID_CARD_LAST4` | `card_last4` no tiene exactamente 4 dígitos |
| `400 INVALID_CARD_NETWORK` | `card_network` no es `VISA`, `MASTERCARD`, `AMEX`, `CABAL`, `NARANJA` ni `UNKNOWN` |
| `400 INVALID_CARD_TOKEN` | `card_token` no tiene un formato válido |

Con estos errores, el pago no empieza y la sesión sigue disponible para reintentar. Se validan acá porque el evento queda
en NATS sin vencimiento: un PAN enviado por error en estos campos no debe llegar a publicarse. Respuesta:

```json
{ "status": "processing", "transaction_id": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10" }
```

### Consultar estado

`GET /api/sessions/{id}/status`:

```json
{
  "transaction_id": "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10",
  "state": "APPROVED",
  "amount_cents": 150000,
  "currency": "ARS",
  "auth_code": "A1B2C3",
  "rejection_code": "",
  "rejection_reason": "",
  "capture_card": false,
  "ttl_seconds": 281,
  "expires_at": "2026-11-05T15:05:00Z"
}
```

`GET /sessions/{id}` devuelve además terminal, comercio, canal y fecha de creación. Atención: su
respuesta usa nombres en PascalCase (`TransactionID`, `State`...), porque el struct no tiene tags JSON.

### Cierre de lote

```json
{
  "terminal_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "merchant_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "terminal_count": 42,
  "terminal_amount": 1250000,
  "currency": "ARS"
}
```

CORS solo permite los orígenes de `CORS_ALLOWED_ORIGINS`, con los métodos `GET`, `POST` y `OPTIONS` y los headers
`Content-Type`, `Authorization`, `X-Terminal-ID` y `X-Session-Token`. Por defecto son los del frontend de pago: `http://localhost:5173` y el host de
`qr_content` en el puerto 5173. CORS solo frena a un navegador: no reemplaza a la autenticación.

Un `500` responde `{"error": "INTERNAL", "message": "internal error"}`, y el detalle queda en el log.

## 7. API gRPC

Contrato: [`terminal_gateway.proto`](../../pkg/proto/terminalgateway/v1/terminal_gateway.proto). El
puerto es 9091. Su único consumidor en producción es Notification, a través de su propio
cliente; el de `infrastructure/grpc/client` no se importa fuera de sus tests.

### mTLS

Con `GRPC_TLS_CERT` y `GRPC_TLS_KEY`, el puerto sirve solo TLS, desde 1.2. Con `GRPC_TLS_CLIENT_CA`, además exige que el
cliente presente un certificado firmado por esa CA y a nombre de `GRPC_TLS_CLIENT_NAMES` (`notification` por defecto), en su
CN o en un SAN DNS: un certificado válido de la misma CA pero de otro servicio también se rechaza. Sin él, el handshake falla
y la llamada termina en `Unavailable`. **Fuera de `ENVIRONMENT=development`, el servicio no arranca sin los tres primeros.**
Docker Compose no usa TLS.

El certificado del servidor tiene que incluir el nombre con el que lo llama Notification (por ejemplo, `terminal-gateway`), o
Notification tiene que declararlo con `GRPC_CLIENT_TLS_SERVER_NAME`. Del lado de Notification se configuran
`GRPC_CLIENT_TLS_CA`, `GRPC_CLIENT_TLS_CERT` y `GRPC_CLIENT_TLS_KEY` (ver
[Notification](NOTIFICATION.md#7-grpc)). Un certificado nuevo requiere reiniciar los dos servicios.

```bash
# Ejemplo con openssl: una CA, el certificado de Terminal Gateway y el de Notification
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 \
  -subj /CN=posnet-ca -keyout ca.key -out ca.pem
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=terminal-gateway \
  -keyout tg.key -out tg.csr
openssl x509 -req -in tg.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -out tg.pem \
  -extfile <(printf 'subjectAltName=DNS:terminal-gateway\nextendedKeyUsage=serverAuth')
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=notification \
  -keyout nt.key -out nt.csr
openssl x509 -req -in nt.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -out nt.pem \
  -extfile <(printf 'extendedKeyUsage=clientAuth')
```

| RPC | Comportamiento actual |
|---|---|
| `SendReceipt` | Valida el request, busca la sesión y envía el comprobante por el WebSocket al **terminal de la sesión**, no al que declara el pedido. Responde `delivered=false` con `SESSION_NOT_FOUND` o, si el terminal no está conectado, `TERMINAL_NOT_CONNECTED` |
| `GetTerminalStatus` | `CONNECTED` si el terminal tiene el WebSocket abierto, con `connected_since` de la conexión más antigua, y `DISCONNECTED` si no. El comercio sale de su sesión activa, si tiene una |

`delivered=true` significa que el comprobante quedó en la cola de la conexión: si el terminal se desconecta justo después,
puede no recibirlo.

## 7b. WebSocket de terminales

Terminal Gateway escucha en `WS_PORT` (8082), en `GET /ws`, con el mismo TLS que la API HTTP. Desde afuera se llega por el
[API Gateway](GATEWAY.md), en `ws://<host>:8000/ws`. Un navegador solo se conecta desde los orígenes de
`CORS_ALLOWED_ORIGINS`; desde otro, el handshake responde `403`.

**Autenticación.** El primer mensaje del terminal, dentro de 10 s, tiene que ser:

| Modo | Mensaje |
|---|---|
| Con `TERMINAL_TOKENS_FILE` | `{"type":"auth","token":"<token del terminal>"}` |
| Sin tokens, solo en desarrollo | `{"type":"auth","terminal_id":"<UUID>"}` |

Se usa el primer mensaje y no un encabezado porque un navegador no puede enviar `Authorization` al abrir un WebSocket. El
token pasa por el mismo límite de intentos que la API HTTP. Si es válido, el servidor responde
`{"type":"auth.ok","terminal_id":"..."}`; si no, `{"type":"error","code":"..."}` y cierra con el código 1008:

| `code` | Cuándo |
|---|---|
| `AUTH_REQUIRED` | El primer mensaje no es `auth` |
| `UNAUTHORIZED` | Token ausente o inválido |
| `TOO_MANY_ATTEMPTS` | Se superó el límite de intentos de esa IP |
| `INVALID_TERMINAL_ID` | Sin tokens, `terminal_id` no es un UUID |

**Mensajes del servidor.** Después de autenticarse, el terminal solo recibe:

| `type` | Cuándo | Campos |
|---|---|---|
| `session.created` | Se creó una sesión del terminal | `transaction_id`, `amount_cents`, `currency`, `channel`, `expires_at` |
| `session.result` | La sesión terminó aprobada o rechazada | `transaction_id`, `state`, `auth_code`, `rejection_code`, `rejection_reason`, `capture_card` |
| `session.expired` | La sesión venció sin pagarse | `transaction_id` |
| `receipt` | Notification envió el comprobante | Los del comprobante: `transaction_id`, `result`, `amount_cents`, `currency`, `auth_code`, etc. |

**Conexión.** Un terminal puede tener varias conexiones, por ejemplo dos pestañas del cajero: todas reciben lo mismo. El
servidor envía un ping cada 30 s y desconecta al terminal que no responde, o al que no lee sus mensajes y llena su cola (32).
Al apagar, cierra todas las conexiones con el código 1001.

Errores: `InvalidArgument` por datos inválidos e `Internal` por fallos de base de datos.

## 8. Eventos NATS

Todos los eventos usan el envelope `DomainEvent` de
[`pkg/events/envelope.go`](../../pkg/events/envelope.go). En los que publica TG, el `correlation_id`
es el `TransactionID`, salvo en el cierre de lote, donde es el `TerminalID`.

### Publicados

| Subject | Stream | Consumidor | Cómo se publica |
|---|---|---|---|
| `posnet.transaction.received.v1` | `POSNET_TRANSACTIONS` | Authorization | Outbox |
| `posnet.transaction.reversal-requested.v1` | `POSNET_TRANSACTIONS` | Authorization | Directo |
| `posnet.transaction.batch-close.v1` | `POSNET_TRANSACTIONS` | Settlement | Directo |

Payload de `transaction.received`:

| Campo | Notas |
|---|---|
| `transaction_id`, `terminal_id`, `merchant_id` | UUID |
| `amount_cents`, `currency` | Centavos e ISO 4217 |
| `stan`, `entry_mode` | `entry_mode` se deriva del canal |
| `card_last4`, `card_network` | Vienen del request de pago, ya validados: con un valor inválido, `/pay` responde `400` |
| `card_token` | Identifica la tarjeta, sin ser el PAN: lo usan la blocklist y Authorization para pedir sus datos. Viene del request de pago; vacío si no llegó. Con un formato inválido, `/pay` responde `400` |
| `emv_data_b64`, `iso8583_raw` | Hoy siempre están vacíos |
| `received_at` | RFC3339 UTC |

### Consumidos

Durable y queue group `gateway-auth-consumer`, sobre el stream `POSNET_AUTH`:

| Parámetro | Valor |
|---|---|
| Filtro | `posnet.auth.>` |
| Entregas máximas | 3 |
| `AckWait` | 30 s |
| Política de entrega | `DeliverNew` |

| Evento | Acción |
|---|---|
| `posnet.auth.approved.v1` | `ApplyApproval` |
| `posnet.auth.rejected.v1` | `ApplyRejection` |
| Otros, incluido `reversal-completed` | `WARN` y `Ack` |

Tratamiento de errores:

| Situación | Acción | Efecto |
|---|---|---|
| Envelope inválido | `Term` | No se reintenta |
| `ValidationError` o `ConflictError` | `Term` | No se reintenta |
| Cualquier otro error | `Nak` | Se reintenta hasta 3 entregas |

Un mensaje que agota las entregas no pasa a una DLQ: ningún código publica en `POSNET_DLQ`. Para
recuperarlo, seguir el runbook
[nats-dlq-reprocess](../runbooks/nats-dlq-reprocess.md).

## 9. Persistencia

Schema `terminal_gateway`. **La fuente real es
[`01-init.sql`](../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql):**
`pgutil.Migrate` no hace nada, `migrations/terminal-gateway/` está vacío y
[`tg_schema.sql`](../../context/terminal-gateway/infrastructure/postgres/tg_schema.sql) no se ejecuta.

| Tabla | Propósito | Claves e índices |
|---|---|---|
| `terminals` | Terminales registrados | PK `id`; `terminal_code` y `certificate_cn` únicos |
| `payment_sessions` | Sesiones de pago | PK `id`; FK `terminal_id` → `terminals` |
| `processed_events` | Idempotencia de eventos | PK `event_id` |
| `outbox` | Eventos pendientes de publicar | PK `id`; índice en `created_at` |

Diferencia entre ambos archivos: `tg_schema.sql` define dos índices parciales sobre `payment_sessions`
(`idx_tg_sessions_terminal_active` e `idx_tg_sessions_expires_at`) que `01-init.sql` no crea. Por eso,
en un entorno levantado con `01-init.sql`, la limpieza, el gauge y la búsqueda de la sesión activa
recorren una tabla sin esos índices.

El init también carga el terminal de prueba: `a1b2c3d4-e5f6-7890-abcd-ef1234567890` (`TRM-0001`).

## 10. Garantías de entrega

### Transactional Outbox

Solo `ProcessPayment` usa este patrón ([`pkg/outbox`](../../pkg/outbox/outbox.go)):

1. En una misma transacción se guarda la sesión en `PROCESSING` y se inserta el evento en `outbox`.
2. El Relay revisa la tabla cada 500 ms y toma hasta 50 filas con `FOR UPDATE SKIP LOCKED`.
3. Publica cada fila con `Nats-Msg-Id = event_id`, la borra y confirma la transacción.

La garantía es *at-least-once*: si el commit falla después de publicar, el reintento queda
deduplicado por JetStream dentro de su ventana de duplicados. Los streams no configuran esa ventana,
así que se aplica el valor por defecto del servidor.

Reversa y cierre de lote publican directamente. Si NATS falla, el cliente recibe `500` y debe
reintentar. Como cada intento genera un `event_id` nuevo, JetStream no deduplica esos reintentos.

### Idempotencia del consumidor

`ApplyApproval` y `ApplyRejection` registran el `event_id` en `processed_events` y notifican solo si
la inserción fue efectiva. Esta protección tiene dos límites (ver [§13](#13-brechas-conocidas)): la
transición de estado se valida antes, y el guardado de la sesión queda fuera de la transacción.

### Apagado

Ante `SIGINT` o `SIGTERM`, el orden es:

1. Cancelar el contexto raíz, lo que detiene el Relay y el job de limpieza.
2. Detener gRPC con `GracefulStop` y HTTP con `Shutdown` (hasta 10 s).
3. Esperar a que terminen los jobs de fondo.
4. Cerrar NATS con `Drain` y luego el pool de Postgres.

## 11. Observabilidad

### Métricas de negocio

| Métrica | Tipo | Significado |
|---|---|---|
| `posnet_sessions_created_total` | Counter | Sesiones creadas |
| `posnet_sessions_approved_total` | Counter | Sesiones aprobadas |
| `posnet_sessions_expired_total` | Counter | Sesiones que el job pasó a `EXPIRED` |
| `posnet_qr_e2e_duration_seconds` | Histogram | Tiempo de creación a aprobación; incluye la espera del pago |
| `posnet_sessions_processing_current` | Gauge | Sesiones en `PROCESSING`; hace un `COUNT` en cada scrape |

También se exponen métricas del pool de Postgres y de mensajes NATS
(`posnet_nats_messages_processed_total` y `posnet_nats_messages_failed_total`).

### Alertas y dashboards

- **Alerta:** `SesionesBloqueadasProcessing` (crítica), cuando el gauge supera 10 durante 2 minutos.
  Ver [`critical.yml`](../../deployments/docker/prometheus/rules/critical.yml).
- **Dashboards de Grafana:** `posnet-saga-overview` (gauge) y `posnet-bc-detail` (latencia E2E).
- **Runbook:** [incident-response](../runbooks/incident-response.md).

### Trazas y logs

Cada comando y consulta abre un span (`command.ProcessPayment`, `query.GetSessionStatus`,
`http.SimulatePay`, `subscriber.handleAuthResult`, etc.). Para seguir un pago en los logs, buscar por
`transaction_id`.

## 12. Configuración y ejecución

### Variables de entorno

| Variable | Default | ¿Se aplica? |
|---|---|---|
| `POSTGRES_DSN` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `NATS_URL` | Obligatoria | Sí; sin ella, el proceso termina con `panic` |
| `HTTP_PORT` | `8081` | Sí |
| `POSTGRES_MAX_CONNS`, `POSTGRES_MIN_CONNS` | `20`, `2` | Sí |
| `POSTGRES_MAX_CONN_LIFETIME`, `POSTGRES_MAX_CONN_IDLE` | `30m`, `5m` | Sí |
| `NATS_NKEY_PATH`, `NATS_TLS_CERT`, `NATS_TLS_KEY`, `NATS_TLS_CA` | Vacías | Sí |
| `OTEL_SERVICE_NAME` | `posnet-terminal-gateway` | Sí |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Sí |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Sí |
| `ENVIRONMENT` | `development` | Sí |
| `POSNET_HOST` | IP local detectada | Sí; define el host de `qr_content` y del origen CORS por defecto |
| `TERMINAL_TOKENS_FILE` | Vacía | Sí; [tokens de los terminales](#autenticación-de-terminales). Vacía deja la API abierta, y solo se permite con `ENVIRONMENT=development`. Si el archivo no existe o es inválido, el servicio no arranca |
| `HTTP_TLS_CERT`, `HTTP_TLS_KEY` | Vacías | Sí; habilitan [HTTPS](#tls). Van juntas. Si no se pueden cargar, el servicio no arranca |
| `HTTP_TLS_EXTERNAL` | `false` | Sí; declara que un proxy termina TLS. Fuera de desarrollo, hace falta esto o `HTTP_TLS_CERT` |
| `TRUSTED_PROXIES` | Vacía | Sí; CIDR separados por comas cuyo `X-Forwarded-For` se acepta para el límite de intentos. Uno inválido impide arrancar |
| `GRPC_TLS_CERT`, `GRPC_TLS_KEY` | Vacías | Sí; habilitan [TLS en gRPC](#mtls). Van juntas. Si no se pueden cargar, el servicio no arranca |
| `GRPC_TLS_CLIENT_CA` | Vacía | Sí; CA de los clientes: exige mTLS. Fuera de desarrollo, hace falta junto con las dos anteriores |
| `GRPC_TLS_CLIENT_NAMES` | `notification` | Sí; clientes aceptados en gRPC, separados por comas, por CN o SAN DNS de su certificado |
| `PAY_TOKEN_KEY` | Vacía | Sí; firma el [token de la sesión](#token-de-la-sesión). Al menos 32 caracteres. Fuera de desarrollo es obligatoria; en desarrollo, sin ella, se usa una clave aleatoria |
| `PAY_TOKEN_KEY_PREVIOUS` | Vacía | Sí; clave anterior, que sigue verificando durante una rotación |
| `PAY_TOKEN_REQUIRED` | `true`, salvo en desarrollo | Sí; `/pay` y `/status` exigen el token. Un valor distinto de `true` o `false` impide arrancar |
| `CORS_ALLOWED_ORIGINS` | Frontend de pago | Sí; orígenes separados por comas, como `https://pagos.example.com`. `*` permite cualquiera |
| `GRPC_PORT` | `9091` | **No**: el puerto está fijo en 9091 |
| `WS_PORT` | `8082` | Sí; puerto del [WebSocket de terminales](#7b-websocket-de-terminales) |
| `TLS_CERT_PATH`, `TLS_KEY_PATH`, `TLS_CA_PATH` | Vacías | **No**: nada usa esta configuración |
| `SESSION_TTL_SECONDS` | `300` | **No**: el TTL está fijo en 5 minutos |
| `SESSION_CLEANUP_EVERY` | `1m` | Sí; cada cuánto vencen las sesiones sin pagar. Si no es positivo, el servicio no arranca |
| `MIGRATIONS_DIR` | `migrations/terminal-gateway` | **No**: `Migrate` no hace nada |

### Levantar el servicio

Con Docker Compose, que crea el schema y el terminal de prueba:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --build terminal-gateway
```

`docker-compose.yml` fija los puertos del servicio, por encima de los genéricos de `.env.docker`.
También publica el 8082, el del WebSocket. Sin Docker, con Postgres y NATS disponibles:

```bash
POSTGRES_DSN='postgres://...' NATS_URL='nats://localhost:4222' go run ./cmd/terminal-gateway
```

El comando `go run ./context/terminal-gateway/...` del README no es válido: el entrypoint está en
`cmd/`. Los templates de Helm de este servicio están vacíos, así que todavía no hay un despliegue en
Kubernetes.

### Probar un pago

Necesita también Authorization y Fraud Detection, para que la Saga responda:

```bash
ID=$(curl -s localhost:8081/api/sessions/create \
  -H 'Content-Type: application/json' \
  -d '{"amount_cents":150000}' | jq -r .transaction_id)

curl -s -X POST localhost:8081/api/sessions/$ID/pay \
  -H 'Content-Type: application/json' \
  -d '{"card_last4":"4242","card_network":"VISA"}'

curl -s localhost:8081/api/sessions/$ID/status | jq
```

### Tests

```bash
go test ./context/terminal-gateway/...

# Carga: flujo QR completo, requiere la Saga activa
locust -f posnet-load-tests/terminal-gateway/terminal_gateway_locust.py \
  --headless -u 10 -r 2 --run-time 3m --host http://localhost:8081
```

Son tests unitarios con mocks; no cubren la integración contra Postgres y NATS reales. Para la carga,
ver también [`LOCUST.md`](../../LOCUST.md).

## 13. Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Seguridad: resolver antes de exponer el servicio

| Brecha | Consecuencia | Dónde |
|---|---|---|
| El límite de intentos es por instancia | Con varias instancias detrás de un balanceador, un atacante tiene 10 intentos por minuto en cada una. Compartirlo requiere un almacenamiento común, como Redis | `tg_http_auth.go` |

### Pagos y consistencia

| Brecha | Consecuencia | Dónde |
|---|---|---|
| La reversa no verifica el estado | Se puede pedir la reversa de una sesión no aprobada, con `original_auth_code` vacío | `RequestReversal` |
| Un terminal puede tener varias sesiones activas | Nada impide la creación, aunque el diseño pide una sola; `GetActiveSession` devuelve una cualquiera | `CreateSession`, `FindActiveByTerminal` |
| El guardado de la sesión queda fuera de la transacción de idempotencia | `Save` usa el pool en lugar de `tx`: si falla el commit, la sesión queda guardada sin el evento marcado | `ApplyApproval`, `ApplyRejection` |
| La transición se valida antes que la idempotencia | Un evento duplicado falla con un error genérico y agota los reintentos en lugar de confirmarse | `ApplyApproval`, `ApplyRejection` |

### Funcionalidad incompleta

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Las conexiones WebSocket son por instancia | Con varias instancias, el resultado lo procesa la que consume el evento de NATS, que puede no ser la que tiene la conexión del terminal: el aviso no llega, y el terminal se entera al consultar. Hace falta repartir los avisos entre instancias | `hub.go` |
| Sin terminales físicos con mTLS | El WebSocket autentica con el token del terminal. `certificate_cn` y `TLS_*_PATH` siguen sin uso | `server.go` |
| Sin parsing de ISO 8583 | `iso8583_raw` y `emv_data_b64` viajan vacíos | `tg_qr_handler.go` |

### Operación

| Brecha | Consecuencia | Dónde |
|---|---|---|
| Si falla el listen de gRPC, el proceso sigue y el apagado se cuelga | Solo se registra el error; `GracefulStop` espera indefinidamente a `ready`. El comentario del código asume que `wire()` lo evita, pero no lo hace | `tg_grpc_server.go`, `tg_wire.go` |
| El Relay no propaga el trace context | La traza se corta entre TG y Authorization en `transaction.received` | `pkg/outbox/outbox.go` |
| Variables de configuración sin efecto | Cambiarlas no tiene ningún resultado (ver [§12](#12-configuración-y-ejecución)) | `tg_wire.go`, `tg_payment_session.go` |
| Sin migraciones reales e índices faltantes | Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `tg_schema.sql` difieren | `pkg/pgutil/migrate.go` |
| Errores de negocio devueltos como `500` | Falsean las métricas de error y dificultan el manejo en el cliente | `tg_http_handler.go`, `tg_qr_handler.go` |
| `/readyz` no verifica NATS | Una instancia sin NATS sigue recibiendo tráfico; los pagos quedan acumulados en el outbox | `tg_http_handler.go` |

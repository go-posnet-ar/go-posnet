# API Gateway — Documentación técnica

> Revisado el 2026-11-07. Este documento describe lo que el código hace hoy.

## 1. Resumen

El gateway es el único punto de entrada HTTP pensado para afuera: el teléfono del cliente, la pantalla del cajero, los
terminales y las herramientas de operación. No tiene dominio ni datos propios. Publica una lista cerrada de rutas de
Terminal Gateway y Settlement, con su path original, y aplica en un solo lugar CORS, TLS, el límite de pedidos de las
rutas públicas y la IP real del cliente.

| Capacidad | Estado | Detalle |
|---|---|---|
| Rutas publicadas | Implementado | Lista cerrada; el resto responde `404`. Ver [§3](#3-rutas) |
| CORS | Implementado | Lo responde solo el gateway; descarta el de los servicios |
| Límite de pedidos de `/pay` y `/status` | Implementado | Por IP, en memoria, por instancia |
| TLS | Implementado | Propio o de un proxy anterior. Obligatorio fuera de desarrollo |
| Autenticación | **No** | La hace cada servicio: el gateway reenvía `Authorization` |
| Token por sesión en `/pay` y `/status` | Implementado en Terminal Gateway | El gateway reenvía `X-Session-Token`; lo verifica [Terminal Gateway](TERMINAL-GATEWAY.md#token-de-la-sesión) |

## 2. Ubicación y límites

```
Teléfono / cajero / terminal / operación
                │  :8000
                ▼
        ┌───────────────┐   /api/sessions/..., /sessions/..., /batch-close   ┌──────────────────┐
        │  API Gateway  │ ─────────────────────────────────────────────────► │ Terminal Gateway │
        │               │   /batches/..., /merchants/..., /reversal-reviews  ├──────────────────┤
        │  :8001 admin  │ ─────────────────────────────────────────────────► │ Settlement       │
        └───────────────┘                                                    └──────────────────┘
```

- **No es un bounded context.** Vive en [`gateway/`](../../gateway/), fuera de `context/`, en el mismo módulo de Go.
- **No autentica:** cada servicio valida su token. El gateway solo reenvía `Authorization`.
- **No se publican** Authorization, Fraud Detection ni Notification: sus APIs HTTP no tienen autenticación. Tampoco
  `/metrics` ni `/readyz` de ningún servicio.

| Ruta | Contenido |
|---|---|
| [`gateway/cmd/gateway/`](../../gateway/cmd/gateway/) | Arranque, servidores y cadena de middlewares |
| [`gateway/internal/config/`](../../gateway/internal/config/) | Configuración y validación |
| [`gateway/internal/proxy/`](../../gateway/internal/proxy/) | Reverse proxy y rutas |
| [`gateway/internal/middleware/`](../../gateway/internal/middleware/) | CORS, límite de pedidos, IP del cliente, métricas |

## 3. Rutas

El path no cambia: para pasar por el gateway, un cliente solo cambia el host y el puerto.

| Método y ruta | Servicio | Acceso |
|---|---|---|
| `POST /api/sessions/{id}/pay` | Terminal Gateway | Token de la sesión, con límite por IP |
| `GET /api/sessions/{id}/status` | Terminal Gateway | Token de la sesión, con límite por IP |
| `POST /api/sessions/create` | Terminal Gateway | Token de terminal |
| `GET /sessions/{id}` | Terminal Gateway | Token de terminal |
| `POST /sessions/{id}/cancel`, `POST /sessions/{id}/reversal` | Terminal Gateway | Token de terminal |
| `POST /batch-close` | Terminal Gateway | Token de terminal |
| `GET /batches/{id}`, `POST /batches/{id}/{force-close,reopen,resolve-dispute,resubmit,settle}` | Settlement | Token de administración |
| `GET /merchants/{merchant_id}/batches`, `GET` y `PUT /merchants/{merchant_id}/timezone` | Settlement | Token de administración |
| `GET /reversal-reviews`, `POST /reversal-reviews/{transaction_id}/resolve` | Settlement | Token de administración |
| `GET /healthz` | Gateway | Público |

- **Un ID que no es UUID** responde `400 INVALID_ID` sin llegar al servicio.
- **Cualquier otra ruta o método** responde `404 NOT_FOUND`.
- **Sin `SETTLEMENT_URL`**, sus rutas no se publican.

Una ruta nueva de un servicio no queda expuesta hasta agregarla en
[`router.go`](../../gateway/internal/proxy/router.go).

## 4. Comportamiento

### IP del cliente

El gateway reemplaza `X-Forwarded-For` por la IP de la conexión: un cliente no puede declarar otra. Si el gateway está
detrás de un balanceador, hay que declararlo en `GATEWAY_TRUSTED_PROXIES`, y entonces se conserva la cadena que este
escribió.

Para que los límites de intentos de Terminal Gateway y Settlement vean la IP del cliente y no la del gateway, los dos
declaran al gateway como proxy de confianza: `TRUSTED_PROXIES` y `SETTLEMENT_TRUSTED_PROXIES`. Si no, agruparían a todos los
clientes bajo la IP del gateway, y 10 tokens inválidos de cualquiera bloquearían a todos durante 5 minutos.

En Docker Compose, el gateway llega a los dos por la red `edge` (`172.30.0.0/24`), con la IP fija `172.30.0.10`, y los dos
confían solo en esa dirección. Usa los alias `terminal-gateway-edge` y `settlement-edge`, que solo existen en esa red: así la
conexión siempre sale desde la IP fija. En otro despliegue, hay que declarar la IP o la subred del gateway en esas variables.

### Límite de pedidos

`/pay` y `/status` no usan el token del terminal, así que el gateway limita los pedidos por IP (en IPv6, por `/64`). Usa un balde de
fichas que se recarga a `GATEWAY_PAY_PER_MINUTE` (20) y `GATEWAY_STATUS_PER_MINUTE` (240) por minuto, con ráfagas de ese
tamaño. `/status` tiene un margen mayor porque el teléfono lo consulta hasta que hay resultado. Al pasarlo, responde `429` con
`Retry-After`. Vive en memoria, por instancia.

### CORS

Solo responde el gateway, con los orígenes de `GATEWAY_CORS_ALLOWED_ORIGINS`. Un preflight `OPTIONS` se responde sin llegar al
servicio, y los encabezados `Access-Control-*` que devuelven los servicios se descartan: dos políticas en la misma respuesta
hacen que el navegador la rechace.

### Errores propios

Tienen la misma forma que los de los servicios: `{"error": "CODIGO", "message": "..."}`.

| Código | Cuándo |
|---|---|
| `400 INVALID_ID` | Un ID de la ruta no es UUID |
| `404 NOT_FOUND` | Ruta o método no publicados |
| `413 BODY_TOO_LARGE` | El cuerpo supera `GATEWAY_MAX_BODY_BYTES` (1 MiB) |
| `429 TOO_MANY_REQUESTS` | Se superó el límite de `/pay` o `/status` |
| `502 UPSTREAM_UNAVAILABLE` | El servicio no responde. El detalle queda en el log, no en la respuesta |
| `504 UPSTREAM_TIMEOUT` | El servicio no respondió en 30 s |

### TLS

Con `GATEWAY_TLS_CERT` y `GATEWAY_TLS_KEY`, el puerto 8000 sirve solo HTTPS, desde TLS 1.2. Si TLS lo termina otro proxy, se
declara con `GATEWAY_TLS_EXTERNAL=true`. **Fuera de `ENVIRONMENT=development`, el gateway no arranca sin una de las dos.**

Hacia los servicios, usa `https://` en `TERMINAL_GATEWAY_URL` o `SETTLEMENT_URL` si sirven HTTPS. Con un certificado propio, la
CA va en `GATEWAY_UPSTREAM_CA`. Si el gateway termina TLS, Terminal Gateway puede declararlo con `HTTP_TLS_EXTERNAL=true`.

## 5. Observabilidad

El puerto 8001 sirve `/healthz`, `/readyz` y `/metrics`, y no se expone fuera de la red interna. `/readyz` no consulta a los
servicios: la caída de uno no debe sacar de rotación al gateway entero.

| Métrica | Tipo | Labels |
|---|---|---|
| `posnet_gateway_requests_total` | Counter | `route` (el patrón, sin IDs), `code` |
| `posnet_gateway_request_duration_seconds` | Histogram | `route`, `code` |

Cada pedido abre un span y propaga el trace context a los servicios.

## 6. Configuración y ejecución

| Variable | Default | Uso |
|---|---|---|
| `GATEWAY_PORT` | `8000` | Puerto público |
| `GATEWAY_ADMIN_PORT` | `8001` | `/healthz`, `/readyz` y `/metrics`. Tiene que ser distinto del público |
| `TERMINAL_GATEWAY_URL` | `http://localhost:8081` | Obligatoria. Sin path |
| `SETTLEMENT_URL` | `http://localhost:8084` | Definida y vacía, no se publican sus rutas |
| `GATEWAY_UPSTREAM_CA` | Vacía | CA para servicios con HTTPS y certificado propio |
| `GATEWAY_CORS_ALLOWED_ORIGINS` | `http://localhost:5173` | Separados por comas. `*` solo en desarrollo |
| `GATEWAY_TLS_CERT`, `GATEWAY_TLS_KEY` | Vacías | HTTPS en el puerto público. Van juntas |
| `GATEWAY_TLS_EXTERNAL` | `false` | Declara que TLS lo termina otro proxy |
| `GATEWAY_TRUSTED_PROXIES` | Vacía | CIDR cuyo `X-Forwarded-For` se acepta |
| `GATEWAY_PAY_PER_MINUTE`, `GATEWAY_STATUS_PER_MINUTE` | `20`, `240` | Límites por IP |
| `GATEWAY_MAX_BODY_BYTES` | `1048576` | Tamaño máximo del cuerpo |
| `ENVIRONMENT` | `development` | Fuera de `development`, exige TLS y prohíbe CORS `*` |
| `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT` | `posnet-gateway`, `otel-collector:4317` | Trazas |

Una configuración inválida impide arrancar.

```bash
# Docker Compose: no carga .env.docker, porque no necesita credenciales
docker compose -f deployments/docker/docker-compose.yml up -d --build gateway

# Sin Docker, contra servicios en localhost
go run ./gateway/cmd/gateway

# Tests
go test ./gateway/...
```

Para probar un pago, se usan los mismos comandos de [Terminal Gateway](TERMINAL-GATEWAY.md#probar-un-pago), cambiando el
puerto `8081` por `8000`.

## 7. Brechas conocidas

| Brecha | Consecuencia |
|---|---|
| Los servicios siguen publicados en Docker Compose | `8081`, `8084` y los demás puertos siguen abiertos en el host, así que se puede saltear el gateway. En producción, solo el gateway debería ser accesible desde afuera |
| Límites por instancia | Con varias instancias, cada una tiene su propio límite. Compartirlos requiere un almacenamiento común, como Redis |
| Sin WebSocket | Cuando Terminal Gateway tenga su servidor WebSocket, el gateway va a necesitar una ruta que haga el upgrade |

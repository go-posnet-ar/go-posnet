# POSNET Load Tests — Locust

Directorio: `load-tests/locust/`

## Instalación

```bash
pip install -r requirements.txt
```

## Archivos

| Archivo | BC | Descripción |
|---|---|---|
| `authorization_locust.py` | Authorization | HTTP + flujo NATS completo |
| `fraud_locust.py` | Fraud Detection | *(próximo)* |
| `settlement_locust.py` | Settlement | *(próximo)* |

---

## Authorization — Uso rápido

```bash
# UI web (recomendado para empezar)
locust -f authorization_locust.py --host http://localhost:8080

# Solo HTTP (sin NATS — más rápido para CI)
locust -f authorization_locust.py AuthHTTPUser --host http://localhost:8080

# Headless — 20 usuarios, 2 por segundo, 2 minutos
locust -f authorization_locust.py \
  --headless -u 20 -r 2 --run-time 2m \
  --host http://localhost:8080

# Con NATS custom
NATS_URL=nats://192.168.0.2:4222 \
  locust -f authorization_locust.py --host http://localhost:8080
```

## Métricas reportadas

### HTTP (AuthHTTPUser)
| Nombre | Qué mide |
|---|---|
| `GET /healthz` | Latencia del liveness probe |
| `GET /readyz` | Latencia del readiness probe (incluye ping a Postgres) |
| `GET /transactions/{id}` | Latencia de consulta de estado |
| `GET /transactions/{id} [invalid]` | Manejo de errores (debe retornar 400) |

### NATS + HTTP (AuthNATSUser)
| Nombre | Qué mide |
|---|---|
| `saga/CHIP/normal/publish` | Latencia del publish a NATS JetStream |
| `saga/CHIP/normal/e2e` | Latencia total: publish → APPROVED/REJECTED |
| `saga/CONTACTLESS/low/publish` | Ídem CONTACTLESS |
| `saga/MAGSTRIPE/high-amount/publish` | Publish con monto alto (activa reglas de fraude) |
| `saga/MAGSTRIPE/high-amount/e2e` | E2E con motor de fraude activado |
| `saga/idempotency/e2e` | Verifica que eventos duplicados no generen efectos dobles |

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | URL de NATS JetStream |
| `POLL_TIMEOUT_S` | `10` | Timeout de polling (segundos) |
| `POLL_INTERVAL_S` | `0.5` | Intervalo de polling (segundos) |
| `TG_TERMINAL_TOKEN` | Vacía | Token del terminal para `terminal_gateway_locust.py`, si Terminal Gateway tiene `TERMINAL_TOKENS_FILE` |

## Umbrales de referencia (MVP en local)

| Métrica | Objetivo | Alerta |
|---|---|---|
| `GET /healthz` p99 | < 10ms | > 50ms |
| `GET /transactions/{id}` p99 | < 100ms | > 500ms |
| `saga/CHIP/normal/e2e` p50 | < 2s | > 5s |
| `saga/CHIP/normal/e2e` p95 | < 4s | > 8s |
| `saga/MAGSTRIPE/high-amount/e2e` p95 | < 5s | > 10s |
| Error rate total | < 1% | > 5% |
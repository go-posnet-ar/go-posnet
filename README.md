# go-posnet

Backend del Sistema POSNET — procesamiento de pagos con tarjeta en tiempo real.

Implementado en **Go 1.25**, siguiendo **Domain-Driven Design (DDD)** con **Arquitectura Hexagonal** (Ports & Adapters), organizado como **monorepo multi-módulo** con un único `go.mod`.

---

## Stack tecnológico

| Componente | Tecnología | Rol |
|---|---|---|
| Lenguaje | Go 1.25 | Backend principal |
| Base de datos | PostgreSQL 16 | Persistencia por Bounded Context |
| Mensajería | NATS JetStream 2.x | Bus de eventos asíncrono entre BCs |
| API externa | REST + WebSocket | Conexión con terminales POSNET |
| API interna | gRPC | Comunicación síncrona entre BCs |
| Migraciones | golang-migrate | Schema versionado por BC |
| Queries SQL | sqlc + pgx/v5 | Acceso a BD type-safe sin ORM |
| Observabilidad | OpenTelemetry | Trazas distribuidas, métricas, logs |
| Métricas | Prometheus | Exportación de métricas operativas |
| Trazas | Jaeger / Tempo | Visualización de trazas distribuidas |
| Seguridad | mTLS + NKey + JWT | Autenticación de terminales y servicios |

---

## Arquitectura

### Bounded Contexts

El sistema está dividido en **5 Bounded Contexts**, cada uno con su propio modelo de dominio, schema de base de datos y configuración de NATS. Ningún BC importa código de otro BC — la comunicación es exclusivamente vía eventos NATS JetStream.

| BC | Directorio | Responsabilidad |
|---|---|---|
| **Terminal Gateway** | `context/terminal-gateway/` | Conexión WebSocket/mTLS con terminales POSNET. Traduce ISO 8583 a eventos de dominio |
| **Authorization** | `context/authorization/` | Orquesta la Saga de autorización: fraud check → adquirente → resultado |
| **Fraud Detection** | `context/fraud-detection/` | Motor de reglas antifraude. Calcula score 0–100 por transacción |
| **Settlement** | `context/settlement/` | Cierre de lote, conciliación y generación de remesas al procesador |
| **Notification** | `context/notification/` | Comprobantes, webhooks al comercio, alertas operativas |

### Estructura hexagonal (por BC)

Cada BC sigue la misma estructura interna:

```
context/{bc}/
├── domain/             # Núcleo — sin dependencias externas
│   ├── aggregate/      # Aggregate Roots y sus invariantes
│   ├── entity/         # Entidades de dominio
│   ├── valueobject/    # Value Objects (inmutables)
│   ├── repository/     # Interfaces (puertos de salida hacia BD)
│   ├── service/        # Domain Services y puertos de salida (NATS, adquirente)
│   └── event/          # Domain Events internos del BC
├── application/        # Casos de uso — orquesta el dominio
│   ├── command/        # Command handlers (escritura)
│   ├── query/          # Query handlers (lectura, CQRS)
│   └── port/           # Puertos de entrada (interfaces públicas del BC)
├── infrastructure/     # Adaptadores concretos
│   ├── postgres/       # Repositorio Postgres (sqlc + pgx/v5)
│   ├── nats/           # Publisher y Subscriber de JetStream
│   ├── grpc/           # Server y clients gRPC
│   └── http/           # Handler REST (healthz, endpoints de operación)
└── config/             # Configuración del BC (env vars)
```

### Regla de dependencias

```
infrastructure → application → domain ← pkg/
```

- `domain/` no importa nada de `application/` ni `infrastructure/`
- Ningún BC importa código de otro BC
- Solo `pkg/` (Shared Kernel) puede ser importado por todos

Estas reglas se verifican en CI con **go-arch-lint**. Si se introduce un import cruzado entre BCs, el build falla.

---

## Estructura del repositorio

```
go-posnet/
├── cmd/                        # Entrypoints — un binario por BC
│   ├── terminal-gateway/
│   ├── authorization/
│   ├── fraud-detection/
│   ├── settlement/
│   └── notification/
│
├── gateway/                    # API Gateway: único punto de entrada HTTP (:8000)
│
├── context/                    # Bounded Contexts
│   ├── terminal-gateway/
│   ├── authorization/
│   ├── fraud-detection/
│   ├── settlement/
│   └── notification/
│
├── pkg/                        # Shared Kernel — importable por todos los BCs
│   ├── domain/                 # Value Objects: Money, PAN, TransactionID, STAN...
│   ├── events/                 # Contratos de eventos inter-BC (envelope + payloads)
│   ├── errors/                 # Tipos de error de dominio
│   ├── observability/          # OpenTelemetry: tracer, logger, NATS hook
│   ├── natsutil/               # Conexión, streams, publisher, idempotency store
│   ├── pgutil/                 # Pool, transacciones, migraciones
│   ├── validator/              # ISO 8583, Luhn, monto, STAN
│   ├── crypto/                 # HMAC, TLS, NKey
│   └── proto/                  # Protobuf generado para gRPC interno
│
├── migrations/                 # Migraciones SQL por BC
│   ├── terminal-gateway/
│   ├── authorization/
│   ├── fraud-detection/
│   ├── settlement/
│   └── notification/
│
├── deployments/
│   ├── docker/                 # Docker Compose (dev, test)
│   ├── helm/                   # Helm charts para Kubernetes
│   └── nats/                   # Configuración de NATS y declaración de streams
│
├── scripts/                    # Utilidades de desarrollo
├── docs/                       # Documentación técnica y ADRs
├── .github/workflows/          # CI/CD (GitHub Actions)
├── go.mod                      # Módulo raíz único
└── Makefile
```

---

## Shared Kernel — `pkg/`

El Shared Kernel contiene únicamente tipos y contratos transversales. **No contiene lógica de negocio de ningún BC.**

### Paquetes principales

| Paquete | Contenido clave |
|---|---|
| `pkg/domain` | `Money` (centavos int64), `PAN` (solo last4), `TransactionID`, `TerminalID`, `MerchantID`, `STAN`, `AuthCode`, `Currency` |
| `pkg/events` | `DomainEvent` envelope, `Wrap`/`Unwrap[T]`, subjects NATS, payloads de los 11 eventos inter-BC |
| `pkg/errors` | `NotFoundError`, `ValidationError`, `ConflictError`, `TimeoutError`, `FraudError`, `AcquirerError` |
| `pkg/observability` | `InitTracer`, `StartSpan`, `RecordError`, `FromContext` (logger), `InjectTraceContext` / `ExtractTraceContext` para NATS |
| `pkg/natsutil` | `Connect` (NKey+TLS), `EnsureStreams` (idempotente), `Publisher`, `IdempotencyStore` |
| `pkg/pgutil` | `NewPool`, `WithTransaction`, `WithReadCommitted`, `Migrate` |
| `pkg/validator` | `LuhnCheck`, `ValidateISO8583`, `ValidateAmount`, `ValidateSTAN` |
| `pkg/crypto` | `SignMessage`/`VerifyMessage` (HMAC-SHA256), `LoadTLSConfig` (mTLS), `LoadNKeyOption` |

### Reglas del Shared Kernel

1. `pkg/` no importa nada de `context/`
2. Un cambio en `pkg/` afecta a todos los BCs — requiere revisión del equipo
3. No contiene lógica de negocio específica de ningún dominio
4. Alta estabilidad — los tipos aquí son contratos públicos

---

## NATS JetStream

### Streams

| Stream | Subjects | Retención | Réplicas |
|---|---|---|---|
| `POSNET_TRANSACTIONS` | `posnet.transaction.>` | 7 días | 3 |
| `POSNET_FRAUD` | `posnet.fraud.>` | 72 horas | 3 |
| `POSNET_AUTH` | `posnet.auth.>` | 7 días | 3 |
| `POSNET_SETTLEMENT` | `posnet.settlement.>` | 30 días | 3 |
| `POSNET_NOTIFICATION` | `posnet.notification.>` | 48 horas | 2 |
| `POSNET_DLQ` | `posnet.dlq.>` | 30 días | 2 |

### Subjects (nomenclatura: `posnet.{dominio}.{evento}.{versión}`)

```
posnet.transaction.received.v1
posnet.transaction.reversal-requested.v1
posnet.transaction.batch-close.v1
posnet.fraud.check-requested.v1
posnet.fraud.score-calculated.v1
posnet.auth.approved.v1
posnet.auth.rejected.v1
posnet.auth.reversal-completed.v1
posnet.settlement.batch-closed.v1
posnet.settlement.completed.v1
posnet.notification.dispatched.v1
```

### Políticas de entrega

- **ACK explícito**: el handler hace `Ack()` solo después de escribir en Postgres
- **Nak con backoff**: errores transitorios → `Nak()` → reintento exponencial (1s → 2s → 4s → 8s → 16s)
- **AckWait**: 30 segundos para consumers de transacciones críticas
- **Idempotencia**: cada handler verifica `event_id` en `processed_events` antes de procesar
- **Dead Letter Queue**: mensajes que superan `MaxDeliver` van a `POSNET_DLQ`

---

## Módulo de go

El proyecto usa **un único `go.mod`** en la raíz. No se usa `go work` ni múltiples módulos — todos los BCs y el Shared Kernel comparten el mismo módulo Go.

```
module github.com/juantevez/go-posnet

go 1.25
```

Esto significa que todos los imports internos usan el mismo prefijo:

```go
import "github.com/juantevez/go-posnet/pkg/domain"
import "github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
```

Ver [docs/adr/003-hexagonal-ddd.md](docs/adr/003-hexagonal-ddd.md) para la justificación de esta decisión.

---

## Primeros pasos

### Prerequisitos

```
Go 1.25
Docker y Docker Compose
make
```

### Levantar el entorno de desarrollo

```bash
# Clonar el repo
git clone https://github.com/juantevez/go-posnet
cd go-posnet

# Levantar Postgres, NATS y el collector de OpenTelemetry
make dev-up
# equivale a: docker compose -f deployments/docker/docker-compose.dev.yml up -d

# Crear streams y consumers en NATS
make nats-setup
# equivale a: ./scripts/nats-setup.sh

# Correr las migraciones de todos los BCs
make migrate
# equivale a: go run ./cmd/{bc}/... migrate (por cada BC)

# Compilar todos los binarios
make build
# equivale a: go build ./cmd/...

# Correr tests
make test
# equivale a: go test ./...

# Verificar arquitectura (imports cruzados)
make arch-check
# equivale a: go-arch-lint check
```

### Variables de entorno

Cada BC tiene su propio conjunto de variables. Copiá `.env.example` y completá los valores:

```bash
cp .env.example .env
```

Variables requeridas por todos los BCs:

```bash
POSTGRES_DSN=postgresql://posnet:posnet@localhost:5432/posnet?sslmode=disable
NATS_URL=nats://localhost:4222
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
ENVIRONMENT=development
```

Variables específicas de Authorization:

```bash
ACQUIRER_HOST=acquirer.example.com
ACQUIRER_PORT=9100
ACQUIRER_TLS_CERT=certs/acquirer-client.crt
ACQUIRER_TLS_KEY=certs/acquirer-client.key
ACQUIRER_TLS_CA=certs/acquirer-ca.crt
ACQUIRER_TIMEOUT_SECONDS=30
```

---

## Makefile — targets disponibles

```bash
make build          # Compila todos los binarios en bin/
make test           # go test ./... con -race
make test-coverage  # Tests con reporte de cobertura HTML
make lint           # golangci-lint run
make arch-check     # go-arch-lint check (imports cruzados)
make generate       # go generate ./... (sqlc + protobuf)
make gen-proto      # Regenera .pb.go desde .proto
make gen-sqlc       # Regenera queries sqlc por BC
make migrate        # Corre migraciones pendientes en todos los BCs
make dev-up         # Levanta infraestructura local (Docker Compose)
make dev-down       # Baja la infraestructura local
make nats-setup     # Crea streams y consumers en NATS
make seed           # Carga datos iniciales para desarrollo
make keygen         # Genera NKeys de NATS para desarrollo
make tidy           # go mod tidy
make clean          # Borra binarios compilados
```

---

## Flujo de una transacción

```
Terminal POSNET
     │  ISO 8583 / WebSocket (mTLS)
     ▼
[Terminal Gateway BC]
     │  posnet.transaction.received.v1
     ▼
[Authorization BC] ──► posnet.fraud.check-requested.v1 ──► [Fraud Detection BC]
     │                                                              │
     │  ◄─────────────── posnet.fraud.score-calculated.v1 ◄────────┘
     │
     │  ISO 8583 / TCP+TLS
     ▼
Host Adquirente → Switch → Banco Emisor
     │
     │  posnet.auth.approved.v1  (o rejected)
     ▼
[Authorization BC]
     │
     ├──► posnet.auth.approved.v1 ──► [Terminal Gateway BC] ──► Terminal (resultado)
     ├──► posnet.auth.approved.v1 ──► [Settlement BC]       ──► Agrega al batch del día
     └──► posnet.auth.approved.v1 ──► [Notification BC]     ──► Comprobante + Webhook
```

---

## Seguridad

| Capa | Mecanismo |
|---|---|
| Terminales → Gateway | mTLS (certificados X.509 por terminal) |
| Servicios internos | mTLS entre pods (service mesh) |
| Autenticación NATS | NKey (Ed25519) + TLS 1.3 |
| Firma de mensajes NATS | HMAC-SHA256 en header `X-Signature` |
| Secrets | HashiCorp Vault (nunca en env vars ni archivos en repo) |
| PAN de tarjeta | Solo últimos 4 dígitos en el backend. El PAN completo viaja cifrado EMV y nunca se persiste |
| PIN del cliente | Cifrado con DUKPT/AES en el PINpad. El backend nunca lo ve en claro |
| Auditoría | `processed_events` es append-only. Nunca se borra ni actualiza |

---

## Observabilidad

Cada transacción genera una **traza distribuida completa** que atraviesa todos los BCs involucrados. El `TransactionID` es propagado como `TraceID` de OpenTelemetry a través de los headers de los mensajes NATS.

```
Jaeger UI → buscar por TransactionID → ver toda la traza completa
```

### Métricas clave (Prometheus)

```
posnet_transactions_total                  # Por estado: approved/rejected
posnet_authorization_duration_seconds      # Latencia E2E de autorización (P50/P95/P99)
posnet_acquirer_request_duration_seconds   # Latencia del adquirente externo
posnet_fraud_score_distribution            # Distribución de scores de fraude
posnet_nats_consumer_lag                   # Mensajes pendientes por consumer
posnet_active_sessions                     # Sesiones WebSocket activas
posnet_webhook_delivery_failures_total     # Fallos de webhook por comercio
```

---

## Documentación

| Documento | Descripción |
|---|---|
| [docs/architecture/README.md](docs/architecture/README.md) | Visión arquitectónica general |
| [docs/adr/001-go-language.md](docs/adr/001-go-language.md) | Por qué Go sobre Java/Rust |
| [docs/adr/002-nats-jetstream.md](docs/adr/002-nats-jetstream.md) | Por qué NATS sobre Kafka |
| [docs/adr/003-hexagonal-ddd.md](docs/adr/003-hexagonal-ddd.md) | Hexagonal + DDD + monorepo |
| [docs/adr/004-sqlc-over-orm.md](docs/adr/004-sqlc-over-orm.md) | Por qué sqlc sobre GORM |
| [docs/technical/GATEWAY.md](docs/technical/GATEWAY.md) | API Gateway: rutas publicadas, límites, CORS y TLS |
| [docs/runbooks/incident-response.md](docs/runbooks/incident-response.md) | Respuesta ante incidentes |
| [docs/runbooks/batch-close-failure.md](docs/runbooks/batch-close-failure.md) | Fallo en cierre de lote |
| [docs/runbooks/nats-dlq-reprocess.md](docs/runbooks/nats-dlq-reprocess.md) | Reprocesamiento desde DLQ |


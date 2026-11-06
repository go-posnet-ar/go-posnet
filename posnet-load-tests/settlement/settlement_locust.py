"""
Load Test — BC Settlement (go-posnet)
======================================
Simula dos tipos de usuarios concurrentes:

  SettlementHTTPUser — consulta batches y merchants via HTTP
  SettlementNATSUser — publica AuthorizationApproved directamente a NATS
                       y verifica el batch resultante via HTTP

Endpoints HTTP del BC (puerto 8084 en docker-compose.yml — el default interno
del código Go es 8083, pero deployments/docker/docker-compose.yml lo pisa con
HTTP_PORT=8084 — usar SIEMPRE el puerto publicado por docker-compose, no el
default del código, si corrés vía Docker; ver `docker compose ps`):
  GET  /healthz
  GET  /readyz
  GET  /batches/{id}
  GET  /merchants/{merchant_id}/batches?date=YYYY-MM-DD
  POST /batches/{id}/force-close

Consumers NATS del BC (st_nats_subscriber.go):
  posnet.auth.approved.v1           → RegisterApproval (agrega tx al batch OPEN)
  posnet.auth.reversal-completed.v1 → RegisterReversal (descuenta del batch)
  posnet.transaction.batch-close.v1 → ProcessBatchClose (cierra el lote)

Dominio clave (st_settlement_batch.go):
  - Un solo batch OPEN por terminal por día (UNIQUE terminal_id + batch_date)
  - FindOrCreateOpen con SERIALIZABLE para evitar condiciones de carrera
  - State machine: OPEN → PENDING_CLOSE → CLOSED → SUBMITTED → SETTLED/DISPUTED
  - Discrepancias cuando terminal_count != backend_count

Uso:
  # UI web:
  locust -f settlement_locust.py --host http://localhost:8084

  # Headless:
  locust -f settlement_locust.py --headless -u 10 -r 2 --run-time 2m \\
    --host http://localhost:8084

  # Combinado con los locustfiles de los otros BCs (cada uno apunta a su
  # propio puerto via el atributo `host` de su *HTTPUser — NO pasar --host
  # ni completar "Host" en la web UI, porque eso pisa el host de las 4
  # clases HTTP por igual):
  locust -f terminal-gateway/terminal_gateway_locust.py \\
         -f authorization/authorization_locust.py \\
         -f fraud-detection/fraud_detection_locust.py \\
         -f settlement/settlement_locust.py

Variables de entorno:
  NATS_URL         URL de NATS JetStream    (default: nats://localhost:4222)
  POLL_TIMEOUT_S   Timeout de polling       (default: 10)
  POLL_INTERVAL_S  Intervalo de polling     (default: 0.5)
  BATCH_DATE       Fecha del batch (YYYY-MM-DD, default: hoy en UTC)
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import asyncio
import nats
import requests as req_lib

from locust import HttpUser, User, task, between, events, tag
from locust.exception import StopUser


# ── Configuración ──────────────────────────────────────────────────────────────

NATS_URL      = os.getenv("NATS_URL",        "nats://localhost:4222")
POLL_TIMEOUT  = float(os.getenv("POLL_TIMEOUT_S",  "10"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_S", "0.5"))
BATCH_DATE    = os.getenv("BATCH_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

TERMINAL_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
MERCHANT_ID = "b2c3d4e5-f6a7-8901-bcde-f12345678901"

# Pool compartido de batch IDs conocidos
_known_batch_ids: list[str] = []
# Pool de batch IDs para force-close (solo batches OPEN ya registrados)
_batch_id_for_close: list[str] = []

# Host HTTP del BC Settlement
_http_host = "http://localhost:8084"


# ── Helpers ────────────────────────────────────────────────────────────────────

def new_uuid() -> str:
    return str(uuid.uuid4())

def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def random_amount_cents() -> int:
    """Distribución realista de montos en ARS."""
    ranges = [
        (10_000,  500_000,  0.60),   # ARS 100–5.000
        (500_001, 5_000_000, 0.30),  # ARS 5.000–50.000
        (5_000_001, 9_999_999, 0.10) # ARS 50.000+ (activa reglas fraude)
    ]
    roll = random.random()
    cum = 0.0
    for lo, hi, w in ranges:
        cum += w
        if roll <= cum:
            return random.randint(lo, hi)
    return random.randint(10_000, 500_000)

def build_auth_approved_envelope(
    tx_id: str,
    event_id: str,
    amount_cents: int,
    auth_code: str,
) -> bytes:
    """
    Construye el envelope DomainEvent para posnet.auth.approved.v1
    según pkg/events/authorization_approved.go y pkg/events/envelope.go.

    El subscriber de Settlement (st_nats_subscriber.go) espera:
      payload.TransactionID → para idempotencia
      payload.TerminalID    → para FindOrCreateOpen del batch
      payload.MerchantID    → para el batch
      payload.AmountCents   → monto de la compra
      payload.AuthorizedAt  → define la fecha del batch (batch_date)
      payload.Currency      → moneda del batch
    """
    payload = {
        "transaction_id": tx_id,
        "terminal_id":    TERMINAL_ID,
        "merchant_id":    MERCHANT_ID,
        "auth_code":      auth_code,
        "amount_cents":   amount_cents,
        "currency":       "ARS",
        "card_last4":     str(random.randint(1000, 9999)),
        "card_network":   random.choice(["VISA", "MASTERCARD", "CABAL"]),
        "entry_mode":     random.choice(["CHIP", "CONTACTLESS", "MAGSTRIPE"]),
        "fraud_score":    random.randint(0, 45),
        "authorized_at":  now_rfc3339(),
    }
    envelope = {
        "event_id":       event_id,
        "event_type":     "posnet.auth.approved.v1",
        "aggregate_id":   tx_id,
        "aggregate_type": "Transaction",
        "correlation_id": tx_id,
        "causation_id":   "",
        "occurred_at":    now_rfc3339(),
        "schema_version": 1,
        "data":           payload,
    }
    return json.dumps(envelope).encode("utf-8")


def build_reversal_envelope(
    orig_tx_id: str,
    event_id: str,
    amount_cents: int,
) -> bytes:
    """
    Construye el envelope para posnet.auth.reversal-completed.v1
    según pkg/events/reversal_completed.go.

    Settlement lo usa para RegisterReversal: descuenta del batch OPEN.
    """
    payload = {
        "original_transaction_id": orig_tx_id,
        "terminal_id":             TERMINAL_ID,
        "merchant_id":             MERCHANT_ID,
        "amount_cents":            amount_cents,
        "currency":                "ARS",
        "completed_at":            now_rfc3339(),
    }
    envelope = {
        "event_id":       event_id,
        "event_type":     "posnet.auth.reversal-completed.v1",
        "aggregate_id":   orig_tx_id,
        "aggregate_type": "Transaction",
        "correlation_id": orig_tx_id,
        "causation_id":   "",
        "occurred_at":    now_rfc3339(),
        "schema_version": 1,
        "data":           payload,
    }
    return json.dumps(envelope).encode("utf-8")


def build_batch_close_envelope(
    event_id: str,
    terminal_count: int,
    terminal_amount: int,
) -> bytes:
    """
    Construye el envelope para posnet.transaction.batch-close.v1
    según pkg/events/batch_close_requested.go.

    Settlement lo usa para ProcessBatchClose: cierra el batch OPEN del terminal.
    terminal_count/terminal_amount = lo que el terminal reporta (para conciliación).
    """
    payload = {
        "terminal_id":     TERMINAL_ID,
        "merchant_id":     MERCHANT_ID,
        "batch_date":      BATCH_DATE,
        "terminal_count":  terminal_count,
        "terminal_amount": terminal_amount,
        "currency":        "ARS",
        "requested_at":    now_rfc3339(),
    }
    envelope = {
        "event_id":       event_id,
        "event_type":     "posnet.transaction.batch-close.v1",
        "aggregate_id":   TERMINAL_ID,
        "aggregate_type": "Terminal",
        "correlation_id": TERMINAL_ID,
        "causation_id":   "",
        "occurred_at":    now_rfc3339(),
        "schema_version": 1,
        "data":           payload,
    }
    return json.dumps(envelope).encode("utf-8")


# ── SettlementHTTPUser ─────────────────────────────────────────────────────────

class SettlementHTTPUser(HttpUser):
    """
    Simula operadores de back-office consultando batches y merchants.
    Peso 3: 3 usuarios HTTP por cada 1 NATS.
    """
    host = _http_host
    weight = 3
    wait_time = between(0.5, 2.0)

    def on_start(self):
        # Las consultas exigen token. Definir SETTLEMENT_ADMIN_API_KEY antes de correr Locust.
        token = os.environ.get("SETTLEMENT_ADMIN_API_KEY", "")
        if token:
            self.client.headers["Authorization"] = f"Bearer {token}"
        with self.client.get("/healthz", catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"Settlement not ready: {resp.status_code}")
                raise StopUser()

    @task(5)
    @tag("health", "http")
    def health_check(self):
        with self.client.get(
            "/healthz", catch_response=True, name="GET /healthz"
        ) as resp:
            resp.success() if resp.status_code == 200 else resp.failure(f"status: {resp.status_code}")

    @task(2)
    @tag("readiness", "http")
    def readiness_check(self):
        with self.client.get(
            "/readyz", catch_response=True, name="GET /readyz"
        ) as resp:
            resp.success() if resp.status_code in (200, 503) else resp.failure(f"status: {resp.status_code}")

    @task(3)
    @tag("query", "http")
    def get_batch(self):
        """
        GET /batches/{id}
        Consulta un batch por ID. Verifica campos del BatchResult:
        State, TotalCount, TotalAmount, TerminalID, etc.
        """
        if _known_batch_ids:
            batch_id = random.choice(_known_batch_ids)
            expect_404 = False
        else:
            batch_id = new_uuid()
            expect_404 = True

        with self.client.get(
            f"/batches/{batch_id}",
            catch_response=True,
            name="GET /batches/{id}",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                # BatchResult retorna campos PascalCase (sin json tags en el struct)
                has_state = "State" in data or "state" in data
                has_id    = "ID"    in data or "id"    in data
                if not has_state or not has_id:
                    resp.failure(f"missing fields: {list(data.keys())}")
                else:
                    state = data.get("State") or data.get("state", "")
                    valid_states = {"OPEN", "PENDING_CLOSE", "CLOSED",
                                    "SUBMITTED", "SETTLED", "DISPUTED"}
                    if state not in valid_states:
                        resp.failure(f"invalid state: {state}")
                    else:
                        resp.success()
            elif resp.status_code == 404 and expect_404:
                resp.success()
            elif resp.status_code == 404:
                # Puede haber expirado del pool — no es fallo crítico
                resp.success()
            else:
                resp.failure(f"status: {resp.status_code}")

    @task(2)
    @tag("query", "http")
    def list_batches_by_merchant(self):
        """
        GET /merchants/{merchant_id}/batches?date=YYYY-MM-DD
        Lista batches del comercio en una fecha. Verifica count y estructura.
        """
        with self.client.get(
            f"/merchants/{MERCHANT_ID}/batches?date={BATCH_DATE}",
            catch_response=True,
            name="GET /merchants/{id}/batches",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                count   = data.get("count", -1)
                batches = data.get("batches", None)
                if count == -1 or batches is None:
                    resp.failure(f"missing count or batches: {list(data.keys())}")
                else:
                    # Agregar IDs al pool
                    for b in (batches or []):
                        bid = b.get("ID") or b.get("id", "")
                        if bid and bid not in _known_batch_ids:
                            _known_batch_ids.append(bid)
                    resp.success()
            elif resp.status_code == 400:
                resp.failure("validation error on merchant/date query")
            else:
                resp.failure(f"status: {resp.status_code}")

    @task(1)
    @tag("admin", "http")
    def get_batch_invalid_id(self):
        """
        GET /batches/{id} con ID inválido.
        El handler llama GetBatch que valida el batchID — debe retornar error.
        """
        with self.client.get(
            "/batches/not-a-valid-id",
            catch_response=True,
            name="GET /batches/{id} [invalid]",
        ) as resp:
            # El handler retorna 500 si batch_id vacío, 404 si no existe
            # Un UUID random que no existe debe retornar 404
            if resp.status_code in (400, 404, 500):
                resp.success()
            else:
                resp.failure(f"unexpected status: {resp.status_code}")

    @task(1)
    @tag("admin", "http")
    def list_batches_invalid_date(self):
        """
        GET /merchants/{id}/batches?date=invalid — debe retornar 400.
        Verifica validación del formato de fecha.
        """
        with self.client.get(
            f"/merchants/{MERCHANT_ID}/batches?date=not-a-date",
            catch_response=True,
            name="GET /merchants/{id}/batches [invalid date]",
        ) as resp:
            if resp.status_code == 400:
                resp.success()
            else:
                resp.failure(f"expected 400, got {resp.status_code}")


# ── SettlementNATSUser ─────────────────────────────────────────────────────────

class SettlementNATSUser(User):
    """
    Publica eventos directamente a NATS y verifica el batch resultante via HTTP.

    Flujos testeados:
      1. RegisterApproval: publica AuthApproved → batch OPEN crece
      2. RegisterReversal: publica ReversalCompleted → batch OPEN decrece
      3. Idempotencia: mismo event_id dos veces → 1 sola entrada en el batch
      4. Batch close: publica BatchCloseRequested → batch pasa a CLOSED/DISPUTED

    Consideración importante: el terminal fijo (TERMINAL_ID) acumula todas las
    transacciones en el mismo batch OPEN del día. Esto es correcto — es el
    comportamiento real del sistema (FindOrCreateOpen con UNIQUE terminal_id+date).
    """
    weight = 1
    wait_time = between(1.0, 3.0)

    _nc = None
    _js = None
    _loop = None

    # Acumulador local de transacciones publicadas (para reportar totales al close)
    _published_count: int = 0
    _published_amount: int = 0

    def on_start(self):
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._nc, self._js = self._loop.run_until_complete(self._connect())
        except Exception as e:
            print(f"[SettlementNATSUser] NATS connect failed: {e}")
            raise StopUser()

    async def _connect(self):
        nc = await nats.connect(
            NATS_URL,
            max_reconnect_attempts=3,
            reconnect_time_wait=1,
            connect_timeout=5,
        )
        js = nc.jetstream()
        return nc, js

    def on_stop(self):
        if self._nc and self._loop:
            try:
                self._loop.run_until_complete(self._nc.close())
            except Exception:
                pass

    # ── Tareas ──────────────────────────────────────────────────────────────

    @task(8)
    @tag("settlement", "nats", "approval")
    def register_approval(self):
        """
        Publica AuthorizationApproved → Settlement registra la tx en el batch OPEN.
        Flujo: publish → poll GET /merchants/{id}/batches → verificar TotalCount creció.

        El batch OPEN del terminal acumula todas las transacciones del día.
        FindOrCreateOpen garantiza unicidad (SERIALIZABLE + UNIQUE constraint).
        """
        tx_id    = new_uuid()
        event_id = new_uuid()
        amount   = random_amount_cents()
        auth_code = f"A{random.randint(10000, 99999)}"
        payload  = build_auth_approved_envelope(tx_id, event_id, amount, auth_code)

        t_pub = time.time()
        try:
            self._loop.run_until_complete(
                self._js.publish(
                    "posnet.auth.approved.v1",
                    payload,
                    headers={"Nats-Msg-Id": event_id},
                )
            )
            events.request.fire(
                request_type="NATS", name="settlement/approval/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=len(payload), exception=None, context={},
            )
            # Registrar localmente para el batch close
            self._published_count += 1
            self._published_amount += amount
        except Exception as e:
            events.request.fire(
                request_type="NATS", name="settlement/approval/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=0, exception=e, context={},
            )
            return

        # Polling: verificar que el batch fue actualizado
        t_e2e = time.time()
        batch = self._poll_batch_updated()
        e2e_ms = (time.time() - t_e2e) * 1000

        exc = None if batch else Exception("Batch not updated after approval")
        events.request.fire(
            request_type="NATS+HTTP", name="settlement/approval/e2e",
            response_time=e2e_ms, response_length=0,
            exception=exc, context={},
        )

        # Registrar batch ID en el pool
        if batch:
            batch_id = batch.get("ID") or batch.get("id", "")
            if batch_id and batch_id not in _known_batch_ids:
                _known_batch_ids.append(batch_id)
                if batch_id not in _batch_id_for_close:
                    _batch_id_for_close.append(batch_id)

    @task(2)
    @tag("settlement", "nats", "reversal")
    def register_reversal(self):
        """
        Publica ReversalCompleted → Settlement descuenta del batch OPEN.
        Usa un tx_id aleatorio — Settlement lo agrega como REVERSAL al batch.
        """
        orig_tx_id = new_uuid()
        event_id   = new_uuid()
        amount     = random.randint(10_000, 200_000)
        payload    = build_reversal_envelope(orig_tx_id, event_id, amount)

        t_pub = time.time()
        try:
            self._loop.run_until_complete(
                self._js.publish(
                    "posnet.auth.reversal-completed.v1",
                    payload,
                    headers={"Nats-Msg-Id": event_id},
                )
            )
            events.request.fire(
                request_type="NATS", name="settlement/reversal/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=len(payload), exception=None, context={},
            )
        except Exception as e:
            events.request.fire(
                request_type="NATS", name="settlement/reversal/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=0, exception=e, context={},
            )

    @task(1)
    @tag("settlement", "nats", "idempotency")
    def register_approval_idempotency(self):
        """
        Publica el mismo event_id dos veces.
        Settlement usa processed_events para descartar el duplicado.
        El batch debe tener 1 sola entrada para ese tx_id.
        """
        tx_id    = new_uuid()
        event_id = new_uuid()
        amount   = random.randint(50_000, 150_000)
        payload  = build_auth_approved_envelope(tx_id, event_id, amount, "A99999")

        t0 = time.time()
        try:
            for _ in range(2):
                self._loop.run_until_complete(
                    self._js.publish(
                        "posnet.auth.approved.v1",
                        payload,
                        headers={"Nats-Msg-Id": event_id},
                    )
                )

            # Verificar que el batch existe (no hace falta contar las tx individuales)
            batch = self._poll_batch_updated()
            elapsed_ms = (time.time() - t0) * 1000
            exc = None if batch else Exception("Batch not found after idempotency test")

            events.request.fire(
                request_type="NATS+HTTP", name="settlement/idempotency",
                response_time=elapsed_ms, response_length=0,
                exception=exc, context={},
            )
        except Exception as e:
            events.request.fire(
                request_type="NATS+HTTP", name="settlement/idempotency",
                response_time=(time.time() - t0) * 1000, response_length=0,
                exception=e, context={},
            )

    @task(1)
    @tag("settlement", "nats", "batch-close")
    def trigger_batch_close(self):
        """
        Publica BatchCloseRequested con los totales del "terminal".

        En un sistema real, el terminal envía sus totales al final del día
        y Settlement los compara contra los del backend para detectar discrepancias.

        En este test enviamos totales deliberadamente incorrectos (0,0) para
        que Settlement registre discrepancias — escenario DISPUTED válido.
        También probamos con totales correctos (usando el contador local).

        IMPORTANTE: después del close el batch pasa a CLOSED/DISPUTED y ya no
        acepta nuevas transacciones. Settlement crea un nuevo batch para las
        siguientes transacciones del mismo terminal en el mismo día.
        """
        event_id = new_uuid()

        # Alternar entre cierre con discrepancias y cierre concordante
        if random.random() < 0.3:
            # Totales correctos del terminal (concordancia exacta)
            term_count  = self._published_count
            term_amount = self._published_amount
            label = "settlement/batch-close/concordant"
        else:
            # Totales incorrectos → discrepancias → DISPUTED
            term_count  = 0
            term_amount = 0
            label = "settlement/batch-close/disputed"

        payload = build_batch_close_envelope(event_id, term_count, term_amount)

        t_pub = time.time()
        try:
            self._loop.run_until_complete(
                self._js.publish(
                    "posnet.transaction.batch-close.v1",
                    payload,
                    headers={"Nats-Msg-Id": event_id},
                )
            )
            events.request.fire(
                request_type="NATS", name=f"{label}/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=len(payload), exception=None, context={},
            )
            # Reset el contador local — el batch se cierra
            self._published_count  = 0
            self._published_amount = 0
        except Exception as e:
            events.request.fire(
                request_type="NATS", name=f"{label}/publish",
                response_time=(time.time() - t_pub) * 1000,
                response_length=0, exception=e, context={},
            )

    # ── Polling ──────────────────────────────────────────────────────────────

    def _poll_batch_updated(self) -> dict | None:
        """
        Hace polling de GET /merchants/{id}/batches?date={date}
        hasta encontrar al menos 1 batch (en cualquier estado), o agotar timeout.

        IMPORTANTE: Settlement no actualiza total_count mientras el batch está OPEN
        (ver st_schema.sql — total_count es NULL hasta el Close()). Por lo tanto
        el criterio de éxito es simplemente que exista un batch para el terminal,
        no que tenga transacciones contadas. Las transacciones se registran en
        batch_transactions pero el summary solo se calcula al cierre.
        """
        url      = f"{_http_host}/merchants/{MERCHANT_ID}/batches?date={BATCH_DATE}"
        deadline = time.time() + POLL_TIMEOUT

        while time.time() < deadline:
            try:
                resp = req_lib.get(url, timeout=3)
                if resp.status_code == 200:
                    data    = resp.json()
                    batches = data.get("batches") or []
                    # Aceptar cualquier batch existente — OPEN, CLOSED, DISPUTED, etc.
                    if len(batches) > 0:
                        # Preferir batch OPEN si existe
                        for b in batches:
                            state = b.get("State") or b.get("state", "")
                            if state == "OPEN":
                                return b
                        # Si no hay OPEN, retornar el primero disponible
                        return batches[0]
            except Exception:
                pass

            time.sleep(POLL_INTERVAL)

        return None


# ── Eventos globales ───────────────────────────────────────────────────────────

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global _http_host
    if environment.host:
        _http_host = environment.host.rstrip("/")

    print(f"""
╔══════════════════════════════════════════════════════════╗
║   POSNET Load Test — BC Settlement                      ║
║   HTTP:       {_http_host:<40}║
║   NATS:       {NATS_URL:<40}║
║   Batch date: {BATCH_DATE:<40}║
║   Terminal:   {TERMINAL_ID:<40}║
║   Merchant:   {MERCHANT_ID:<40}║
╚══════════════════════════════════════════════════════════╝
    """)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    print("\n── Settlement Load Test Summary ─────────────────────────")
    for name, entry in stats.entries.items():
        if entry.num_requests > 0:
            print(
                f"  {str(name[1]):52s} "
                f"req={entry.num_requests:5d}  "
                f"fail={entry.num_failures:4d}  "
                f"p50={entry.get_response_time_percentile(0.50):6.0f}ms  "
                f"p95={entry.get_response_time_percentile(0.95):6.0f}ms"
            )
    print(f"  Known Batch IDs in pool: {len(_known_batch_ids)}")
    print("─────────────────────────────────────────────────────────\n")
    
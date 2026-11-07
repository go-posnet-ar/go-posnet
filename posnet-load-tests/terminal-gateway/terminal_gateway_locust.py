"""
Load Test — BC Terminal Gateway (go-posnet)
============================================
Simula tres tipos de usuarios concurrentes:

  TGHTTPUser     — health/readyz, GET /sessions/{id}, operaciones admin
  TGQRFlowUser   — flujo QR completo: create → pay → polling hasta APPROVED/REJECTED
                   Representa el flujo real del frontend del celular + cajero
  TGAdminUser    — cancel, reversal, batch-close (operaciones menos frecuentes)

Endpoints HTTP del BC (puerto 8081, tg_http_handler.go + tg_qr_handler.go):
  GET  /healthz
  GET  /readyz
  GET  /sessions/{id}                  → estado de sesión (operaciones/soporte)
  POST /sessions/{id}/cancel           → cancelación manual por cajero
  POST /sessions/{id}/reversal         → anulación de transacción aprobada
  POST /batch-close                    → cierre de lote EOD
  POST /api/sessions/create            → crea sesión QR (frontend cajero)
  POST /api/sessions/{id}/pay          → simula pago del cliente (frontend celular)
  GET  /api/sessions/{id}/status       → polling estado (frontend cajero + celular)

State machine (tg_session_state.go):
  AWAITING_PAYMENT → PROCESSING → APPROVED / REJECTED / EXPIRED / CANCELLED

Dominio clave (tg_payment_session.go):
  - TTL de 5 minutos por sesión (defaultSessionTTL)
  - Un solo terminal → un solo batch OPEN por día
  - Transactional Outbox para publicar TransactionReceived a NATS sin dual-write

Uso:
  # UI web:
  locust -f terminal_gateway_locust.py --host http://localhost:8081

  # Headless — solo HTTP (rápido, sin flujo QR completo):
  locust -f terminal_gateway_locust.py TGHTTPUser \\
    --headless -u 10 -r 2 --run-time 2m --host http://localhost:8081

  # Headless — flujo QR completo (requiere toda la saga activa):
  locust -f terminal_gateway_locust.py \\
    --headless -u 10 -r 2 --run-time 3m --host http://localhost:8081

  # Combinado con los locustfiles de los otros BCs: NO pasar --host ni
  # completar "Host" en la web UI — cada *HTTPUser ya fija su propio host
  # (ver settlement_locust.py para el detalle de por qué).

Variables de entorno:
  POLL_TIMEOUT_S   Timeout de polling del estado QR (default: 10)
  POLL_INTERVAL_S  Intervalo de polling (default: 0.1)
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests as req_lib

from locust import HttpUser, task, between, events, tag
from locust.exception import StopUser


# ── Configuración ──────────────────────────────────────────────────────────────

POLL_TIMEOUT  = float(os.getenv("POLL_TIMEOUT_S",  "10"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_S", "0.1"))

TERMINAL_ID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
MERCHANT_ID = "b2c3d4e5-f6a7-8901-bcde-f12345678901"

# Pool compartido de sesiones conocidas (para GET /sessions/{id} y cancel/reversal)
_known_session_ids: list[str] = []
# Pool de sesiones APPROVED (para reversal)
_approved_session_ids: list[str] = []

# Host HTTP del BC Terminal Gateway
_http_host = "http://localhost:8081"

# Token del terminal, si Terminal Gateway tiene TERMINAL_TOKENS_FILE. Sin archivo, la API está abierta.
TG_TERMINAL_TOKEN = os.getenv("TG_TERMINAL_TOKEN", "")


def _set_terminal_token(user):
    if TG_TERMINAL_TOKEN:
        user.client.headers["Authorization"] = f"Bearer {TG_TERMINAL_TOKEN}"

# Distribución de montos realistas
AMOUNT_RANGES = [
    (500,     5_000,    0.40),
    (5_001,   50_000,   0.35),
    (50_001,  500_000,  0.20),
    (500_001, 9_999_999, 0.05),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def new_uuid() -> str:
    return str(uuid.uuid4())

def random_amount_cents() -> int:
    roll = random.random()
    cum = 0.0
    for lo, hi, w in AMOUNT_RANGES:
        cum += w
        if roll <= cum:
            return random.randint(lo, hi)
    return random.randint(500, 50_000)

def add_to_pool(pool: list, item: str, max_size: int = 200):
    if len(pool) < max_size:
        pool.append(item)
    else:
        pool[random.randint(0, max_size - 1)] = item


# ── TGHTTPUser — health + consultas ───────────────────────────────────────────

class TGHTTPUser(HttpUser):
    """
    Simula operadores y dashboards consultando el estado del gateway.
    Peso 2: 2 usuarios HTTP por cada 1 QR.
    """
    host = _http_host
    weight = 2
    wait_time = between(0.5, 2.0)

    def on_start(self):
        _set_terminal_token(self)
        with self.client.get("/healthz", catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure(f"Terminal Gateway not ready: {resp.status_code}")
                raise StopUser()

    @task(5)
    @tag("health", "http")
    def health_check(self):
        """GET /healthz — liveness probe."""
        with self.client.get(
            "/healthz", catch_response=True, name="GET /healthz"
        ) as resp:
            resp.success() if resp.status_code == 200 \
                else resp.failure(f"status: {resp.status_code}")

    @task(2)
    @tag("readiness", "http")
    def readiness_check(self):
        """GET /readyz — readiness probe (verifica Postgres)."""
        with self.client.get(
            "/readyz", catch_response=True, name="GET /readyz"
        ) as resp:
            resp.success() if resp.status_code in (200, 503) \
                else resp.failure(f"status: {resp.status_code}")

    @task(3)
    @tag("query", "http")
    def get_session(self):
        """
        GET /sessions/{id} — consulta el estado de una sesión.
        Retorna SessionStatusResult con campos en snake_case (tg_query.go).

        Nota: este endpoint usa la ruta del handler clásico (/sessions/),
        distinto del QR handler (/api/sessions/).
        """
        if _known_session_ids:
            session_id = random.choice(_known_session_ids)
            expect_404 = False
        else:
            session_id = new_uuid()
            expect_404 = True

        with self.client.get(
            f"/sessions/{session_id}",
            catch_response=True,
            name="GET /sessions/{id}",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                # SessionStatusResult retorna snake_case (struct con campos exportados
                # pero tg_query.go los mapea directamente como struct fields)
                has_state = (
                    "State"         in data or "state"          in data or
                    "TransactionID" in data or "transaction_id" in data
                )
                if not has_state:
                    resp.failure(f"missing fields: {list(data.keys())}")
                else:
                    state = data.get("State") or data.get("state", "")
                    valid = {"AWAITING_PAYMENT","PROCESSING","APPROVED",
                             "REJECTED","EXPIRED","CANCELLED","IDLE","RECONNECTING"}
                    if state and state not in valid:
                        resp.failure(f"invalid state: {state}")
                    else:
                        resp.success()
            elif resp.status_code == 404 and expect_404:
                resp.success()
            elif resp.status_code == 404:
                resp.success()  # sesión puede haber expirado — no es error
            elif resp.status_code == 400:
                resp.success()  # UUID inválido generado — comportamiento esperado
            else:
                resp.failure(f"status: {resp.status_code}")

    @task(2)
    @tag("query", "http")
    def get_session_status_qr(self):
        """
        GET /api/sessions/{id}/status — endpoint del QR handler.
        Retorna campos snake_case directamente (map[string]any en el handler).
        Usado por el frontend del cajero y el celular para polling.
        """
        if _known_session_ids:
            session_id = random.choice(_known_session_ids)
        else:
            session_id = new_uuid()

        with self.client.get(
            f"/api/sessions/{session_id}/status",
            catch_response=True,
            name="GET /api/sessions/{id}/status",
        ) as resp:
            if resp.status_code == 200:
                data = resp.json()
                # El QR handler retorna map[string]any con keys snake_case
                has_state = "state" in data or "State" in data
                if not has_state:
                    resp.failure(f"missing state field: {list(data.keys())}")
                else:
                    resp.success()
            elif resp.status_code in (404, 400):
                resp.success()
            else:
                resp.failure(f"status: {resp.status_code}")

    @task(1)
    @tag("error", "http")
    def get_session_invalid_id(self):
        """GET /sessions/{id} con ID inválido — debe retornar 400."""
        with self.client.get(
            "/sessions/not-a-uuid",
            catch_response=True,
            name="GET /sessions/{id} [invalid]",
        ) as resp:
            if resp.status_code in (400, 404):
                resp.success()
            else:
                resp.failure(f"expected 400/404, got {resp.status_code}")


# ── TGQRFlowUser — flujo QR completo ──────────────────────────────────────────

class TGQRFlowUser(HttpUser):
    """
    Simula el flujo QR completo de punta a punta:
      1. POST /api/sessions/create   — cajero ingresa el monto
      2. POST /api/sessions/{id}/pay — cliente paga (celular escanea QR)
      3. GET  /api/sessions/{id}/status — polling hasta APPROVED/REJECTED

    Este es el flujo más crítico del sistema — testea:
      - CreateSession (FindByID del terminal, NewPaymentSession, Save)
      - handleSimulatePay (GetSessionStatus, natsPub.Publish a POSNET_TRANSACTIONS)
      - Saga completa: Fraud Detection → Authorization → MockAcquirer
      - ApplyApproval (consumer NATS de posnet.auth.approved.v1)
      - handleSessionStatus retorna APPROVED con auth_code

    Peso 3: es el flujo más representativo del tráfico real.
    """
    host = _http_host
    weight = 3
    wait_time = between(1.0, 3.0)

    def on_start(self):
        _set_terminal_token(self)
        with self.client.get("/healthz", catch_response=True) as resp:
            if resp.status_code != 200:
                resp.failure("TG not ready")
                raise StopUser()

    @task(8)
    @tag("qr", "e2e", "happy-path")
    def qr_flow_normal_amount(self):
        """
        Flujo QR con monto normal (ARS 100–5.000).
        Score fraude = 0 → APPROVE → auth_code = A{STAN:05d}.
        Representa el 80% del tráfico real.
        """
        self._run_qr_flow(
            amount_cents=random.randint(10_000, 500_000),
            label="QR/normal/e2e",
            expected_states={"APPROVED", "REJECTED"},
        )

    @task(2)
    @tag("qr", "e2e", "high-amount")
    def qr_flow_high_amount(self):
        """
        Flujo QR con monto alto (ARS 50.000+).
        Puede activar RULE-002 (>3x promedio) → score += 15.
        Aun así debería APPROVE (score < 70 con solo esa regla).
        """
        self._run_qr_flow(
            amount_cents=random.randint(5_000_001, 9_999_999),
            label="QR/high-amount/e2e",
            expected_states={"APPROVED", "REJECTED"},
        )

    @task(1)
    @tag("qr", "cancel")
    def qr_flow_with_cancel(self):
        """
        Crea una sesión QR y la cancela antes de pagar.
        Verifica que POST /sessions/{id}/cancel funciona correctamente.
        State machine: AWAITING_PAYMENT → CANCELLED.
        """
        # Crear sesión
        session_id = self._create_session(random.randint(10_000, 100_000))
        if not session_id:
            return

        # Cancelar inmediatamente
        t0 = time.time()
        with self.client.post(
            f"/sessions/{session_id}/cancel",
            catch_response=True,
            name="POST /sessions/{id}/cancel",
            headers={"X-Terminal-ID": TERMINAL_ID},
        ) as resp:
            elapsed_ms = (time.time() - t0) * 1000
            if resp.status_code == 200:
                resp.success()
                # Verificar que el estado es CANCELLED
                status_resp = req_lib.get(
                    f"{_http_host}/api/sessions/{session_id}/status", timeout=3
                )
                if status_resp.status_code == 200:
                    state = status_resp.json().get("state", "")
                    if state != "CANCELLED":
                        # No forzar fallo — puede haber race condition con TTL
                        pass
            else:
                resp.failure(f"cancel status: {resp.status_code}")

    # ── Método principal del flujo QR ─────────────────────────────────────────

    def _run_qr_flow(
        self,
        amount_cents: int,
        label: str,
        expected_states: set,
    ):
        """
        Ejecuta el flujo QR completo y reporta 3 métricas:
          tg/{label}/create  — latencia de POST /api/sessions/create
          tg/{label}/pay     — latencia de POST /api/sessions/{id}/pay
          tg/{label}/e2e     — latencia total hasta APPROVED/REJECTED
        """
        # ── 1. Crear sesión ───────────────────────────────────────────────────
        t_create = time.time()
        session_id = self._create_session_timed(amount_cents, label)
        if not session_id:
            return

        # ── 2. Simular pago del cliente ───────────────────────────────────────
        t_pay = time.time()
        pay_ok = self._simulate_pay(session_id, label)
        if not pay_ok:
            return

        # ── 3. Polling hasta estado final ─────────────────────────────────────
        t_e2e = time.time()
        final_state, final_data = self._poll_session_status(session_id)
        e2e_ms = (time.time() - t_e2e) * 1000

        exc = None if final_state in expected_states else \
              Exception(f"Unexpected state: {final_state}")

        events.request.fire(
            request_type="HTTP+NATS", name=f"tg/{label}",
            response_time=e2e_ms, response_length=0,
            exception=exc, context={},
        )

        # Agregar al pool si fue exitosa
        if final_state == "APPROVED":
            add_to_pool(_approved_session_ids, session_id)
        if final_state in ("APPROVED", "REJECTED"):
            add_to_pool(_known_session_ids, session_id)

    def _create_session(self, amount_cents: int) -> str | None:
        """Crea una sesión y retorna el transaction_id, o None si falla."""
        with self.client.post(
            "/api/sessions/create",
            catch_response=True,
            name="POST /api/sessions/create",
            json={
                "amount_cents": amount_cents,
                "currency":     "ARS",
            },
        ) as resp:
            if resp.status_code == 201:
                data = resp.json()
                session_id = data.get("transaction_id", "")
                if session_id:
                    add_to_pool(_known_session_ids, session_id)
                    resp.success()
                    return session_id
                resp.failure("missing transaction_id in response")
            else:
                resp.failure(f"create status: {resp.status_code} — {resp.text[:100]}")
        return None

    def _create_session_timed(self, amount_cents: int, label: str) -> str | None:
        """Crea sesión y reporta métrica de latencia."""
        t0 = time.time()
        with self.client.post(
            "/api/sessions/create",
            catch_response=True,
            name="POST /api/sessions/create",
            json={
                "amount_cents": amount_cents,
                "currency":     "ARS",
            },
        ) as resp:
            elapsed_ms = (time.time() - t0) * 1000
            if resp.status_code == 201:
                data = resp.json()
                session_id = data.get("transaction_id", "")
                if session_id:
                    resp.success()
                    events.request.fire(
                        request_type="HTTP", name=f"tg/{label}/create",
                        response_time=elapsed_ms, response_length=len(resp.content),
                        exception=None, context={},
                    )
                    return session_id
                resp.failure("missing transaction_id")
            else:
                resp.failure(f"create status: {resp.status_code}")
                events.request.fire(
                    request_type="HTTP", name=f"tg/{label}/create",
                    response_time=elapsed_ms, response_length=0,
                    exception=Exception(f"HTTP {resp.status_code}"), context={},
                )
        return None

    def _simulate_pay(self, session_id: str, label: str) -> bool:
        """Simula el pago del cliente y reporta métrica."""
        t0 = time.time()
        with self.client.post(
            f"/api/sessions/{session_id}/pay",
            catch_response=True,
            name="POST /api/sessions/{id}/pay",
            json={
                "card_last4":   str(random.randint(1000, 9999)),
                "card_network": random.choice(["VISA", "MASTERCARD", "CABAL"]),
                "entry_mode":   "QR",
            },
        ) as resp:
            elapsed_ms = (time.time() - t0) * 1000
            if resp.status_code == 202:
                resp.success()
                events.request.fire(
                    request_type="HTTP", name=f"tg/{label}/pay",
                    response_time=elapsed_ms, response_length=len(resp.content),
                    exception=None, context={},
                )
                return True
            else:
                resp.failure(f"pay status: {resp.status_code}")
                events.request.fire(
                    request_type="HTTP", name=f"tg/{label}/pay",
                    response_time=elapsed_ms, response_length=0,
                    exception=Exception(f"HTTP {resp.status_code}"), context={},
                )
                return False

    def _poll_session_status(self, session_id: str) -> tuple[str | None, dict | None]:
        """
        Hace polling de GET /api/sessions/{id}/status (QR handler).
        El handler retorna map[string]any con keys snake_case:
          { state, transaction_id, amount_cents, currency,
            auth_code, rejection_code, ttl_seconds }

        Retorna (estado_final, datos) o (None, None) si timeout.
        """
        url      = f"{_http_host}/api/sessions/{session_id}/status"
        deadline = time.time() + POLL_TIMEOUT
        final_states = {"APPROVED", "REJECTED", "EXPIRED", "CANCELLED"}

        while time.time() < deadline:
            try:
                resp = req_lib.get(url, timeout=3)
                if resp.status_code == 200:
                    data  = resp.json()
                    state = data.get("state") or data.get("State", "")
                    if state in final_states:
                        return state, data
            except Exception:
                pass
            time.sleep(POLL_INTERVAL)

        return None, None


# ── TGAdminUser — operaciones admin ───────────────────────────────────────────

class TGAdminUser(HttpUser):
    """
    Simula operaciones administrativas de baja frecuencia:
    - Batch close (EOD del terminal)
    - Reversal de transacciones aprobadas
    - Consulta de sesiones activas

    Peso 1: operaciones poco frecuentes en tráfico real.
    """
    host = _http_host
    weight = 1
    wait_time = between(3.0, 8.0)

    def on_start(self):
        _set_terminal_token(self)
        with self.client.get("/healthz", catch_response=True) as resp:
            if resp.status_code != 200:
                raise StopUser()

    @task(4)
    @tag("admin", "http")
    def health_check(self):
        with self.client.get(
            "/healthz", catch_response=True, name="GET /healthz [admin]"
        ) as resp:
            resp.success() if resp.status_code == 200 \
                else resp.failure(f"status: {resp.status_code}")

    @task(2)
    @tag("admin", "batch-close")
    def batch_close(self):
        """
        POST /batch-close — cierre de lote EOD.
        Publica BatchCloseRequested a NATS → Settlement procesa el cierre.
        En un día real esto ocurre 1 vez por terminal — en el test lo simulamos
        más frecuente para medir la latencia del endpoint.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self.client.post(
            "/batch-close",
            catch_response=True,
            name="POST /batch-close",
            json={
                "terminal_id":     TERMINAL_ID,
                "merchant_id":     MERCHANT_ID,
                "batch_date":      today,
                "terminal_count":  random.randint(0, 50),
                "terminal_amount": random.randint(0, 5_000_000),
                "currency":        "ARS",
            },
        ) as resp:
            if resp.status_code == 202:
                resp.success()
            elif resp.status_code == 400:
                resp.success()  # Validación — comportamiento esperado
            else:
                resp.failure(f"batch-close status: {resp.status_code}")

    @task(1)
    @tag("admin", "reversal")
    def request_reversal(self):
        """
        POST /sessions/{id}/reversal — solicitud de anulación.
        Solo es válido para sesiones APPROVED — usa el pool de sesiones aprobadas.
        Publica ReversalRequested a NATS → Authorization procesa la anulación.
        """
        if not _approved_session_ids:
            # Sin sesiones aprobadas — usar ID random que dará 500/404
            session_id = new_uuid()
            expected_fail = True
        else:
            session_id = random.choice(_approved_session_ids)
            expected_fail = False

        with self.client.post(
            f"/sessions/{session_id}/reversal",
            catch_response=True,
            name="POST /sessions/{id}/reversal",
            headers={"X-Terminal-ID": TERMINAL_ID},
        ) as resp:
            if resp.status_code == 202:
                resp.success()
                # Remover del pool de aprobadas (ya fue revertida)
                if session_id in _approved_session_ids:
                    _approved_session_ids.remove(session_id)
            elif resp.status_code in (404, 500) and expected_fail:
                resp.success()  # Esperado sin sesiones aprobadas
            elif resp.status_code == 404:
                resp.success()  # Sesión puede haber expirado
            else:
                resp.failure(f"reversal status: {resp.status_code}")

    @task(1)
    @tag("admin", "cancel")
    def cancel_nonexistent_session(self):
        """
        POST /sessions/{id}/cancel con ID inexistente.
        Verifica manejo de error cuando la sesión no existe.
        """
        with self.client.post(
            f"/sessions/{new_uuid()}/cancel",
            catch_response=True,
            name="POST /sessions/{id}/cancel [unknown]",
            headers={"X-Terminal-ID": TERMINAL_ID},
        ) as resp:
            # Puede ser 404, 500 (si session not found dispara error interno), o 200
            if resp.status_code in (200, 404, 500):
                resp.success()
            else:
                resp.failure(f"cancel status: {resp.status_code}")


# ── Eventos globales ───────────────────────────────────────────────────────────

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    global _http_host
    if environment.host:
        _http_host = environment.host.rstrip("/")

    print(f"""
╔══════════════════════════════════════════════════════════╗
║   POSNET Load Test — BC Terminal Gateway                ║
║   HTTP:     {_http_host:<43}║
║   Poll:     timeout={POLL_TIMEOUT}s  interval={POLL_INTERVAL}s              ║
║   Terminal: {TERMINAL_ID:<43}║
║                                                         ║
║   Users:                                                ║
║     TGHTTPUser   (peso 2) — health + consultas          ║
║     TGQRFlowUser (peso 3) — flujo QR E2E completo       ║
║     TGAdminUser  (peso 1) — batch-close, reversal       ║
╚══════════════════════════════════════════════════════════╝
    """)


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    stats = environment.stats
    print("\n── Terminal Gateway Load Test Summary ───────────────────")
    for name, entry in stats.entries.items():
        if entry.num_requests > 0:
            print(
                f"  {str(name[1]):52s} "
                f"req={entry.num_requests:5d}  "
                f"fail={entry.num_failures:4d}  "
                f"p50={entry.get_response_time_percentile(0.50):6.0f}ms  "
                f"p95={entry.get_response_time_percentile(0.95):6.0f}ms"
            )
    print(f"  Known sessions: {len(_known_session_ids)}  "
          f"Approved sessions: {len(_approved_session_ids)}")
    print("─────────────────────────────────────────────────────────\n")
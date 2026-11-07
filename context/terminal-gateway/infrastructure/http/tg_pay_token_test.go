package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/pkg/domain"
)

var (
	payKey      = []byte("0123456789abcdef0123456789abcdef")
	previousKey = []byte("fedcba9876543210fedcba9876543210")
)

func TestPayTokens_IssueAndVerify(t *testing.T) {
	p := NewPayTokens(payKey, nil, true)
	id := domain.NewTransactionID().String()
	token := p.Issue(id)

	if !p.valid(id, token) {
		t.Fatal("el token emitido no es válido")
	}
	if p.valid(domain.NewTransactionID().String(), token) {
		t.Error("el token de una sesión vale para otra")
	}
	if NewPayTokens(previousKey, nil, true).valid(id, token) {
		t.Error("un token firmado con otra clave es válido")
	}
	if strings.ContainsAny(token, "+/=") {
		t.Errorf("token = %q: tiene que ser seguro en una URL", token)
	}

	// Rotación: con la clave anterior configurada, los QR ya emitidos siguen valiendo.
	rotated := NewPayTokens(previousKey, payKey, true)
	if !rotated.valid(id, token) {
		t.Error("después de rotar, el token emitido con la clave anterior dejó de valer")
	}
	if rotated.Issue(id) == token {
		t.Error("después de rotar, se sigue firmando con la clave anterior")
	}
}

func payRouter(t *testing.T, svc *fakeSessionService, p *PayTokens) http.Handler {
	t.Helper()
	repo := &fakeSessionRepo{findResult: newSession(t)}
	return NewRouter(svc, query.NewSessionQueryHandler(repo), &fakePool{}, nil, nil, p)
}

func TestCreate_ReturnsTheTokenInTheQRFragment(t *testing.T) {
	svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
	p := NewPayTokens(payKey, nil, true)
	rec := httptest.NewRecorder()
	payRouter(t, svc, p).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader(`{"amount_cents":1000}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	var body struct {
		TransactionID string `json:"transaction_id"`
		PayToken      string `json:"pay_token"`
		QRContent     string `json:"qr_content"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if body.PayToken != p.Issue(body.TransactionID) {
		t.Errorf("pay_token = %q, want el de la sesión", body.PayToken)
	}
	qr, err := url.Parse(body.QRContent)
	if err != nil {
		t.Fatalf("qr_content = %q: %v", body.QRContent, err)
	}
	if qr.Fragment != "t="+body.PayToken || qr.RawQuery != "" {
		t.Errorf("qr_content = %q, want el token en el fragmento y no en la query", body.QRContent)
	}
}

func TestPayAndStatus_SessionToken(t *testing.T) {
	session := newSession(t)
	id := session.ID().String()
	valid := NewPayTokens(payKey, nil, true).Issue(id)
	other := NewPayTokens(payKey, nil, true).Issue(domain.NewTransactionID().String())

	cases := []struct {
		name     string
		required bool
		token    string
		want     int
		wantCode string
	}{
		{"exigido y válido", true, valid, 0, ""},
		{"exigido y ausente", true, "", http.StatusUnauthorized, "SESSION_TOKEN_REQUIRED"},
		{"exigido y de otra sesión", true, other, http.StatusUnauthorized, "INVALID_SESSION_TOKEN"},
		{"sin exigir y ausente", false, "", 0, ""},
		{"sin exigir e inválido", false, "x", http.StatusUnauthorized, "INVALID_SESSION_TOKEN"},
	}
	for _, tc := range cases {
		for _, ep := range []struct{ method, path string }{
			{http.MethodPost, "/api/sessions/" + id + "/pay"},
			{http.MethodGet, "/api/sessions/" + id + "/status"},
		} {
			t.Run(tc.name+" "+ep.method, func(t *testing.T) {
				svc := &fakeSessionService{}
				repo := &fakeSessionRepo{findResult: session}
				router := NewRouter(svc, query.NewSessionQueryHandler(repo), &fakePool{}, nil, nil, NewPayTokens(payKey, nil, tc.required))
				req := httptest.NewRequest(ep.method, ep.path, strings.NewReader(`{"card_last4":"4242"}`))
				if tc.token != "" {
					req.Header.Set(SessionTokenHeader, tc.token)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)

				if tc.want == 0 {
					if rec.Code == http.StatusUnauthorized {
						t.Fatalf("status = 401, body = %s, want aceptado", rec.Body)
					}
					return
				}
				if rec.Code != tc.want || !strings.Contains(rec.Body.String(), tc.wantCode) {
					t.Errorf("status = %d, body = %s, want %d %s", rec.Code, rec.Body, tc.want, tc.wantCode)
				}
				if len(svc.processPaymentCalls) != 0 {
					t.Error("un pedido rechazado empezó el pago")
				}
			})
		}
	}
}

func TestCORS_AllowsTheSessionTokenHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/api/sessions/x/pay", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	corsMiddleware([]string{"http://localhost:5173"}, okHandler()).ServeHTTP(rec, req)
	if !strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), SessionTokenHeader) {
		t.Errorf("Access-Control-Allow-Headers = %q", rec.Header().Get("Access-Control-Allow-Headers"))
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

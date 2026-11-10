package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serveHealth(t *testing.T, h *Health, path string) (*httptest.ResponseRecorder, healthResponse) {
	t.Helper()
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))

	var body healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	return rec, body
}

func TestLiveIgnoresFailingChecks(t *testing.T) {
	h := NewHealth()
	h.Add("db", CheckerFunc(func(context.Context) error { return errors.New("down") }))

	rec, body := serveHealth(t, h, "/livez")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body.Status != "ok" {
		t.Errorf("body status = %q, want ok", body.Status)
	}
}

func TestReady(t *testing.T) {
	ok := CheckerFunc(func(context.Context) error { return nil })
	down := CheckerFunc(func(context.Context) error { return errors.New("connection refused") })

	tests := []struct {
		name       string
		checks     map[string]Checker
		wantCode   int
		wantStatus string
		wantChecks map[string]string
	}{
		{
			name:       "no checks",
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{},
		},
		{
			name:       "all checks pass",
			checks:     map[string]Checker{"db": ok, "kafka": ok},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
			wantChecks: map[string]string{"db": "ok", "kafka": "ok"},
		},
		{
			name:       "one check fails",
			checks:     map[string]Checker{"db": ok, "kafka": down},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: "unavailable",
			wantChecks: map[string]string{"db": "ok", "kafka": "connection refused"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHealth()
			for name, c := range tt.checks {
				h.Add(name, c)
			}

			rec, body := serveHealth(t, h, "/readyz")

			if rec.Code != tt.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if body.Status != tt.wantStatus {
				t.Errorf("body status = %q, want %q", body.Status, tt.wantStatus)
			}
			if len(body.Checks) != len(tt.wantChecks) {
				t.Errorf("checks = %v, want %v", body.Checks, tt.wantChecks)
			}
			for name, want := range tt.wantChecks {
				if got := body.Checks[name]; got != want {
					t.Errorf("check %q = %q, want %q", name, got, want)
				}
			}
		})
	}
}

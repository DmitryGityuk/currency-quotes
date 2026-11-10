package httpapi

import (
	"context"
	"net/http"
	"time"
)

// Checker reports whether a dependency such as the database or Kafka can
// be used right now.
type Checker interface {
	Check(ctx context.Context) error
}

// CheckerFunc adapts an ordinary function to the Checker interface.
type CheckerFunc func(ctx context.Context) error

// Check calls f(ctx).
func (f CheckerFunc) Check(ctx context.Context) error { return f(ctx) }

// Health serves the liveness and readiness probes.
//
// /livez answers "is the process alive?" and never touches dependencies:
// otherwise a database outage would make the orchestrator restart healthy
// instances and turn a partial outage into a full one.
//
// /readyz answers "can this instance take traffic?" and runs every check.
type Health struct {
	checks  map[string]Checker
	timeout time.Duration
}

// NewHealth returns a Health with no readiness checks.
func NewHealth() *Health {
	return &Health{checks: make(map[string]Checker), timeout: 2 * time.Second}
}

// Add registers a readiness check. Call it before the server starts.
func (h *Health) Add(name string, c Checker) {
	h.checks[name] = c
}

// Register mounts /livez and /readyz on mux.
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /livez", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func (h *Health) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (h *Health) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: make(map[string]string, len(h.checks))}
	code := http.StatusOK
	for name, c := range h.checks {
		if err := c.Check(ctx); err != nil {
			resp.Checks[name] = err.Error()
			resp.Status = "unavailable"
			code = http.StatusServiceUnavailable
			continue
		}
		resp.Checks[name] = "ok"
	}
	writeJSON(w, code, resp)
}

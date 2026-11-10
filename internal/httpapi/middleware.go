package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"time"
	"uuid"
)

// Middleware wraps an http.Handler with extra behaviour.
type Middleware func(http.Handler) http.Handler

// Chain wraps h so that the first middleware is the outermost one:
// Chain(h, a, b) handles a request as a → b → h.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for _, mw := range slices.Backward(mws) {
		h = mw(h)
	}
	return h
}

// RequestIDHeader carries the request ID in requests and responses.
const RequestIDHeader = "X-Request-ID"

type requestIDKey struct{}

// RequestID takes the request ID from the X-Request-ID header or generates
// a new one, stores it in the request context and echoes it in the response.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = uuid.New().String()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFrom returns the request ID stored by RequestID, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// validRequestID accepts a client-supplied ID only if it is short and
// printable ASCII, so a client cannot put newlines or megabytes into logs.
func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		if id[i] < '!' || id[i] > '~' {
			return false
		}
	}
	return true
}

// Logging writes one log record per request. Health probes and metric
// scrapes are logged at debug level: they come every few seconds.
func Logging(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			level := slog.LevelInfo
			if r.URL.Path == "/livez" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
				level = slog.LevelDebug
			}
			logger.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000),
				slog.String("request_id", RequestIDFrom(r.Context())),
			)
		})
	}
}

// HTTPObserver receives one call per request, for metrics.
type HTTPObserver interface {
	ObserveHTTP(method, route string, status int, took time.Duration)
}

// Metrics reports every request to o. Put it last in Chain, right above the
// mux: the mux fills r.Pattern on the request it gets, and labelling by route
// pattern ("/api/v1/quotes/updates/{id}") instead of the raw path keeps the
// number of time series small.
func Metrics(o HTTPObserver) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			o.ObserveHTTP(r.Method, routeOf(r), rec.status, time.Since(start))
		})
	}
}

// routeOf returns the matched pattern without its method, or "unmatched".
func routeOf(r *http.Request) string {
	route := r.Pattern
	if _, path, found := strings.Cut(route, " "); found {
		route = path
	}
	if route == "" {
		return "unmatched"
	}
	return route
}

// statusRecorder remembers the status code written by the next handler.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true // an implicit 200
	return r.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the original writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Recover turns a panic in a handler into a 500 response and a log record
// with the stack trace, so one bad request does not crash the process. If
// the handler had already started the response, the status line is sent and
// cannot be changed, so only the log record is written.
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			defer func() { //nolint:contextcheck // the closure takes its context from r
				p := recover()
				if p == nil {
					return
				}
				// net/http uses this panic on purpose to abort a response.
				if err, ok := p.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(p)
				}
				logger.ErrorContext(r.Context(), "panic in http handler",
					slog.Any("panic", p),
					slog.String("stack", string(debug.Stack())),
					slog.String("request_id", RequestIDFrom(r.Context())),
				)
				if !rec.wroteHeader {
					writeProblem(w, r, http.StatusInternalServerError, codeInternal, "")
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

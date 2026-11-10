package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDocs(t *testing.T) {
	spec := []byte("openapi: 3.1.0\n")
	mux := http.NewServeMux()
	RegisterDocs(mux, spec)

	tests := []struct {
		name        string
		path        string
		wantStatus  int
		wantType    string
		wantBody    string
		wantHeaders map[string]string
	}{
		{
			name:       "specification",
			path:       "/openapi.yaml",
			wantStatus: http.StatusOK,
			wantType:   "application/yaml",
			wantBody:   string(spec),
		},
		{
			name:       "swagger ui loads the specification",
			path:       "/docs",
			wantStatus: http.StatusOK,
			wantType:   "text/html; charset=utf-8",
			wantBody:   `url: "/openapi.yaml"`,
		},
		{
			name:        "root redirects to docs",
			path:        "/",
			wantStatus:  http.StatusFound,
			wantHeaders: map[string]string{"Location": "/docs"},
		},
		{
			name:       "unknown path is not redirected",
			path:       "/nope",
			wantStatus: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, http.NoBody))

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantType != "" {
				if got := rec.Header().Get("Content-Type"); got != tt.wantType {
					t.Errorf("Content-Type = %q, want %q", got, tt.wantType)
				}
			}
			if !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Errorf("body does not contain %q:\n%s", tt.wantBody, rec.Body.String())
			}
			for k, want := range tt.wantHeaders {
				if got := rec.Header().Get(k); got != want {
					t.Errorf("%s = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestDocsPagePinsSwaggerUIVersion(t *testing.T) {
	if strings.Contains(docsPage, "{{") {
		t.Fatal("docs page still contains a placeholder")
	}
	for _, hash := range []string{swaggerUICSSHash, swaggerUIJSHash} {
		if !strings.Contains(docsPage, `integrity="`+hash+`"`) {
			t.Errorf("docs page does not pin %s", hash)
		}
	}
	if got := strings.Count(docsPage, "swagger-ui-dist@"+swaggerUIVersion+"/"); got != 2 {
		t.Errorf("docs page references swagger-ui-dist@%s %d times, want 2 (css and js)", swaggerUIVersion, got)
	}
}

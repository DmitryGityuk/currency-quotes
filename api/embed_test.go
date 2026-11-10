package api

import (
	"bytes"
	"testing"
)

func TestOpenAPIIsEmbedded(t *testing.T) {
	if !bytes.HasPrefix(OpenAPI, []byte("openapi: 3.1.")) {
		t.Fatalf("embedded specification does not start with the OpenAPI version line: %.40q", OpenAPI)
	}
	// Every endpoint the service promises must be in the contract.
	for _, path := range []string{
		"/api/v1/quotes/updates:",
		"/api/v1/quotes/updates/{id}:",
		"/api/v1/quotes/latest:",
		"/livez:",
		"/readyz:",
		"/metrics:",
	} {
		if !bytes.Contains(OpenAPI, []byte("\n  "+path+"\n")) {
			t.Errorf("specification has no path %s", path)
		}
	}
}

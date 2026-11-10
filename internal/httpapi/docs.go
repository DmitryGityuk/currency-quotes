package httpapi

import (
	"io"
	"net/http"
	"strings"
)

// swaggerUIVersion pins the Swagger UI build that /docs loads from the CDN.
// The integrity hashes make the browser refuse the files if the CDN ever
// serves something else; update them together with the version.
const (
	swaggerUIVersion = "5.21.0"
	swaggerUICSSHash = "sha384-WoOxtFhjrhn23jYeguEcSJkYdgSIer0UxZkoMKKEqROW+TDEmHEPwckfxWmZXSIw"
	swaggerUIJSHash  = "sha384-sVLSl7HyCV1nd7RZmv/iLgSAiKQD9VfnzE0//SWrbZUtoVy2sPhQuAHF5hNCpDp7"
)

// docsPage loads Swagger UI and points it at /openapi.yaml. The page comes
// from the API itself, so "Try it out" calls the same origin and needs no
// CORS, whatever host port the API is published on.
var docsPage = strings.NewReplacer(
	"{{version}}", swaggerUIVersion,
	"{{css_hash}}", swaggerUICSSHash,
	"{{js_hash}}", swaggerUIJSHash,
).Replace(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Currency Quotes API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{{version}}/swagger-ui.css" integrity="{{css_hash}}" crossorigin="anonymous">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{{version}}/swagger-ui-bundle.js" integrity="{{js_hash}}" crossorigin="anonymous"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/openapi.yaml",
      dom_id: "#swagger-ui",
      deepLinking: true,
      tryItOutEnabled: true,
      displayRequestDuration: true
    });
  </script>
</body>
</html>
`)

// RegisterDocs mounts the OpenAPI specification at /openapi.yaml, a Swagger
// UI page for it at /docs, and a redirect from / to /docs.
func RegisterDocs(mux *http.ServeMux, spec []byte) {
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		// application/yaml is registered by RFC 9512.
		w.Header().Set("Content-Type", "application/yaml")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(spec)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, docsPage)
	})
	// {$} matches only "/" itself, so unknown paths still get 404.
	mux.Handle("GET /{$}", http.RedirectHandler("/docs", http.StatusFound))
}

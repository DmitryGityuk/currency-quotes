// Package api holds the OpenAPI specification of the service. The file is
// the source of truth for the HTTP contract; it is embedded into the binary
// and served at /openapi.yaml.
package api

import _ "embed"

// OpenAPI is the contents of openapi.yaml.
//
//go:embed openapi.yaml
var OpenAPI []byte

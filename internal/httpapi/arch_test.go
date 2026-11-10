package httpapi

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/DmitryGityuk/currency-quotes"

// The assignment requires that the request handler does not perform the
// update. This test makes it structural: the HTTP package must not import,
// even indirectly, anything that fetches rates or talks to Kafka.
func TestAPIDoesNotFetchRates(t *testing.T) {
	forbidden := []string{
		module + "/internal/rates",
		module + "/internal/worker",
		"github.com/twmb/franz-go",
	}
	root := filepath.Join("..", "..")

	seen := map[string]bool{}
	var visit func(pkg string, chain []string)
	visit = func(pkg string, chain []string) {
		if seen[pkg] {
			return
		}
		seen[pkg] = true
		for _, f := range forbidden {
			if pkg == f || strings.HasPrefix(pkg, f+"/") {
				t.Errorf("httpapi depends on %s via %s", pkg, strings.Join(chain, " -> "))
			}
		}
		if !strings.HasPrefix(pkg, module+"/") {
			return // only the module's own packages are followed
		}
		for _, imp := range packageImports(t, filepath.Join(root, strings.TrimPrefix(pkg, module+"/"))) {
			visit(imp, append(chain, imp))
		}
	}
	visit(module+"/internal/httpapi", []string{"httpapi"})

	if !seen[module+"/internal/quotes"] {
		t.Error("the walk did not reach internal/quotes; the test is not looking where it should")
	}
}

// packageImports returns the imports of the non-test Go files in dir.
func packageImports(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var imports []string
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			imports = append(imports, path)
		}
	}
	return imports
}

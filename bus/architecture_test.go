// SPDX-License-Identifier: GPL-3.0-only

package bus_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	rootModule = "github.com/antst/sessionbus"
	sdkModule  = rootModule + "/bus/sdk/go"
)

func TestRetainedTreeMatchesRepositoryBoundary(t *testing.T) {
	repository := filepath.Clean("..")
	for _, path := range []string{"wrappers", "cmd", "docs/products", "integrations", "internal", "scripts", "skills", "examples", ".agents", ".claude-plugin", ".codex-plugin"} {
		if _, err := os.Stat(filepath.Join(repository, filepath.FromSlash(path))); !os.IsNotExist(err) {
			t.Errorf("removed peers or legacy path remains: %s", path)
		}
	}
	for _, path := range []string{
		"bus/cmd/sessionbus", "bus/cmd/sessionbus-hub", "bus/cmd/sessionbus-call", "bus/cmd/example-peer",
		"bus/internal/conn", "bus/internal/daemon", "bus/internal/federation", "bus/internal/structuredprocess",
		"bus/sdk/go/protocol", "bus/sdk/go/internal/rpc", "bus/sdk/go/internal/stateroot", "bus/sdk/go/socketpath", "bus/sdk/js",
		"deploy/sessionbus", "deploy/sessionbus-hub", "docs/designs",
	} {
		if info, err := os.Stat(filepath.Join(repository, filepath.FromSlash(path))); err != nil || !info.IsDir() {
			t.Errorf("retained path %s: %v", path, err)
		}
	}
}

func TestModuleAndImportBoundaries(t *testing.T) {
	repository := filepath.Clean("..")
	rootMod := read(t, filepath.Join(repository, "go.mod"))
	if !bytes.Contains(rootMod, []byte("module "+rootModule)) || !bytes.Contains(rootMod, []byte(sdkModule+" v0.1.0-pre.2")) || !bytes.Contains(rootMod, []byte("replace "+sdkModule+" => ./bus/sdk/go")) || bytes.Count(rootMod, []byte("replace ")) != 1 {
		t.Fatalf("root go.mod violates split shape:\n%s", rootMod)
	}
	if _, err := os.Stat(filepath.Join(repository, "go.work")); !os.IsNotExist(err) {
		t.Fatalf("go.work must not be committed: %v", err)
	}
	sdkMod := read(t, filepath.Join(repository, "bus", "sdk", "go", "go.mod"))
	if !bytes.Contains(sdkMod, []byte("module "+sdkModule)) || bytes.Contains(sdkMod, []byte("replace ")) {
		t.Fatalf("SDK go.mod violates split shape:\n%s", sdkMod)
	}
	checkGoImports(t, filepath.Join(repository, "bus"), func(path, imported string) {
		inSDK := strings.Contains(filepath.ToSlash(path), "/bus/sdk/go/")
		if inSDK && strings.HasPrefix(imported, rootModule) && !strings.HasPrefix(imported, sdkModule) {
			t.Errorf("SDK %s imports daemon module package %s", path, imported)
		}
		if !inSDK && strings.HasPrefix(imported, rootModule+"/wrappers/") {
			t.Errorf("daemon %s imports peers package %s", path, imported)
		}
	})
	command := exec.Command("go", "list", "-m", "all")
	command.Dir = filepath.Join(repository, "bus", "sdk", "go")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("independent SDK graph: %v\n%s", err, output)
	}
	for _, line := range bytes.Split(bytes.TrimSpace(output), []byte{'\n'}) {
		if bytes.Equal(line, []byte(rootModule)) {
			t.Fatalf("SDK graph contains daemon module:\n%s", output)
		}
	}
}

func TestSDKFixturesAndPackageAllowlistUseProtocolAuthority(t *testing.T) {
	repository := filepath.Clean("..")
	schemaPath := filepath.Join(repository, "bus", "sdk", "go", "protocol", "session.schema.json")
	schema := read(t, schemaPath)
	var manifest struct {
		Files []string `json:"files"`
	}
	if err := json.Unmarshal(read(t, filepath.Join(repository, "bus", "package.json")), &manifest); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"sdk/js/index.js": true, "sdk/js/connection.js": true, "sdk/js/caller.js": true,
		"sdk/js/schema.js": true, "sdk/js/LICENSE": true, "sdk/go/protocol/session.schema.json": true,
		"sdk/js/protocol.d.ts": true, "sdk/js/TYPES.md": true,
	}
	if len(manifest.Files) != len(want) {
		t.Fatalf("npm files = %#v", manifest.Files)
	}
	for _, path := range manifest.Files {
		if !want[path] {
			t.Errorf("npm package includes non-SDK path %q", path)
		}
	}
	jsSchema := read(t, filepath.Join(repository, "bus", "sdk", "js", "schema.js"))
	if !bytes.Contains(jsSchema, []byte(`require("../go/protocol/session.schema.json")`)) {
		t.Fatal("JavaScript SDK does not load the canonical schema")
	}
	var decoded map[string]any
	if json.Unmarshal(schema, &decoded) != nil || decoded["$id"] != "urn:sessionbus:session:v1" {
		t.Fatal("canonical schema is not valid Sessionbus schema JSON")
	}
}

func TestSPDXAndLicenseBoundaries(t *testing.T) {
	repository := filepath.Clean("..")
	walkSources(t, filepath.Join(repository, "bus", "sdk", "go"), ".go", "// SPDX-License-Identifier: MIT")
	walkSources(t, filepath.Join(repository, "bus", "sdk", "js"), ".js", "// SPDX-License-Identifier: MIT")
	err := filepath.WalkDir(filepath.Join(repository, "bus"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.Contains(filepath.ToSlash(path), "/sdk/go/") {
			return err
		}
		if firstLine(read(t, path)) != "// SPDX-License-Identifier: GPL-3.0-only" {
			t.Errorf("daemon source lacks GPL SPDX header: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(read(t, filepath.Join(repository, "LICENSE")), []byte("GNU GENERAL PUBLIC LICENSE")) {
		t.Fatal("root GPL-3.0-only license missing")
	}
	for _, path := range []string{"bus/sdk/go/LICENSE", "bus/sdk/js/LICENSE"} {
		if !bytes.Contains(read(t, filepath.Join(repository, filepath.FromSlash(path))), []byte("MIT License")) {
			t.Errorf("SDK MIT license missing: %s", path)
		}
	}
}

func TestGeneratedProtocolMatchesSignedDesign(t *testing.T) {
	repository := filepath.Clean("..")
	design := string(read(t, filepath.Join(repository, "docs", "designs", "UNIVERSAL-SESSION-PROTOCOL.md")))
	wire := section(t, design, "## 1. Wire\n", "## 2. Daemon\n")
	kit := section(t, design, "### 3.1 Product contract\n", "### 3.2 Full-duplex lifecycle\n")
	want := wire + strings.TrimSuffix(kit, "\n")
	got := read(t, filepath.Join(repository, "bus", "docs", "PROTOCOL.md"))
	if string(got) != want {
		t.Fatal("bus/docs/PROTOCOL.md drifted from the signed design")
	}
}

func TestBusSourceContainsNoProductNames(t *testing.T) {
	forbidden := []string{"claude", "codex", "grok", "qwen", "dashi", "dsh", "opencode", "kilo", "pi", "omp"}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || path == "architecture_test.go" || filepath.Ext(path) == ".md" {
			return err
		}
		lower := bytes.ToLower(read(t, path))
		for _, product := range forbidden {
			if containsToken(lower, []byte(product)) {
				t.Errorf("%s contains product token %q", path, product)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDaemonDoesNotReferenceAdapterLaneDirectory(t *testing.T) {
	for _, root := range []string{"internal/daemon", "cmd/sessionbus"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return err
			}
			positions := token.NewFileSet()
			file, err := parser.ParseFile(positions, path, nil, 0)
			if err != nil {
				return err
			}
			socketpathImports := map[string]bool{}
			for _, imported := range file.Imports {
				importPath, err := strconv.Unquote(imported.Path.Value)
				if err != nil {
					return err
				}
				if importPath == sdkModule+"/socketpath" {
					name := "socketpath"
					if imported.Name != nil {
						name = imported.Name.Name
					}
					socketpathImports[name] = true
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selected, ok := call.Fun.(*ast.SelectorExpr); ok {
						if qualifier, ok := selected.X.(*ast.Ident); ok && socketpathImports[qualifier.Name] && (selected.Sel.Name == "Lane" || selected.Sel.Name == "Prepare") {
							t.Errorf("%s calls adapter lane-path API %s", positions.Position(call.Pos()), selected.Sel.Name)
						}
					}
				}
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Error(err)
					return false
				}
				for _, segment := range strings.Split(strings.ReplaceAll(value, "\\", "/"), "/") {
					if segment == "lanes" {
						t.Errorf("%s references adapter-owned lanes directory", positions.Position(literal.Pos()))
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFormerBrandGuardAllowsOnlyHistoricalReferences(t *testing.T) {
	repository := filepath.Clean("..")
	for _, root := range []string{filepath.Join(repository, "bus"), filepath.Join(repository, "deploy"), filepath.Join(repository, "docs", "designs"), filepath.Join(repository, "docs", "END-GOAL.md"), filepath.Join(repository, "README.md"), filepath.Join(repository, "go.mod")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			if containsFormerBrand([]byte(path)) || containsFormerBrandReferences(path, read(t, path)) {
				t.Errorf("former brand remains outside a historical reference: %s", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeploymentUsesCurrentRuntimeAndIdentifiers(t *testing.T) {
	repository := filepath.Clean("..")
	assets := map[string][]string{
		"deploy/sessionbus/systemd/user/sessionbus.service":            {"RuntimeDirectory=sessionbus", "libexec/sessionbus/host/current/bin/sessionbus"},
		"deploy/sessionbus/launchd/net.antst.sessionbus.plist":         {"net.antst.sessionbus", "libexec/sessionbus/host/current/bin/sessionbus"},
		"deploy/sessionbus-hub/systemd/user/sessionbus-hub.service":    {"SESSIONBUS_HUB_LISTEN=:7419", "SESSIONBUS_HUB_CONFIG=%h/.config/sessionbus/hub.json"},
		"deploy/sessionbus-hub/launchd/net.antst.sessionbus-hub.plist": {"net.antst.sessionbus-hub", "SESSIONBUS_HUB_LISTEN", "SESSIONBUS_HUB_CONFIG"},
	}
	for path, values := range assets {
		body := read(t, filepath.Join(repository, filepath.FromSlash(path)))
		for _, value := range values {
			if !bytes.Contains(body, []byte(value)) {
				t.Errorf("%s lacks %q", path, value)
			}
		}
	}
}

func walkSources(t *testing.T, root, extension, header string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && filepath.Ext(path) == extension && firstLine(read(t, path)) != header {
			t.Errorf("source lacks %s header: %s", header, path)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func checkGoImports(t *testing.T, root string, check func(string, string)) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, specification := range file.Imports {
			value, err := strconv.Unquote(specification.Path.Value)
			if err != nil {
				return err
			}
			check(path, value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func firstLine(body []byte) string {
	line, _, _ := bytes.Cut(body, []byte{'\n'})
	return string(line)
}

func section(t *testing.T, document, start, end string) string {
	t.Helper()
	from, to := strings.Index(document, start), strings.Index(document, end)
	if from < 0 || to <= from {
		t.Fatal("protocol section missing")
	}
	return document[from:to]
}

func containsFormerBrandReferences(path string, contents []byte) bool {
	evidence := []byte("/home/antst/agent" + "bus-evidence/")
	cleaned := make([]byte, 0, len(contents))
	for _, line := range bytes.Split(contents, []byte{'\n'}) {
		for start := bytes.Index(line, evidence); start >= 0; start = bytes.Index(line, evidence) {
			end := start + len(evidence)
			for end < len(line) && !bytes.ContainsRune([]byte(" \t`; )"), rune(line[end])) {
				end++
			}
			line = append(line[:start], line[end:]...)
		}
		parts := bytes.Split(line, []byte{'`'})
		for index := 1; index < len(parts); index += 2 {
			if commitQualified(parts[index]) {
				parts[index] = nil
			}
		}
		cleaned = append(cleaned, bytes.Join(parts, nil)...)
		cleaned = append(cleaned, ' ')
	}
	words := bytes.Join(bytes.Fields(cleaned), []byte{' '})
	return containsFormerBrand(cleaned) || bytes.Contains(words, []byte("Agent "+"Sessions"))
}

func containsFormerBrand(value []byte) bool {
	lower := bytes.ToLower(value)
	for _, former := range [][]byte{[]byte("agent" + "bus"), []byte("agent" + "_sessions"), []byte("agent" + "-sessions")} {
		if bytes.Contains(lower, former) {
			return true
		}
	}
	return false
}

func commitQualified(value []byte) bool {
	colon := bytes.IndexByte(value, ':')
	if colon < 7 || colon > 40 {
		return false
	}
	for _, character := range value[:colon] {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	path := value[colon+1:]
	if line := bytes.LastIndexByte(path, ':'); line >= 0 {
		if !sourceLine(path[line+1:]) {
			return false
		}
		path = path[:line]
	}
	if len(path) == 0 {
		return false
	}
	for _, segment := range bytes.Split(path, []byte{'/'}) {
		if len(segment) == 0 || bytes.Equal(segment, []byte("..")) {
			return false
		}
		for _, character := range segment {
			if !(character >= 'A' && character <= 'Z') && !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '_' && character != '.' && character != '-' {
				return false
			}
		}
	}
	return bytes.ContainsRune(path, '/') || bytes.ContainsRune(path, '.')
}

func sourceLine(value []byte) bool {
	hyphens := 0
	for index, character := range value {
		if character >= '0' && character <= '9' {
			continue
		}
		hyphens++
		if character != '-' || hyphens > 1 || index == 0 || index == len(value)-1 {
			return false
		}
	}
	return len(value) > 0
}

func containsToken(contents, token []byte) bool {
	for from := 0; ; {
		index := bytes.Index(contents[from:], token)
		if index < 0 {
			return false
		}
		index += from
		before := index == 0 || !productCharacter(contents[index-1])
		afterAt := index + len(token)
		after := afterAt == len(contents) || !productCharacter(contents[afterAt])
		if before && after {
			return true
		}
		from = index + 1
	}
}

func productCharacter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-'
}

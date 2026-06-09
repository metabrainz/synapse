package listenbrainz

import (
	"os"
	"strings"
	"testing"
)

// TestAllMatchesSourceFiles enforces the "one .go file = one event" convention:
// every event source file must have a corresponding entry in All. Counts .go files
// excluding _test.go and all.go.
func TestAllMatchesSourceFiles(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	var eventFiles int
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") || name == "all.go" {
			continue
		}
		eventFiles++
	}
	if len(All) != eventFiles {
		t.Errorf("All has %d events but there are %d event source files — did you forget to add it to all.go?", len(All), eventFiles)
	}
}

func TestAllHasNoDuplicates(t *testing.T) {
	seen := make(map[string]bool)
	for _, et := range All {
		name := et.EventName()
		if seen[name] {
			t.Errorf("duplicate event name %q in All", name)
		}
		seen[name] = true
	}
}

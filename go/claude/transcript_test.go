package claude

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadTranscript(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home) // os.UserHomeDir reads $HOME on Unix
	dir := filepath.Join(home, ".claude", "projects", "-tmp-proj")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		`{"type":"queue-operation"}`,
		`{"type":"user","cwd":"/tmp/proj","message":{"role":"user","content":"what is here?"}}`,
		`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"hmm"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Read","input":{}}]}}`,
		`{"type":"user","message":{"content":[{"type":"tool_result","content":"file text"}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"text","text":"A Go project."}]}}`,
		`{"type":"user","isMeta":true,"message":{"content":"<system stuff>"}}`,
		`not json at all`,
	}
	if err := os.WriteFile(filepath.Join(dir, "abc.jsonl"), []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}

	tr, err := ReadTranscript("abc")
	if err != nil {
		t.Fatal(err)
	}
	if tr.Dir != "/tmp/proj" || len(tr.Turns) != 2 {
		t.Fatalf("got %+v", tr)
	}
	if tr.Turns[0].Text != "what is here?" || tr.Turns[1].Text != "A Go project." || tr.Turns[1].Tools[0] != "Read" {
		t.Fatalf("turns %+v", tr.Turns)
	}
	if _, err := ReadTranscript("missing"); err != ErrNoTranscript {
		t.Fatalf("missing: %v", err)
	}
	if _, err := ReadTranscript("../etc/passwd"); err != ErrNoTranscript {
		t.Fatalf("path traversal not rejected: %v", err)
	}
}

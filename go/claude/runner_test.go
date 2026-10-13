package claude

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agentmesh/protocol"
)

// recorder is a Sink that remembers what it was sent. Real sinks may be
// called from several goroutines, so it's guarded like one.
type recorder struct {
	mu   sync.Mutex
	text strings.Builder
	raw  []string
}

func (r *recorder) Send(t string, _ ...protocol.Attachment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.text.WriteString(t)
}

func (r *recorder) Raw(typ string, _ any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.raw = append(r.raw, typ)
}

func newTestSession(t *testing.T) *Session {
	bin, err := filepath.Abs("testdata/fake-claude.sh")
	if err != nil {
		t.Fatal(err)
	}
	return NewSession(Options{Dir: t.TempDir(), Bin: bin})
}

func TestTurnsStreamAndResume(t *testing.T) {
	s := newTestSession(t)
	var r1, r2 recorder
	if _, err := s.Run(context.Background(), "hello", &r1); err != nil {
		t.Fatal(err)
	}
	if r1.text.String() != "got: hello" || len(r1.raw) != 1 || r1.raw[0] != "tool" {
		t.Fatalf("turn 1: %q %v", r1.text.String(), r1.raw)
	}
	if _, err := s.Run(context.Background(), "again", &r2); err != nil {
		t.Fatal(err)
	}
	if r2.text.String() != "(resumed) got: again" {
		t.Fatalf("turn 2 should resume: %q", r2.text.String())
	}
	_, turns, cost, sid := s.Stats()
	if turns != 2 || cost < 0.0019 || sid != "sess-1" {
		t.Fatalf("stats: turns=%d cost=%v sid=%q", turns, cost, sid)
	}
}

func TestStaleSessionStartsFresh(t *testing.T) {
	s := newTestSession(t)
	s.sessionID = "stale" // as if a previous turn left an unusable id
	var r recorder
	if _, err := s.Run(context.Background(), "hi", &r); err != nil {
		t.Fatal(err)
	}
	if r.text.String() != "got: hi" || !strings.Contains(strings.Join(r.raw, ","), "status_note") {
		t.Fatalf("got %q %v", r.text.String(), r.raw)
	}
}

func TestWatchdogStopsAStalledCLI(t *testing.T) {
	t.Setenv("FAKE_STALL", "1") // restored automatically after the test
	s := newTestSession(t)
	s.opts.FirstOutput = 500 * time.Millisecond
	start := time.Now()
	_, err := s.Run(context.Background(), "hi", &recorder{})
	if err == nil || !strings.Contains(err.Error(), "login") {
		t.Fatalf("want a stall error mentioning login, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("took %s: the process group was not killed", time.Since(start))
	}
}

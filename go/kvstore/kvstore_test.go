package kvstore

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startServer(t *testing.T, jetStream bool) *nats.Conn {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: jetStream, StoreDir: t.TempDir(), // KV data lives on disk here
	})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// Both implementations must behave the same, so one test runs against each.
func TestStores(t *testing.T) {
	ctx := context.Background()
	js, err := OpenJetStream(ctx, startServer(t, true), "test_bucket")
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]Store{"memory": NewMemory(), "jetstream": js} {
		t.Run(name, func(t *testing.T) {
			if v, err := s.Get(ctx, "a.b"); v != nil || err != nil {
				t.Fatalf("missing key: got %q, %v", v, err)
			}
			if err := s.Put(ctx, "a.b", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := s.Put(ctx, "a.b", []byte("two")); err != nil { // overwrite
				t.Fatal(err)
			}
			if v, _ := s.Get(ctx, "a.b"); !bytes.Equal(v, []byte("two")) {
				t.Fatalf("got %q", v)
			}
			if err := s.Delete(ctx, "a.b"); err != nil {
				t.Fatal(err)
			}
			if v, _ := s.Get(ctx, "a.b"); v != nil {
				t.Fatalf("after delete: %q", v)
			}
			if err := s.Delete(ctx, "never.there"); err != nil {
				t.Fatalf("deleting a missing key should be fine: %v", err)
			}
		})
	}
}

// Data written through one connection is visible through another: that's
// what lets a restarted agent find its sessions.
func TestJetStreamSurvivesReconnect(t *testing.T) {
	ctx := context.Background()
	nc := startServer(t, true)
	s1, err := OpenJetStream(ctx, nc, "test_bucket")
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.Put(ctx, "claude.t.proj", []byte("sess-123"))

	nc2, err := nats.Connect(nc.ConnectedUrl())
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	s2, err := OpenJetStream(ctx, nc2, "test_bucket")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := s2.Get(ctx, "claude.t.proj"); string(v) != "sess-123" {
		t.Fatalf("got %q", v)
	}
}

func TestOpenFallsBackWithoutJetStream(t *testing.T) {
	s, warning := Open(context.Background(), startServer(t, false))
	if !strings.HasPrefix(s.Kind(), "memory") || !strings.Contains(warning, "-js") {
		t.Fatalf("kind %q, warning %q", s.Kind(), warning)
	}
}

func TestKey(t *testing.T) {
	if got := Key("ollama", "srini", "llama3.2", "a b/c"); got != "ollama.srini.llama3_2.a_b_c" {
		t.Fatalf("got %q", got)
	}
	if got := Key("x", ""); got != "x._" {
		t.Fatalf("got %q", got)
	}
}

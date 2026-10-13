// End-to-end tests: a real NATS server (embedded in the test process), real
// micro services, real wire traffic. The package name ends in _test, so these
// tests use mesh only through its exported API, like a real caller would.
package mesh_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"agentmesh/agent"
	"agentmesh/mesh"
	"agentmesh/protocol"
)

// startServer runs nats-server inside the test on a random port.
func startServer(t *testing.T) *nats.Conn {
	t.Helper() // failures are reported at the caller's line
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	t.Cleanup(ns.Shutdown) // runs when the test finishes
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func startAgent(t *testing.T, opts agent.Options) *agent.Host {
	t.Helper()
	if opts.Owner == "" {
		opts.Owner = "t"
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = time.Second
	}
	opts.Logger = nil
	h, err := agent.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Stop() })
	return h
}

func collect(ctx context.Context, seq func(func(mesh.Event) bool), answer string) []mesh.Event {
	var evs []mesh.Event
	for ev := range seq {
		evs = append(evs, ev)
		if ev.Type == mesh.EvQuery {
			ev.Answer(answer)
		}
	}
	return evs
}

func types(evs []mesh.Event) string {
	var s []string
	for _, e := range evs {
		s = append(s, string(e.Type))
	}
	return strings.Join(s, ",")
}

func text(evs []mesh.Event) string {
	var b strings.Builder
	for _, e := range evs {
		if e.Type == mesh.EvText {
			b.WriteString(e.Text)
		}
	}
	return b.String()
}

// fixture starts the same set of agents for every test.
func fixture(t *testing.T) (*nats.Conn, *mesh.Mesh, []mesh.Agent) {
	nc := startServer(t)
	echo := func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
		r.Send("echo:" + e.Prompt)
		return nil
	}
	startAgent(t, agent.Options{Conn: nc, Agent: "echo", Name: "a", AttachmentsOK: true, OnPrompt: echo})
	for i := range 2 { // two instances of one identity: the queue group balances them
		startAgent(t, agent.Options{Conn: nc, Agent: "lb", Name: "x", OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
			r.Send([]string{"inst0", "inst1"}[i])
			return nil
		}})
	}
	startAgent(t, agent.Options{Conn: nc, Agent: "asker", Name: "q", OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
		a, ok := r.Ask(ctx, "proceed?", 2*time.Second)
		r.Send("got:" + a + map[bool]string{true: "", false: "(timeout)"}[ok])
		return nil
	}})
	startAgent(t, agent.Options{Conn: nc, Agent: "fail", Name: "f", OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
		r.Send("partial")
		return &protocol.Error{Code: 429, Message: "slow down", Body: map[string]any{"error": "rate_limited", "retry_after_s": 3}}
	}})
	startAgent(t, agent.Options{Conn: nc, Agent: "slow", Name: "s", KeepAlive: 200 * time.Millisecond,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
			time.Sleep(900 * time.Millisecond)
			r.Send("finally")
			return nil
		}})
	startAgent(t, agent.Options{Conn: nc, Agent: "ctl", Name: "control", Metadata: map[string]string{"role": "controller"}, OnPrompt: echo})
	startAgent(t, agent.Options{Conn: nc, Agent: "meta", Name: "m", Metadata: map[string]string{"role": "orchestrator"}, OnPrompt: echo})

	m := mesh.New(nc)
	if err := m.Watch(); err != nil {
		t.Fatal(err)
	}
	all, err := m.Discover(300 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return nc, m, all
}

func one(t *testing.T, all []mesh.Agent, target string) mesh.Agent {
	t.Helper()
	got := mesh.MatchTargets(all, target)
	if len(got) != 1 {
		t.Fatalf("MatchTargets(%q) = %d agents", target, len(got))
	}
	return got[0]
}

func TestMesh(t *testing.T) {
	_, m, all := fixture(t)
	ctx := context.Background()

	t.Run("discovery returns metadata and endpoints", func(t *testing.T) {
		a := one(t, all, "echo")
		ep := a.Endpoints["prompt"]
		if a.ProtocolVersion != "0.3" || ep.Subject != "agents.prompt.echo.t.a" || ep.QueueGroup != "agents" ||
			ep.Metadata["attachments_ok"] != "true" || ep.Metadata["max_payload"] == "" ||
			a.Endpoints["status"].Subject != "agents.status.echo.t.a" {
			t.Fatalf("unexpected record: %+v", a)
		}
	})

	t.Run("ack, content, terminator", func(t *testing.T) {
		evs := collect(ctx, m.Prompt(ctx, one(t, all, "echo"), "hi", mesh.PromptOptions{}), "")
		if types(evs) != "ack,text,done" || text(evs) != "echo:hi" {
			t.Fatalf("got %s %q", types(evs), text(evs))
		}
	})

	t.Run("mid-stream query round trip", func(t *testing.T) {
		evs := collect(ctx, m.Prompt(ctx, one(t, all, "asker"), "go", mesh.PromptOptions{}), "yes please")
		if text(evs) != "got:yes please" {
			t.Fatalf("got %q", text(evs))
		}
	})

	t.Run("error mid-stream then terminator", func(t *testing.T) {
		evs := collect(ctx, m.Prompt(ctx, one(t, all, "fail"), "go", mesh.PromptOptions{}), "")
		if types(evs) != "ack,text,error,done" || evs[2].Code != 429 || !strings.Contains(string(evs[2].Body), "retry_after_s") {
			t.Fatalf("got %s %+v", types(evs), evs)
		}
	})

	t.Run("keep-alive beats a short inactivity timeout", func(t *testing.T) {
		evs := collect(ctx, m.Prompt(ctx, one(t, all, "slow"), "go", mesh.PromptOptions{Inactivity: 500 * time.Millisecond}), "")
		if text(evs) != "finally" || strings.Count(types(evs), "ack") < 2 {
			t.Fatalf("got %s", types(evs))
		}
	})

	t.Run("inactivity timeout when nobody answers", func(t *testing.T) {
		a := one(t, all, "echo")
		a.Endpoints = map[string]mesh.Endpoint{"prompt": {Name: "prompt", Subject: "agents.prompt.nobody.t.x"}}
		evs := collect(ctx, m.Prompt(ctx, a, "hi", mesh.PromptOptions{Inactivity: 200 * time.Millisecond}), "")
		if types(evs) != "timeout" {
			t.Fatalf("got %s", types(evs))
		}
	})

	t.Run("attachments refused locally", func(t *testing.T) {
		evs := collect(ctx, m.Prompt(ctx, one(t, all, "asker"), "x",
			mesh.PromptOptions{Attachments: []protocol.Attachment{{Filename: "a", Content: "aGk="}}}), "")
		if types(evs) != "error" { // never published
			t.Fatalf("got %s", types(evs))
		}
	})

	t.Run("same identity is one agent; queue group spreads load", func(t *testing.T) {
		n := 0
		for _, a := range all {
			if a.Token == "lb" {
				n++
			}
		}
		if n != 2 {
			t.Fatalf("want 2 lb instances, got %d", n)
		}
		seen := map[string]bool{}
		for range 20 {
			seen[text(collect(ctx, m.Prompt(ctx, one(t, all, "lb"), "who", mesh.PromptOptions{}), ""))] = true
		}
		if !seen["inst0"] || !seen["inst1"] {
			t.Fatalf("load not spread: %v", seen)
		}
	})

	t.Run("fan-out merges streams", func(t *testing.T) {
		by := map[string]string{}
		for ev := range m.Fanout(ctx, mesh.MatchTargets(all, "echo,lb"), "hey", mesh.PromptOptions{}) {
			if ev.Type == mesh.EvText {
				by[ev.From.Token] = ev.Text
			}
		}
		if by["echo"] != "echo:hey" || !strings.HasPrefix(by["lb"], "inst") {
			t.Fatalf("got %v", by)
		}
	})

	t.Run("breaking out of a fan-out stops cleanly", func(t *testing.T) {
		for range m.Fanout(ctx, mesh.MatchTargets(all, "all"), "hey", mesh.PromptOptions{}) {
			break // the deferred cancel must unblock every producer goroutine
		}
	})

	t.Run("target matching", func(t *testing.T) {
		labels := func(as []mesh.Agent) string {
			var s []string
			for _, a := range as {
				s = append(s, a.Token)
			}
			return strings.Join(s, ",")
		}
		cases := map[string]string{
			"ech":     "",     // no substring bleed
			"ctl":     "ctl",  // controllers only when named
			"meta":    "meta", // same for orchestrators
			"@echo/t": "echo", // label prefix, @ ignored
			"all":     "asker,echo,fail,lb,slow",
		}
		for in, want := range cases {
			if got := labels(mesh.MatchTargets(all, in)); got != want {
				t.Errorf("MatchTargets(%q) = %q, want %q", in, got, want)
			}
		}
	})

	t.Run("status and heartbeats agree", func(t *testing.T) {
		a := one(t, all, "echo")
		hb, err := m.Status(a, 2*time.Second)
		if err != nil || hb.InstanceID != a.InstanceID {
			t.Fatalf("status %+v, %v", hb, err)
		}
		time.Sleep(1200 * time.Millisecond)
		if m.Liveness(a.InstanceID) != "online" {
			t.Fatal("not online")
		}
	})
}

func TestStopRemovesFromDiscovery(t *testing.T) {
	nc := startServer(t)
	m := mesh.New(nc)
	h, err := agent.New(agent.Options{Conn: nc, Agent: "temp", Owner: "t", Name: "z",
		OnPrompt: func(context.Context, protocol.Envelope, *agent.Responder) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		all, _ := m.Discover(300 * time.Millisecond)
		return len(mesh.MatchTargets(all, "temp"))
	}
	if count() != 1 {
		t.Fatal("not discovered")
	}
	_ = h.Stop()
	if count() != 0 {
		t.Fatal("still discovered after Stop")
	}
}

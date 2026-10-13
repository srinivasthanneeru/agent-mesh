package meshmcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	natsserver "github.com/nats-io/nats-server/v2/server" // alias: our package has its own `server` type
	"github.com/nats-io/nats.go"

	"agentmesh/agent"
	"agentmesh/claude"
	"agentmesh/kvstore"
	"agentmesh/mesh"
	"agentmesh/ollama"
	"agentmesh/ollama/ollamatest"
	"agentmesh/protocol"
)

// fixture: embedded NATS with JetStream, three agents, and an MCP client
// connected to our MCP server through in-memory pipes (what Claude Desktop
// does over stdin/stdout).
func fixture(t *testing.T) (*mcp.ClientSession, kvstore.Store) {
	t.Helper()
	ctx := context.Background()
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: t.TempDir()})
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
	store, err := kvstore.OpenJetStream(ctx, nc, "test_mesh")
	if err != nil {
		t.Fatal(err)
	}

	start := func(o agent.Options) {
		o.Conn, o.Owner, o.Heartbeat = nc, "t", time.Second
		h, err := agent.New(o)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Stop() })
	}
	start(agent.Options{Agent: "echo", Name: "main", Description: "echoes",
		OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
			r.Send("echo: " + e.Prompt)
			return nil
		}})
	start(agent.Options{Agent: "shell", Name: "box", Description: "asks first",
		OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
			answer, _ := r.Ask(ctx, "Run `"+e.Prompt+"`? (yes/no)", 5*time.Second)
			r.Send("answer was " + answer)
			return nil
		}})

	fake := ollamatest.New()
	t.Cleanup(fake.Close)
	oa := ollama.New(ollama.Options{BaseURL: fake.URL, Model: "llama3.2", Store: store, KeyPrefix: kvstore.Key("ollama", "t", "llama")})
	start(agent.Options{Agent: "ollama", Name: "llama", Description: "local model",
		OnPrompt: func(ctx context.Context, e protocol.Envelope, r *agent.Responder) error {
			conv := "default"
			if raw, ok := e.Extra["conversation"]; ok {
				conv = strings.Trim(string(raw), `"`)
			}
			_, err := oa.Chat(ctx, conv, e.Prompt, r)
			return err
		}})

	m := mesh.New(nc)
	if err := m.Watch(); err != nil {
		t.Fatal(err)
	}
	fakeTranscript := func(id string) (claude.Transcript, error) {
		return claude.Transcript{SessionID: id, Dir: "/tmp/my project", Turns: []claude.Turn{
			{Role: "user", Text: "what does this repo do?"},
			{Role: "assistant", Text: "It builds an agent mesh.", Tools: []string{"Read", "Grep", "Read"}},
		}}, nil
	}
	srv := newServer(m, store, fakeTranscript)

	st, ct := mcp.NewInMemoryTransports()
	if _, err := srv.Connect(ctx, st, nil); err != nil {
		t.Fatal(err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs, store
}

// call runs a tool and returns its text; isErr reports a tool-level error.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) (out string, isErr bool) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String(), res.IsError
}

func TestTools(t *testing.T) {
	cs, store := fixture(t)

	t.Run("tools are listed with schemas", func(t *testing.T) {
		res, err := cs.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]bool{}
		for _, tool := range res.Tools {
			names[tool.Name] = true
		}
		for _, want := range []string{"mesh_agents", "mesh_ask", "mesh_sessions", "mesh_session"} {
			if !names[want] {
				t.Errorf("missing tool %s", want)
			}
		}
	})

	t.Run("mesh_agents", func(t *testing.T) {
		out, _ := call(t, cs, "mesh_agents", nil)
		for _, want := range []string{"echo/t/main", "shell/t/box", "ollama/t/llama"} {
			if !strings.Contains(out, want) {
				t.Errorf("missing %s in %q", want, out)
			}
		}
	})

	t.Run("mesh_ask one agent", func(t *testing.T) {
		out, isErr := call(t, cs, "mesh_ask", map[string]any{"target": "echo", "prompt": "hi"})
		if isErr || !strings.Contains(out, "### echo/t/main\necho: hi") {
			t.Fatalf("got %q", out)
		}
	})

	t.Run("questions are declined, never approved", func(t *testing.T) {
		out, _ := call(t, cs, "mesh_ask", map[string]any{"target": "shell", "prompt": "rm -rf /tmp/x"})
		if !strings.Contains(out, "answer was no") || !strings.Contains(out, "declined automatically") {
			t.Fatalf("got %q", out)
		}
	})

	t.Run("unknown target is a tool error listing agents", func(t *testing.T) {
		out, isErr := call(t, cs, "mesh_ask", map[string]any{"target": "nope", "prompt": "hi"})
		if !isErr || !strings.Contains(out, "echo/t/main") {
			t.Fatalf("got %q, isErr=%v", out, isErr)
		}
	})

	t.Run("ollama conversation continues and is listed and shown", func(t *testing.T) {
		call(t, cs, "mesh_ask", map[string]any{"target": "ollama", "prompt": "my name is Srini", "conversation": "c1"})
		out, _ := call(t, cs, "mesh_ask", map[string]any{"target": "ollama", "prompt": "what is my name?", "conversation": "c1"})
		if !strings.Contains(out, "remember 1 earlier") {
			t.Fatalf("conversation not continued: %q", out)
		}
		list, _ := call(t, cs, "mesh_sessions", nil)
		if !strings.Contains(list, "ollama.t.llama.c1") || !strings.Contains(list, "4 messages") {
			t.Fatalf("sessions: %q", list)
		}
		show, _ := call(t, cs, "mesh_session", map[string]any{"session": "ollama.t.llama.c1"})
		if !strings.Contains(show, "**user:** my name is Srini") || !strings.Contains(show, `conversation "c1"`) {
			t.Fatalf("session: %q", show)
		}
	})

	t.Run("claude session is listed and shown with resume command", func(t *testing.T) {
		_ = store.Put(context.Background(), "claude.t.proj", []byte("sess-42"))
		list, _ := call(t, cs, "mesh_sessions", map[string]any{"agent": "cc"})
		if !strings.Contains(list, "claude.t.proj") || !strings.Contains(list, "sess-42") || strings.Contains(list, "ollama") {
			t.Fatalf("filtered sessions: %q", list)
		}
		show, _ := call(t, cs, "mesh_session", map[string]any{"session": "claude.t.proj"})
		for _, want := range []string{"It builds an agent mesh.", "(tools: Grep, Read)", "cd '/tmp/my project' && claude --resume sess-42"} {
			if !strings.Contains(show, want) {
				t.Errorf("missing %q in %q", want, show)
			}
		}
	})

	t.Run("bad session ids are tool errors", func(t *testing.T) {
		if _, isErr := call(t, cs, "mesh_session", map[string]any{"session": "nonsense"}); !isErr {
			t.Error("want error for unknown id format")
		}
		if _, isErr := call(t, cs, "mesh_session", map[string]any{"session": "ollama.t.llama.missing"}); !isErr {
			t.Error("want error for missing session")
		}
	})
}

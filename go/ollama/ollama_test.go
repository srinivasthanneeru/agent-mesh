package ollama

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"agentmesh/ollama/ollamatest"
	"agentmesh/protocol"
)

type recorder struct {
	mu     sync.Mutex
	chunks []string
}

func (r *recorder) Send(t string, _ ...protocol.Attachment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks = append(r.chunks, t)
}
func (r *recorder) Raw(string, any) {}
func (r *recorder) text() string    { return strings.Join(r.chunks, "") }

func setup(t *testing.T, opts Options) (*Adapter, *ollamatest.Server) {
	t.Helper()
	fake := ollamatest.New()
	t.Cleanup(fake.Close)
	opts.BaseURL = fake.URL
	if opts.Model == "" {
		opts.Model = "llama3.2"
	}
	return New(opts), fake
}

func TestStreamsTokenByToken(t *testing.T) {
	a, _ := setup(t, Options{})
	var r recorder
	u, err := a.Chat(context.Background(), "default", "hello there", &r)
	if err != nil {
		t.Fatal(err)
	}
	if r.text() != "I remember 0 earlier messages. You said: hello there" {
		t.Fatalf("got %q", r.text())
	}
	if len(r.chunks) < 5 { // one chunk per token, not one big blob
		t.Fatalf("expected many chunks, got %d", len(r.chunks))
	}
	if u.OutputTokens != len(r.chunks) || u.TokensPerSec <= 0 {
		t.Fatalf("usage %+v", u)
	}
}

func TestRemembersTheConversation(t *testing.T) {
	a, fake := setup(t, Options{System: "be brief"})
	ctx := context.Background()
	for _, p := range []string{"my name is Srini", "what is my name?"} {
		if _, err := a.Chat(ctx, "default", p, &recorder{}); err != nil {
			t.Fatal(err)
		}
	}
	reqs := fake.Requests()
	second := reqs[1] // what the model actually saw on turn 2
	roles := []string{}
	for _, m := range second {
		roles = append(roles, m["role"])
	}
	if strings.Join(roles, ",") != "system,user,assistant,user" || second[1]["content"] != "my name is Srini" {
		t.Fatalf("turn 2 messages: %v", second)
	}
	if n := len(a.History("default")); n != 4 {
		t.Fatalf("history has %d messages, want 4", n)
	}
}

func TestConversationsAreSeparate(t *testing.T) {
	a, _ := setup(t, Options{})
	ctx := context.Background()
	_, _ = a.Chat(ctx, "alice", "hi", &recorder{})
	var r recorder
	_, _ = a.Chat(ctx, "bob", "hi", &r)
	if !strings.Contains(r.text(), "remember 0") {
		t.Fatalf("bob saw alice's memory: %q", r.text())
	}
}

func TestHistoryIsCapped(t *testing.T) {
	a, _ := setup(t, Options{MaxHistory: 4})
	for i := range 5 {
		_, _ = a.Chat(context.Background(), "c", "msg"+string(rune('0'+i)), &recorder{})
	}
	h := a.History("c")
	if len(h) != 4 || h[0].Content != "msg3" {
		t.Fatalf("history %v", h)
	}
}

func TestResetForgets(t *testing.T) {
	a, _ := setup(t, Options{})
	_, _ = a.Chat(context.Background(), "default", "one", &recorder{})
	a.Reset("default")
	var r recorder
	_, _ = a.Chat(context.Background(), "default", "two", &r)
	if !strings.Contains(r.text(), "remember 0") {
		t.Fatalf("got %q", r.text())
	}
}

func TestFailedTurnIsNotRemembered(t *testing.T) {
	a, _ := setup(t, Options{})
	_, err := a.Chat(context.Background(), "default", "FAIL", &recorder{})
	if err == nil || !strings.Contains(err.Error(), "out of memory") {
		t.Fatalf("want mid-stream error, got %v", err)
	}
	if n := len(a.History("default")); n != 0 {
		t.Fatalf("failed turn was remembered (%d messages)", n)
	}
}

func TestMissingModelIs404WithHint(t *testing.T) {
	a, _ := setup(t, Options{Model: "nope"})
	_, err := a.Chat(context.Background(), "default", "hi", &recorder{})
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != 404 || !strings.Contains(pe.Message, "ollama pull nope") ||
		!strings.HasPrefix(pe.Message, `model "nope" not found`) { // decoded, not raw JSON
		t.Fatalf("got %v", err)
	}
}

func TestStallWatchdog(t *testing.T) {
	a, _ := setup(t, Options{FirstToken: 300 * time.Millisecond})
	start := time.Now()
	_, err := a.Chat(context.Background(), "default", "STALL", &recorder{})
	if err == nil || !strings.Contains(err.Error(), "nothing within") || time.Since(start) > 3*time.Second {
		t.Fatalf("got %v after %s", err, time.Since(start))
	}
}

func TestSameConversationIsBusy(t *testing.T) {
	a, fake := setup(t, Options{})
	fake.TokenDelay = 50 * time.Millisecond
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Chat(context.Background(), "default", "slow one", &recorder{})
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := a.Chat(context.Background(), "default", "second", &recorder{}); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	if _, err := a.Chat(context.Background(), "other", "parallel is fine", &recorder{}); err != nil {
		t.Fatalf("other conversation should run: %v", err)
	}
	<-done
}

func TestUnreachableServer(t *testing.T) {
	a := New(Options{BaseURL: "127.0.0.1:1", Model: "llama3.2"}) // also tests the "host:port" form
	_, err := a.Chat(context.Background(), "default", "hi", &recorder{})
	if err == nil || !strings.Contains(err.Error(), "ollama serve") {
		t.Fatalf("got %v", err)
	}
}

func TestModelsAndHasModel(t *testing.T) {
	a, _ := setup(t, Options{})
	models, err := a.Models(context.Background())
	if err != nil || !HasModel(models, "llama3.2") || HasModel(models, "mistral") {
		t.Fatalf("models %v, err %v", models, err)
	}
}

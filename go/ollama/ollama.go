// Package ollama talks to a local Ollama server (https://ollama.com) and
// turns its token stream into protocol chunks, with conversation memory.
//
// Ollama's chat API is stateless: every request carries the whole
// conversation in `messages`. So "memory" is simply us keeping that list,
// appending each question and answer, and sending it back next time.
//
//	POST /api/chat {"model":"llama3.2","messages":[...],"stream":true}
//	→ one JSON object per line (NDJSON):
//	  {"message":{"role":"assistant","content":"The"},"done":false}
//	  {"message":{"role":"assistant","content":" sky"},"done":false}
//	  {"done":true,"prompt_eval_count":26,"eval_count":298,"eval_duration":4799921000}
//
// Thinking models (qwen3, deepseek-r1, ...) first stream their reasoning in
// message.thinking with an empty content. We forward it as a separate
// "thinking" chunk (callers that don't know the type ignore it) and never
// store it in memory: only the final answer is remembered.
package ollama

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"agentmesh/protocol"
)

// Sink receives the streamed answer; *agent.Responder satisfies it.
type Sink interface {
	Send(text string, attachments ...protocol.Attachment)
	Raw(typ string, data any)
}

// Message is one turn in Ollama's chat format.
type Message struct {
	Role    string `json:"role"` // "system", "user" or "assistant"
	Content string `json:"content"`
}

// Options configures an Adapter.
type Options struct {
	BaseURL    string        // default $OLLAMA_HOST or http://127.0.0.1:11434
	Model      string        // e.g. "llama3.2"
	System     string        // optional system prompt, sent first on every request
	MaxHistory int           // messages kept per conversation (default 40); older ones are dropped
	KeepAlive  string        // how long Ollama keeps the model loaded, e.g. "10m"
	FirstToken time.Duration // give up if no token arrives in this time (default 5m: first load can be slow)
	// Think controls reasoning for "thinking" models such as qwen3 or deepseek-r1:
	// "" = the model's default, "true" / "false" = force it on or off.
	Think string
	HTTP  *http.Client
}

// Usage is what Ollama reports when a reply finishes.
type Usage struct {
	PromptTokens int     `json:"prompt_tokens"`
	OutputTokens int     `json:"output_tokens"`
	TokensPerSec float64 `json:"tokens_per_sec"`
	DurationMs   int64   `json:"duration_ms"`
}

// ErrBusy means this conversation is already answering a prompt.
var ErrBusy = errors.New("this conversation is already answering a prompt")

// conversation is one memory. Callers pick one with the envelope field
// "conversation"; without it everyone shares "default".
type conversation struct {
	busy    bool
	history []Message
}

// Adapter is safe for concurrent use: different conversations run in
// parallel, the same conversation one prompt at a time.
type Adapter struct {
	opts  Options
	mu    sync.Mutex
	convs map[string]*conversation
}

// New returns an Adapter with defaults filled in.
func New(opts Options) *Adapter {
	if opts.BaseURL == "" {
		opts.BaseURL = os.Getenv("OLLAMA_HOST")
	}
	if opts.BaseURL == "" {
		opts.BaseURL = "http://127.0.0.1:11434"
	}
	if !strings.Contains(opts.BaseURL, "://") { // OLLAMA_HOST is often just host:port
		opts.BaseURL = "http://" + opts.BaseURL
	}
	opts.BaseURL = strings.TrimRight(opts.BaseURL, "/")
	if opts.MaxHistory == 0 {
		opts.MaxHistory = 40
	}
	if opts.FirstToken == 0 {
		opts.FirstToken = 5 * time.Minute
	}
	if opts.HTTP == nil {
		opts.HTTP = &http.Client{} // no overall timeout: answers stream for as long as they take
	}
	return &Adapter{opts: opts, convs: map[string]*conversation{}}
}

// Model is the configured model name.
func (a *Adapter) Model() string { return a.opts.Model }

// Models lists the models installed in Ollama (GET /api/tags).
func (a *Adapter) Models(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.opts.BaseURL+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.opts.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot reach Ollama at %s (is it running? `ollama serve`): %w", a.opts.BaseURL, err)
	}
	defer resp.Body.Close()
	var body struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	names := make([]string, len(body.Models))
	for i, m := range body.Models {
		names[i] = m.Name
	}
	return names, nil
}

// HasModel reports whether name is installed; "llama3.2" matches "llama3.2:latest".
func HasModel(installed []string, name string) bool {
	for _, m := range installed {
		if m == name || m == name+":latest" {
			return true
		}
	}
	return false
}

// History returns a copy of a conversation's messages.
func (a *Adapter) History(conv string) []Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	if c := a.convs[conv]; c != nil {
		return append([]Message(nil), c.history...)
	}
	return nil
}

// Reset forgets a conversation.
func (a *Adapter) Reset(conv string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.convs, conv)
}

// Chat sends prompt with the conversation's history, streams each token to
// sink as it arrives, and on success remembers both sides of the exchange.
// A failed or cancelled reply is not remembered, so it can't poison later turns.
func (a *Adapter) Chat(ctx context.Context, conv, prompt string, sink Sink) (Usage, error) {
	a.mu.Lock()
	c := a.convs[conv]
	if c == nil {
		c = &conversation{}
		a.convs[conv] = c
	}
	if c.busy {
		a.mu.Unlock()
		return Usage{}, ErrBusy
	}
	c.busy = true
	history := append([]Message(nil), c.history...) // copy: we release the lock during the request
	a.mu.Unlock()

	defer func() {
		a.mu.Lock()
		c.busy = false
		a.mu.Unlock()
	}()

	messages := make([]Message, 0, len(history)+2)
	if a.opts.System != "" {
		messages = append(messages, Message{"system", a.opts.System})
	}
	messages = append(messages, history...)
	messages = append(messages, Message{"user", prompt})

	answer, usage, err := a.stream(ctx, messages, sink)
	if err != nil {
		return usage, err
	}

	a.mu.Lock()
	c.history = append(c.history, Message{"user", prompt}, Message{"assistant", answer})
	if over := len(c.history) - a.opts.MaxHistory; over > 0 {
		c.history = c.history[over:] // keep the newest messages
	}
	a.mu.Unlock()
	return usage, nil
}

// chunk is one NDJSON line from /api/chat.
type chunk struct {
	Message struct {
		Content  string `json:"content"`
		Thinking string `json:"thinking"`
	} `json:"message"`
	Done            bool   `json:"done"`
	Error           string `json:"error"`
	PromptEvalCount int    `json:"prompt_eval_count"`
	EvalCount       int    `json:"eval_count"`
	EvalDuration    int64  `json:"eval_duration"`  // nanoseconds
	TotalDuration   int64  `json:"total_duration"` // nanoseconds
}

func (a *Adapter) stream(ctx context.Context, messages []Message, sink Sink) (string, Usage, error) {
	body := map[string]any{"model": a.opts.Model, "messages": messages, "stream": true}
	if a.opts.KeepAlive != "" {
		body["keep_alive"] = a.opts.KeepAlive
	}
	switch a.opts.Think {
	case "true":
		body["think"] = true
	case "false":
		body["think"] = false
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", Usage{}, err
	}

	// If no token arrives within FirstToken, cancel the request. Loading a
	// big model the first time can take a while, hence the generous default.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The timer's func runs on another goroutine, so `stalled` must be atomic.
	var stalled atomic.Bool
	watchdog := time.AfterFunc(a.opts.FirstToken, func() { stalled.Store(true); cancel() })
	defer watchdog.Stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.opts.BaseURL+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return "", Usage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.opts.HTTP.Do(req)
	if err != nil {
		if stalled.Load() {
			return "", Usage{}, fmt.Errorf("ollama sent nothing within %s", a.opts.FirstToken)
		}
		return "", Usage{}, fmt.Errorf("cannot reach Ollama at %s (is it running? `ollama serve`): %w", a.opts.BaseURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Errors come back as {"error": "..."}, e.g. a model that isn't pulled.
		var e struct {
			Error string `json:"error"`
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if json.Unmarshal(raw, &e) != nil || e.Error == "" {
			e.Error = strings.TrimSpace(string(raw))
		}
		if resp.StatusCode == http.StatusNotFound {
			return "", Usage{}, protocol.Errorf(404, "%s (run: ollama pull %s)", e.Error, a.opts.Model)
		}
		return "", Usage{}, fmt.Errorf("ollama %s: %s", resp.Status, e.Error)
	}

	var (
		answer strings.Builder
		usage  Usage
	)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for sc.Scan() {
		var c chunk
		if json.Unmarshal(sc.Bytes(), &c) != nil {
			continue
		}
		if c.Error != "" { // errors can also arrive mid-stream
			return "", usage, fmt.Errorf("ollama: %s", c.Error)
		}
		if c.Message.Thinking != "" {
			watchdog.Stop() // reasoning counts as the model responding
			sink.Raw("thinking", c.Message.Thinking)
		}
		if c.Message.Content != "" {
			watchdog.Stop()
			answer.WriteString(c.Message.Content)
			sink.Send(c.Message.Content) // word-by-word: one chunk per token
		}
		if c.Done {
			usage = Usage{
				PromptTokens: c.PromptEvalCount,
				OutputTokens: c.EvalCount,
				DurationMs:   c.TotalDuration / int64(time.Millisecond),
			}
			if c.EvalDuration > 0 {
				usage.TokensPerSec = float64(c.EvalCount) / (float64(c.EvalDuration) / float64(time.Second))
			}
			return answer.String(), usage, nil
		}
	}
	switch {
	case stalled.Load():
		return "", usage, fmt.Errorf("ollama sent nothing within %s", a.opts.FirstToken)
	case ctx.Err() != nil:
		return "", usage, ctx.Err()
	case sc.Err() != nil:
		return "", usage, sc.Err()
	}
	return "", usage, errors.New("ollama closed the stream before finishing")
}

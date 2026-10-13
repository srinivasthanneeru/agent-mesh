// Command ollama puts a local Ollama model on the fabric, with conversation
// memory and token-by-token streaming.
//
//	go run ./cmd/ollama -model llama3.2 [-name llama] [-system "You are terse."] [-history 40]
//
// Talking to it:
//
//	@llama what is NATS?        normal prompt; it remembers earlier turns
//	@llama /reset               forget this conversation
//	@llama /history             show what it remembers
//
// Callers can keep separate memories by adding "conversation": "<id>" to
// the JSON envelope; without it everyone shares the "default" conversation.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"agentmesh/agent"
	"agentmesh/internal/env"
	"agentmesh/ollama"
	"agentmesh/protocol"
)

func main() {
	model := flag.String("model", "llama3.2", "Ollama model name")
	name := flag.String("name", "", "instance name (default: derived from the model)")
	owner := flag.String("owner", env.Owner(), "owner")
	url := flag.String("url", "", "Ollama URL (default $OLLAMA_HOST or http://127.0.0.1:11434)")
	system := flag.String("system", "", "system prompt")
	history := flag.Int("history", 40, "messages remembered per conversation")
	keepAlive := flag.String("keep-alive", "10m", "how long Ollama keeps the model loaded")
	think := flag.String("think", "", `reasoning for thinking models: "true", "false", or empty for the model default`)
	flag.Parse()

	if *name == "" {
		// "llama3.2:3b" → "llama3-2-3b"
		*name = protocol.SanitizeToken(strings.NewReplacer(":", "-", ".", "-").Replace(*model))
	}
	oa := ollama.New(ollama.Options{BaseURL: *url, Model: *model, System: *system, MaxHistory: *history, KeepAlive: *keepAlive, Think: *think})
	if *think != "" && *think != "true" && *think != "false" {
		log.Fatalf(`-think must be "true", "false" or empty, got %q`, *think)
	}

	// Warn early (but keep running) if Ollama is down or the model is missing:
	// the user can start Ollama or pull the model while the agent waits.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if models, err := oa.Models(ctx); err != nil {
		log.Printf("warning: %v", err)
	} else if !ollama.HasModel(models, *model) {
		log.Printf("warning: model %q is not installed; run: ollama pull %s (installed: %s)", *model, *model, strings.Join(models, ", "))
	}
	cancel()

	nc, err := env.Connect("ollama-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	h, err := agent.New(agent.Options{
		Conn:        nc,
		Agent:       "ollama",
		Owner:       *owner,
		Name:        *name,
		Session:     *name,
		Description: fmt.Sprintf("Ollama %s (local model, remembers the conversation)", *model),
		Metadata:    map[string]string{"model": *model, "harness": "ollama"},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			conv := conversationID(e)
			switch strings.TrimSpace(e.Prompt) {
			case "/reset":
				oa.Reset(conv)
				res.Send("Conversation forgotten.")
				return nil
			case "/history":
				res.Send(formatHistory(oa.History(conv)))
				return nil
			}
			u, err := oa.Chat(ctx, conv, e.Prompt, res)
			if errors.Is(err, ollama.ErrBusy) {
				return protocol.Errorf(409, "%v", err)
			}
			if err != nil {
				return err
			}
			res.Raw("usage", map[string]any{
				"model": *model, "prompt_tokens": u.PromptTokens, "output_tokens": u.OutputTokens,
				"tokens_per_sec": u.TokensPerSec, "duration_ms": u.DurationMs,
				"remembered": len(oa.History(conv)),
			})
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Start(); err != nil {
		log.Fatal(err)
	}
	sig, stop := env.SignalContext()
	defer stop()
	<-sig.Done()
	_ = h.Stop()
}

// conversationID reads the optional "conversation" field from the envelope.
// This is why the protocol keeps unknown fields: extensions like this one.
func conversationID(e protocol.Envelope) string {
	var id string
	if raw, ok := e.Extra["conversation"]; ok && json.Unmarshal(raw, &id) == nil && id != "" {
		return id
	}
	return "default"
}

func formatHistory(h []ollama.Message) string {
	if len(h) == 0 {
		return "(no memory yet)"
	}
	var b strings.Builder
	for _, m := range h {
		text := m.Content
		if len(text) > 120 {
			text = text[:120] + "…"
		}
		fmt.Fprintf(&b, "%-10s %s\n", m.Role+":", strings.ReplaceAll(text, "\n", " "))
	}
	return strings.TrimRight(b.String(), "\n")
}

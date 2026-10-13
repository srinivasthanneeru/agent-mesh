// Command claude puts a long-lived, headless Claude Code conversation on the
// fabric. It remembers the conversation across prompts, and across restarts:
// the Claude session id is saved in the NATS KV bucket "agent_mesh" (needs
// nats-server -js), so a restarted agent continues where it left off.
//
// Every reply ends with the session id and the command to continue the same
// conversation in a terminal (`claude --resume <id>`, run in -dir).
//
//	go run ./cmd/claude -dir ~/code/project [-name project] [-model sonnet]
//	                    [-mode default|acceptEdits|plan|bypassPermissions] [-tools Read,Glob,Grep]
//
// Commands you can send it:
//
//	/session   show the current session id and how to resume it
//	/new       start a new conversation (the old one stays resumable in Claude)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agentmesh/agent"
	"agentmesh/claude"
	"agentmesh/internal/env"
	"agentmesh/kvstore"
	"agentmesh/protocol"
)

func main() {
	cwd, _ := os.Getwd()
	dir := flag.String("dir", cwd, "directory Claude works in")
	name := flag.String("name", "", "instance name (default: the directory name)")
	owner := flag.String("owner", env.Owner(), "owner")
	model := flag.String("model", "", "model alias, e.g. sonnet")
	mode := flag.String("mode", "default", "permission mode")
	tools := flag.String("tools", "Read,Glob,Grep", "allowed tools, comma separated")
	flag.Parse()

	abs, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatal(err)
	}
	if *name == "" {
		*name = protocol.SanitizeToken(filepath.Base(abs))
	}

	nc, err := env.Connect("claude-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	// Where the session id is kept between restarts.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	store, warning := kvstore.Open(ctx, nc)
	cancel()
	if warning != "" {
		log.Printf("warning: %s", warning)
	}
	key := kvstore.Key("claude", *owner, *name)

	session := claude.NewSession(claude.Options{
		Dir: abs, Model: *model, PermissionMode: *mode, AllowedTools: strings.Split(*tools, ","),
		OnSession: func(id string) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var err error
			if id == "" {
				err = store.Delete(ctx, key)
			} else {
				err = store.Put(ctx, key, []byte(id))
			}
			if err != nil {
				log.Printf("warning: could not save session id: %v", err)
			}
		},
	})

	// Pick up the conversation from before the restart, if there was one.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	saved, err := store.Get(ctx, key)
	cancel()
	switch {
	case err != nil:
		log.Printf("warning: could not load saved session: %v", err)
	case saved != nil:
		session.Resume(string(saved))
		log.Printf("resuming Claude session %s (stored in %s)", saved, store.Kind())
	}

	h, err := agent.New(agent.Options{
		Conn:        nc,
		Agent:       "cc",
		AgentID:     "claude-code",
		Owner:       *owner,
		Name:        *name,
		Session:     *name,
		Description: "Claude Code in " + abs,
		Metadata:    map[string]string{"cwd": abs, "harness": "claude-code"},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			switch strings.TrimSpace(e.Prompt) {
			case "/session":
				if id := session.SessionID(); id != "" {
					res.Send(fmt.Sprintf("Session %s\nContinue it in a terminal:\n  %s", id, resumeCommand(abs, id)))
				} else {
					res.Send("No conversation yet: the next prompt starts one.")
				}
				return nil
			case "/new":
				old := session.SessionID()
				session.Forget()
				msg := "Started a new conversation."
				if old != "" {
					msg += "\nThe previous one is still in Claude Code:\n  " + resumeCommand(abs, old)
				}
				res.Send(msg)
				return nil
			}

			r, err := session.Run(ctx, e.Prompt, res)
			if errors.Is(err, claude.ErrBusy) {
				return protocol.Errorf(409, "%v", err)
			}
			if err != nil {
				return err
			}
			_, _, total, id := session.Stats()
			usage := map[string]any{"cost_usd": r.CostUSD, "total_cost_usd": total, "turns": r.Turns, "duration_ms": r.DurationMs}
			if id != "" {
				usage["session_id"] = id
				usage["resume"] = resumeCommand(abs, id)
			}
			res.Raw("usage", usage)
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Start(); err != nil {
		log.Fatal(err)
	}
	log.Printf("sessions stored in %s", store.Kind())

	sig, stop := env.SignalContext()
	defer stop()
	<-sig.Done()
	_ = h.Stop()
}

// resumeCommand is what to type to continue a conversation in Claude Code.
// Claude Code keeps sessions per directory, so the cd matters.
func resumeCommand(dir, id string) string {
	return "cd " + shellQuote(dir) + " && claude --resume " + id
}

// shellQuote wraps s in single quotes for the shell, so paths with spaces
// work. A single quote inside becomes '\” (close, escaped quote, reopen).
func shellQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " '\"\\$`!*?&;|<>()[]{}#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

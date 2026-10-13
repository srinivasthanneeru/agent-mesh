// Command claude puts a long-lived, headless Claude Code conversation on the
// fabric. It remembers the conversation across prompts.
//
//	go run ./cmd/claude -dir ~/code/project [-name project] [-model sonnet]
//	                    [-mode default|acceptEdits|plan|bypassPermissions] [-tools Read,Glob,Grep]
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"agentmesh/agent"
	"agentmesh/claude"
	"agentmesh/internal/env"
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

	session := claude.NewSession(claude.Options{
		Dir: abs, Model: *model, PermissionMode: *mode, AllowedTools: strings.Split(*tools, ","),
	})

	nc, err := env.Connect("claude-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

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
			r, err := session.Run(ctx, e.Prompt, res)
			if errors.Is(err, claude.ErrBusy) {
				return protocol.Errorf(409, "%v", err)
			}
			if err != nil {
				return err
			}
			_, _, total, _ := session.Stats()
			res.Raw("usage", map[string]any{"cost_usd": r.CostUSD, "total_cost_usd": total, "turns": r.Turns, "duration_ms": r.DurationMs})
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Start(); err != nil {
		log.Fatal(err)
	}
	ctx, stop := env.SignalContext()
	defer stop()
	<-ctx.Done()
	_ = h.Stop()
}

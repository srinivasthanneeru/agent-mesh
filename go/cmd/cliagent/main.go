// Command cliagent wraps ANY command-line tool that takes a prompt and
// prints an answer. The prompt is appended as the last argument (or piped to
// stdin with -stdin). This is how heterogeneous tools join the fabric.
//
//	go run ./cmd/cliagent -agent ollama -name llama -cmd "ollama run llama3.2"
//	go run ./cmd/cliagent -agent codex -cmd "codex exec"
//	go run ./cmd/cliagent -agent gemini -cmd "gemini -p"
package main

import (
	"context"
	"flag"
	"log"
	"os/exec"
	"strings"
	"time"

	"agentmesh/agent"
	"agentmesh/internal/env"
	"agentmesh/protocol"
)

type responderWriter struct{ res *agent.Responder }

func (w responderWriter) Write(p []byte) (int, error) {
	w.res.Send(string(p))
	return len(p), nil
}

func main() {
	command := flag.String("cmd", "", `command to run, e.g. "ollama run llama3.2"`)
	token := flag.String("agent", "", "agent token (default: the command name)")
	name := flag.String("name", "main", "instance name")
	owner := flag.String("owner", env.Owner(), "owner")
	dir := flag.String("dir", "", "working directory")
	stdin := flag.Bool("stdin", false, "pipe the prompt to stdin instead of passing it as an argument")
	flag.Parse()

	fields := strings.Fields(*command)
	if len(fields) == 0 {
		log.Fatal(`usage: cliagent -cmd "<command>" [-agent token] [-name name] [-stdin]`)
	}
	if *token == "" {
		*token = fields[0]
	}
	*token = protocol.SanitizeToken(*token)

	nc, err := env.Connect(*token + "-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	h, err := agent.New(agent.Options{
		Conn:        nc,
		Agent:       *token,
		Owner:       *owner,
		Name:        *name,
		Description: "CLI wrapper: " + *command,
		Metadata:    map[string]string{"cmd": *command},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			args := fields[1:]
			if !*stdin {
				// Copy before appending so concurrent prompts never share a backing array.
				args = append(append([]string{}, args...), e.Prompt)
			}
			cmd := exec.CommandContext(ctx, fields[0], args...)
			cmd.Dir = *dir
			if *stdin {
				cmd.Stdin = strings.NewReader(e.Prompt)
			}
			var stderr strings.Builder
			cmd.Stdout, cmd.Stderr = responderWriter{res}, &stderr
			if err := cmd.Run(); err != nil {
				lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
				return protocol.Errorf(500, "%s: %v: %s", fields[0], err, lines[len(lines)-1])
			}
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

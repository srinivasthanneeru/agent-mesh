// Command shell runs each prompt as a shell command in a fixed directory,
// streaming its output. Before running, it pauses the stream with a §7
// mid-stream query and waits for the caller to approve.
//
//	go run ./cmd/shell [-dir /path] [-name box1] [-yes]   (-yes skips approval)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"agentmesh/agent"
	"agentmesh/internal/env"
	"agentmesh/protocol"
)

// responderWriter adapts a Responder to io.Writer, so a process's output can
// be copied straight into the response stream.
type responderWriter struct{ res *agent.Responder }

func (w responderWriter) Write(p []byte) (int, error) {
	w.res.Send(string(p))
	return len(p), nil
}

func main() {
	cwd, _ := os.Getwd()
	host, _ := os.Hostname()
	dir := flag.String("dir", cwd, "directory to run commands in")
	name := flag.String("name", protocol.SanitizeToken(strings.Split(host, ".")[0]), "instance name")
	owner := flag.String("owner", env.Owner(), "owner")
	yes := flag.Bool("yes", false, "run without asking for approval")
	flag.Parse()

	nc, err := env.Connect("shell-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	yesRE := regexp.MustCompile(`(?i)^y(es)?$`)
	h, err := agent.New(agent.Options{
		Conn:        nc,
		Agent:       "shell",
		Owner:       *owner,
		Name:        *name,
		Description: fmt.Sprintf("Runs shell commands in %s (asks before running)", *dir),
		Metadata:    map[string]string{"cwd": *dir, "host": host},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			command := strings.TrimSpace(e.Prompt)
			if !*yes {
				answer, ok := res.Ask(ctx, fmt.Sprintf("Run `%s` in %s on %s? (yes/no)", command, *dir, host), 2*time.Minute)
				if !ok {
					return protocol.Errorf(409, "no approval received (timed out)")
				}
				if !yesRE.MatchString(strings.TrimSpace(answer)) {
					res.Send("not run: caller declined.")
					return nil
				}
			}
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
			cmd.Dir = *dir
			w := responderWriter{res}
			cmd.Stdout, cmd.Stderr = w, w
			if err := cmd.Run(); err != nil {
				// errors.As checks whether err (or anything it wraps) is an *exec.ExitError.
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					res.Send(fmt.Sprintf("\n[exit %d]", exitErr.ExitCode()))
					return nil
				}
				return err
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

var _ io.Writer = responderWriter{} // compile-time interface check

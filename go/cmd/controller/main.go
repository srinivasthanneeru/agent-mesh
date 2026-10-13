// Command controller is a control-plane agent at agents.{verb}.cc-pool.{owner}.control
// with three extension verbs:
//
//	agents.spawn.cc-pool.{owner}.control  {"cwd", "name"?, "model"?, "permission_mode"?, "tools"?, "max_lifetime_s"?}
//	agents.stop.cc-pool.{owner}.control   {"session_id"}
//	agents.list.cc-pool.{owner}.control   {}
//
// Each spawned session is its own agent at agents.prompt.cc-pool.{owner}.{session}:
// discoverable, promptable, heartbeating, and gone when stopped or expired.
//
//	go run ./cmd/controller [-owner you] [-root ~/code]   (-root limits allowed directories)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"agentmesh/agent"
	"agentmesh/claude"
	"agentmesh/internal/env"
	"agentmesh/protocol"
)

const token = "cc-pool"

type managed struct {
	id        string
	host      *agent.Host
	session   *claude.Session
	dir       string
	createdAt time.Time
	expiresAt time.Time   // zero value = never
	timer     *time.Timer // nil when there is no lifetime
}

// pool is the controller's state. The mutex protects the map because NATS
// handlers and expiry timers run on different goroutines.
type pool struct {
	nc       *nats.Conn
	owner    string
	root     string
	ctlSubj  string
	mu       sync.Mutex
	sessions map[string]*managed
}

type spawnRequest struct {
	Cwd            string   `json:"cwd"`
	Name           string   `json:"name"`
	Model          string   `json:"model"`
	PermissionMode string   `json:"permission_mode"`
	Tools          []string `json:"tools"`
	MaxLifetimeS   int      `json:"max_lifetime_s"`
}

func main() {
	owner := flag.String("owner", env.Owner(), "owner")
	root := flag.String("root", "", "only allow sessions under this directory")
	flag.Parse()

	nc, err := env.Connect("cc-pool-controller")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	p := &pool{nc: nc, owner: *owner, sessions: map[string]*managed{}}
	if *root != "" {
		p.root, _ = filepath.Abs(*root)
	}

	ctl, err := agent.New(agent.Options{
		Conn: nc, Agent: token, Owner: *owner, Name: "control",
		Description: "Spawns and manages headless Claude Code sessions",
		Metadata:    map[string]string{"role": "controller"},
		Heartbeat:   5 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			res.Send(p.help())
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := ctl.Start(); err != nil {
		log.Fatal(err)
	}
	p.ctlSubj = ctl.Subjects().Prompt

	// Method values: p.spawn is a function bound to p, usable as an ExtensionFunc.
	for verb, fn := range map[string]agent.ExtensionFunc{"spawn": p.spawn, "stop": p.stopHandler, "list": p.list} {
		if _, err := ctl.AddEndpoint(verb, fn); err != nil {
			log.Fatal(err)
		}
	}

	ctx, stop := env.SignalContext()
	defer stop()
	<-ctx.Done()
	for _, id := range p.ids() {
		p.stopSession(id)
	}
	_ = ctl.Stop()
}

func (p *pool) help() string {
	var b strings.Builder
	fmt.Fprintf(&b, "cc-pool controller. Sessions: %d\n", len(p.ids()))
	fmt.Fprintf(&b, `spawn: agents.spawn.%s.%s.control {"cwd": "/abs/path", "name"?, "model"?, "permission_mode"?, "tools"?, "max_lifetime_s"?}`+"\n", token, p.owner)
	fmt.Fprintf(&b, `stop:  agents.stop.%s.%s.control {"session_id": "..."}`+"\n", token, p.owner)
	fmt.Fprintf(&b, "list:  agents.list.%s.%s.control", token, p.owner)
	return b.String()
}

func (p *pool) ids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.sessions))
	for id := range p.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (p *pool) spawn(req micro.Request) (any, error) {
	var r spawnRequest
	if err := json.Unmarshal(req.Data(), &r); err != nil {
		return nil, protocol.Errorf(400, "body must be a JSON object")
	}
	dir, err := filepath.Abs(r.Cwd)
	if r.Cwd == "" || err != nil {
		return nil, protocol.Errorf(400, "cwd is required")
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return nil, protocol.Errorf(400, "cwd is not a directory: %s", dir)
	}
	if p.root != "" && dir != p.root && !strings.HasPrefix(dir, p.root+string(filepath.Separator)) {
		return nil, protocol.Errorf(403, "cwd must be under %s", p.root)
	}

	id := "s-" + agent.NewID()[:8]
	if r.Name != "" {
		id = protocol.SanitizeToken(r.Name)
	}
	if !protocol.ValidToken(id) {
		return nil, protocol.Errorf(400, "invalid session name")
	}
	if r.PermissionMode == "" {
		r.PermissionMode = "default"
	}
	if len(r.Tools) == 0 {
		r.Tools = []string{"Read", "Glob", "Grep"}
	}

	p.mu.Lock()
	_, exists := p.sessions[id]
	if !exists {
		p.sessions[id] = &managed{id: id} // reserve the name while we set up
	}
	p.mu.Unlock()
	if exists {
		return nil, protocol.Errorf(409, "session %s already exists", id)
	}

	session := claude.NewSession(claude.Options{Dir: dir, Model: r.Model, PermissionMode: r.PermissionMode, AllowedTools: r.Tools})
	host, err := agent.New(agent.Options{
		Conn: p.nc, Agent: token, AgentID: "claude-code", Owner: p.owner, Name: id, Session: id,
		Description: "Claude Code session in " + dir,
		Metadata:    map[string]string{"cwd": dir, "role": "session", "controller": p.ctlSubj},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			r, err := session.Run(ctx, e.Prompt, res)
			if errors.Is(err, claude.ErrBusy) {
				return protocol.Errorf(409, "session is busy")
			}
			if err != nil {
				return err
			}
			_, _, total, _ := session.Stats()
			res.Raw("usage", map[string]any{"cost_usd": r.CostUSD, "total_cost_usd": total, "turns": r.Turns})
			return nil
		},
	})
	if err == nil {
		err = host.Start()
	}
	if err != nil {
		p.mu.Lock()
		delete(p.sessions, id)
		p.mu.Unlock()
		return nil, err
	}

	m := &managed{id: id, host: host, session: session, dir: dir, createdAt: time.Now()}
	if r.MaxLifetimeS > 0 {
		life := time.Duration(r.MaxLifetimeS) * time.Second
		m.expiresAt = m.createdAt.Add(life)
		m.timer = time.AfterFunc(life, func() { p.stopSession(id) })
	}
	p.mu.Lock()
	p.sessions[id] = m
	p.mu.Unlock()

	s := host.Subjects()
	return map[string]string{"session_id": id, "subject": s.Prompt, "status_subject": s.Status, "heartbeat_subject": s.Heartbeat, "cwd": dir}, nil
}

func (p *pool) stopHandler(req micro.Request) (any, error) {
	var r struct {
		SessionID string `json:"session_id"`
	}
	_ = json.Unmarshal(req.Data(), &r)
	if !p.stopSession(r.SessionID) {
		return nil, protocol.Errorf(404, "no session %q", r.SessionID)
	}
	return map[string]any{"ok": true, "session_id": r.SessionID}, nil
}

// stopSession removes a session from the map and from the network.
func (p *pool) stopSession(id string) bool {
	p.mu.Lock()
	m, ok := p.sessions[id]
	if ok {
		delete(p.sessions, id)
	}
	p.mu.Unlock()
	if !ok || m.host == nil {
		return ok
	}
	if m.timer != nil {
		m.timer.Stop()
	}
	_ = m.host.Stop() // cancels any running turn and deregisters the service
	log.Printf("[cc-pool] stopped %s", id)
	return true
}

type sessionInfo struct {
	SessionID          string  `json:"session_id"`
	Cwd                string  `json:"cwd"`
	Subject            string  `json:"subject"`
	Busy               bool    `json:"busy"`
	Turns              int     `json:"turns"`
	TotalCostUSD       float64 `json:"total_cost_usd"`
	AgeS               int     `json:"age_s"`
	RemainingLifetimeS *int    `json:"remaining_lifetime_s"` // a pointer so "no limit" encodes as null
}

func (p *pool) list(micro.Request) (any, error) {
	out := []sessionInfo{}
	for _, id := range p.ids() {
		p.mu.Lock()
		m := p.sessions[id]
		p.mu.Unlock()
		if m == nil || m.host == nil {
			continue
		}
		busy, turns, cost, _ := m.session.Stats()
		info := sessionInfo{SessionID: id, Cwd: m.dir, Subject: m.host.Subjects().Prompt, Busy: busy, Turns: turns,
			TotalCostUSD: cost, AgeS: int(time.Since(m.createdAt).Seconds())}
		if !m.expiresAt.IsZero() {
			left := max(0, int(time.Until(m.expiresAt).Seconds())) // max is built in since Go 1.21
			info.RemainingLifetimeS = &left
		}
		out = append(out, info)
	}
	return map[string]any{"sessions": out}, nil
}

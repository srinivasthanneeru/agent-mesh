// Package meshmcp exposes the agent mesh as an MCP (Model Context Protocol)
// server, so any MCP client (Claude Desktop, Claude Code, Codex, ...) can
// see and continue the same sessions the mesh agents keep in NATS.
//
// Tools:
//
//	mesh_agents    which agents are online
//	mesh_ask       prompt one or more agents (optionally continuing a conversation)
//	mesh_sessions  list saved sessions (Ollama conversations, Claude Code sessions)
//	mesh_session   show one saved session in full
//
// The sessions themselves stay where the agents put them (the "agent_mesh"
// JetStream KV bucket, and Claude Code's own files); this package only reads
// them and talks to agents through the normal protocol, so every app sees
// one shared truth.
package meshmcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"agentmesh/claude"
	"agentmesh/kvstore"
	"agentmesh/mesh"
	"agentmesh/protocol"
)

// Version is reported to MCP clients.
const Version = "0.1.0"

// maxReply caps how much of one agent's answer is returned, to keep tool
// results a reasonable size for the calling model.
const maxReply = 20000

type server struct {
	mesh  *mesh.Mesh
	store kvstore.Store
	// readTranscript is a field (not a direct call) so tests can replace it.
	readTranscript func(id string) (claude.Transcript, error)
}

// New builds the MCP server. Run it with server.Run(ctx, &mcp.StdioTransport{}).
func New(m *mesh.Mesh, store kvstore.Store) *mcp.Server {
	return newServer(m, store, claude.ReadTranscript)
}

// newServer lets tests supply their own transcript reader.
func newServer(m *mesh.Mesh, store kvstore.Store, readTranscript func(string) (claude.Transcript, error)) *mcp.Server {
	s := &server{mesh: m, store: store, readTranscript: readTranscript}
	srv := mcp.NewServer(&mcp.Implementation{Name: "agent-mesh", Version: Version}, nil)

	mcp.AddTool(srv, &mcp.Tool{
		Name:        "mesh_agents",
		Description: "List the AI agents currently online on the agent mesh (Claude Code, local Ollama models, shell, ...), with what each one does.",
	}, s.agents)
	mcp.AddTool(srv, &mcp.Tool{
		Name: "mesh_ask",
		Description: "Send a prompt to one or more mesh agents and return their answers. " +
			"Agents remember earlier turns: the Claude agent continues its Claude Code session, " +
			"and an Ollama agent continues the given conversation (default \"default\"). " +
			"Questions an agent asks mid-answer (such as permission to run a command) are declined automatically.",
	}, s.ask)
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "mesh_sessions",
		Description: "List saved mesh sessions: Ollama conversations and Claude Code sessions, with a preview of each.",
	}, s.sessions)
	mcp.AddTool(srv, &mcp.Tool{
		Name:        "mesh_session",
		Description: "Show one saved mesh session in full. For Claude Code sessions, also gives the command to continue it in Claude Code.",
	}, s.session)
	return srv
}

// text wraps a string as a tool result.
func text(s string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}
}

// fail reports a problem to the calling model as a tool error (IsError),
// rather than a protocol error, so the model can read it and react.
func fail(format string, args ...any) *mcp.CallToolResult {
	r := text(fmt.Sprintf(format, args...))
	r.IsError = true
	return r
}

// ---------------------------------------------------------------------------
// mesh_agents

func (s *server) agents(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
	all, err := s.mesh.Discover(750 * time.Millisecond)
	if err != nil {
		return fail("discovery failed: %v", err), nil, nil
	}
	agents := mesh.UniqueBySubject(all)
	if len(agents) == 0 {
		return text("No agents are online. Start some with go/scripts/demo.sh."), nil, nil
	}
	var b strings.Builder
	for _, a := range agents {
		role := ""
		if r := a.Metadata["role"]; r != "" {
			role = " [" + r + "]"
		}
		fmt.Fprintf(&b, "- %s%s: %s\n", a.Label(), role, a.Description)
	}
	b.WriteString("\nUse mesh_ask with a target such as \"ollama\", \"cc\", a full label, or several separated by commas.")
	return text(b.String()), nil, nil
}

// ---------------------------------------------------------------------------
// mesh_ask

// AskInput is the mesh_ask input. The struct tags become the tool's JSON
// schema: `json` gives the field names, `jsonschema` the descriptions.
type AskInput struct {
	Target       string `json:"target" jsonschema:"which agents: a token like ollama or cc, a name, a full label like ollama/srini/llama3-2, several separated by commas, or all"`
	Prompt       string `json:"prompt" jsonschema:"what to send"`
	Conversation string `json:"conversation,omitempty" jsonschema:"conversation id for agents that keep several (Ollama); omit for the default one"`
}

// reply collects one agent's streamed answer.
type reply struct {
	label    string
	text     strings.Builder
	notes    []string
	finished bool
}

func (s *server) ask(ctx context.Context, req *mcp.CallToolRequest, in AskInput) (*mcp.CallToolResult, any, error) {
	if strings.TrimSpace(in.Target) == "" || strings.TrimSpace(in.Prompt) == "" {
		return fail("target and prompt are required"), nil, nil
	}
	all, err := s.mesh.Discover(750 * time.Millisecond)
	if err != nil {
		return fail("discovery failed: %v", err), nil, nil
	}
	targets := mesh.MatchTargets(all, in.Target)
	if len(targets) == 0 {
		var names []string
		for _, a := range mesh.UniqueBySubject(all) {
			names = append(names, a.Label())
		}
		return fail("no agent matches %q. Online: %s", in.Target, strings.Join(names, ", ")), nil, nil
	}

	opts := mesh.PromptOptions{Inactivity: 2 * time.Minute}
	if in.Conversation != "" {
		opts.Extra = map[string]any{"conversation": in.Conversation} // unknown envelope fields reach the agent
	}

	replies := map[string]*reply{}
	var order []string
	for _, a := range targets {
		replies[a.Label()] = &reply{label: a.Label()}
		order = append(order, a.Label())
	}
	done := 0 // only this loop touches it: Fanout delivers events to one goroutine

	for ev := range s.mesh.Fanout(ctx, targets, in.Prompt, opts) {
		r := replies[ev.From.Label()]
		switch ev.Type {
		case mesh.EvText:
			r.text.WriteString(ev.Text)
		case mesh.EvQuery:
			// Safety: an MCP caller is another AI, so never approve on the user's behalf.
			ev.Answer("no")
			r.notes = append(r.notes, "asked \""+ev.Query.Prompt+"\": declined automatically (approve it in the mesh console: bin/mesh chat)")
		case mesh.EvError:
			r.notes = append(r.notes, fmt.Sprintf("error %d: %s", ev.Code, ev.Message))
		case mesh.EvTimeout:
			r.notes = append(r.notes, "stopped: no output for 2 minutes")
		case mesh.EvUnknown:
			if note := describe(ev.Chunk); note != "" {
				r.notes = append(r.notes, note)
			}
		case mesh.EvDone:
			r.finished = true
			done++
			if token := req.Params.GetProgressToken(); token != nil {
				_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
					ProgressToken: token, Progress: float64(done), Total: float64(len(targets)),
					Message: r.label + " finished",
				})
			}
		}
	}

	var b strings.Builder
	for _, label := range order {
		r := replies[label]
		fmt.Fprintf(&b, "### %s\n", label)
		answer := strings.TrimSpace(r.text.String())
		if len(answer) > maxReply {
			answer = answer[:maxReply] + "\n…(truncated)"
		}
		if answer != "" {
			b.WriteString(answer + "\n")
		}
		for _, n := range r.notes {
			b.WriteString("_" + n + "_\n")
		}
		if !r.finished && ctx.Err() == nil {
			b.WriteString("_(stream ended without finishing)_\n")
		}
		b.WriteString("\n")
	}
	return text(strings.TrimSpace(b.String())), nil, nil
}

// describe turns our custom chunk types into a one-line note (or "" to skip).
func describe(c *protocol.Chunk) string {
	var d map[string]any
	_ = json.Unmarshal(c.Data, &d)
	switch c.Type {
	case "tool":
		return fmt.Sprintf("used tool %v", d["name"])
	case "usage":
		if resume, ok := d["resume"].(string); ok {
			return "continue in Claude Code: " + resume
		}
		if _, ok := d["output_tokens"]; ok {
			return fmt.Sprintf("%v, %v tokens, remembers %v messages", d["model"], d["output_tokens"], d["remembered"])
		}
	case "status_note":
		return fmt.Sprint(d["message"])
	}
	return "" // thinking, plan, ...: not useful in a summary
}

// ---------------------------------------------------------------------------
// mesh_sessions / mesh_session

// sessionInfo is one saved session, decoded from its KV key and value.
type sessionInfo struct {
	Key          string // the id callers pass to mesh_session
	Kind         string // "ollama" or "claude"
	Agent        string // mesh label, e.g. ollama/srini/llama3-2
	Conversation string // Ollama conversation id
	ClaudeID     string // Claude Code session id
}

func parseKey(key string) sessionInfo {
	p := strings.Split(key, ".")
	switch {
	case len(p) == 4 && p[0] == "ollama":
		return sessionInfo{Key: key, Kind: "ollama", Agent: "ollama/" + p[1] + "/" + p[2], Conversation: p[3]}
	case len(p) == 3 && p[0] == "claude":
		return sessionInfo{Key: key, Kind: "claude", Agent: "cc/" + p[1] + "/" + p[2]}
	}
	return sessionInfo{Key: key, Kind: "other"}
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func preview(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ") // collapse newlines and runs of spaces
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// SessionsInput filters mesh_sessions.
type SessionsInput struct {
	Agent string `json:"agent,omitempty" jsonschema:"only sessions of agents whose label contains this text, e.g. ollama or cc"`
}

func (s *server) sessions(ctx context.Context, _ *mcp.CallToolRequest, in SessionsInput) (*mcp.CallToolResult, any, error) {
	keys, err := s.store.Keys(ctx, "")
	if err != nil {
		return fail("could not list sessions: %v", err), nil, nil
	}
	var b strings.Builder
	n := 0
	for _, key := range keys {
		info := parseKey(key)
		if info.Kind == "other" || (in.Agent != "" && !strings.Contains(info.Agent, in.Agent)) {
			continue
		}
		val, err := s.store.Get(ctx, key)
		if err != nil || val == nil {
			continue
		}
		n++
		switch info.Kind {
		case "ollama":
			var msgs []message
			_ = json.Unmarshal(val, &msgs)
			first, last := "", ""
			for _, m := range msgs {
				if m.Role == "user" {
					if first == "" {
						first = m.Content
					}
					last = m.Content
				}
			}
			fmt.Fprintf(&b, "- **%s** (%s, conversation %q): %d messages. First: %q. Last: %q\n",
				key, info.Agent, info.Conversation, len(msgs), preview(first, 60), preview(last, 60))
		case "claude":
			id := string(val)
			t, terr := s.readTranscript(id)
			line := fmt.Sprintf("- **%s** (%s): Claude Code session %s", key, info.Agent, id)
			if terr == nil {
				first := ""
				for _, turn := range t.Turns {
					if turn.Role == "user" {
						first = turn.Text
						break
					}
				}
				line += fmt.Sprintf(", %d turns, first: %q", len(t.Turns), preview(first, 60))
			}
			b.WriteString(line + "\n")
		}
	}
	if n == 0 {
		msg := "No saved sessions yet."
		if strings.HasPrefix(s.store.Kind(), "memory") {
			msg += " (The NATS server has no JetStream, so nothing is being saved: start nats-server with -js.)"
		}
		return text(msg), nil, nil
	}
	b.WriteString("\nShow one with mesh_session; continue one with mesh_ask (same agent, and for Ollama the same conversation).")
	return text(b.String()), nil, nil
}

// SessionInput selects one session.
type SessionInput struct {
	Session string `json:"session" jsonschema:"session id as listed by mesh_sessions, e.g. ollama.srini.llama3-2.default or claude.srini.agent-mesh"`
}

func (s *server) session(ctx context.Context, _ *mcp.CallToolRequest, in SessionInput) (*mcp.CallToolResult, any, error) {
	info := parseKey(in.Session)
	if info.Kind == "other" {
		return fail("unknown session id %q: use one listed by mesh_sessions", in.Session), nil, nil
	}
	val, err := s.store.Get(ctx, in.Session)
	if err != nil {
		return fail("could not read session: %v", err), nil, nil
	}
	if val == nil {
		return fail("no saved session %q", in.Session), nil, nil
	}

	var b strings.Builder
	switch info.Kind {
	case "ollama":
		var msgs []message
		if err := json.Unmarshal(val, &msgs); err != nil {
			return fail("session %q is unreadable: %v", in.Session, err), nil, nil
		}
		fmt.Fprintf(&b, "Ollama conversation %q with %s (%d messages)\n\n", info.Conversation, info.Agent, len(msgs))
		for _, m := range msgs {
			fmt.Fprintf(&b, "**%s:** %s\n\n", m.Role, m.Content)
		}
		fmt.Fprintf(&b, "Continue it: mesh_ask with target %q and conversation %q.", info.Agent, info.Conversation)

	case "claude":
		id := string(val)
		t, err := s.readTranscript(id)
		fmt.Fprintf(&b, "Claude Code session %s (agent %s)\n\n", id, info.Agent)
		if err != nil {
			fmt.Fprintf(&b, "_Transcript not readable on this machine: %v_\n\n", err)
		}
		for _, turn := range t.Turns {
			fmt.Fprintf(&b, "**%s:** %s", turn.Role, turn.Text)
			if len(turn.Tools) > 0 {
				fmt.Fprintf(&b, " _(tools: %s)_", strings.Join(dedupe(turn.Tools), ", "))
			}
			b.WriteString("\n\n")
		}
		fmt.Fprintf(&b, "Continue it through the mesh: mesh_ask with target %q.", info.Agent)
		if t.Dir != "" {
			fmt.Fprintf(&b, "\nOr in Claude Code: cd %s && claude --resume %s", shellQuote(t.Dir), id)
		}
	}
	return text(b.String()), nil, nil
}

func dedupe(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

func shellQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " '\"\\$`!*?&;|<>()[]{}#~") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

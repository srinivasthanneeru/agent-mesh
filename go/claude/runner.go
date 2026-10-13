// Package claude drives the Claude Code CLI headlessly
// (`claude -p --output-format stream-json`) and translates its event stream
// into protocol chunks.
//
// A Session is one conversation: after a successful turn it remembers
// Claude's session_id and later turns pass --resume, so the agent keeps its
// memory. Claude never knows about NATS; this package is the translator.
package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"agentmesh/protocol"
)

// Sink is where a turn's output goes. *agent.Responder satisfies it, but so
// does any type with these two methods: Go interfaces are satisfied
// implicitly, there's no "implements" keyword. That keeps this package
// independent of NATS and easy to test.
type Sink interface {
	Send(text string, attachments ...protocol.Attachment)
	Raw(typ string, data any)
}

// Options configures a Session.
type Options struct {
	Dir            string   // working directory Claude operates in
	Model          string   // e.g. "sonnet"; empty = CLI default
	PermissionMode string   // default | acceptEdits | bypassPermissions | plan
	AllowedTools   []string // e.g. Read, Glob, Grep
	Bin            string   // defaults to $CLAUDE_BIN or "claude"
	// FirstOutput is how long to wait for Claude to start answering before
	// giving up. The CLI retries silently forever on some failures (for
	// example an expired login). Default: $CLAUDE_FIRST_OUTPUT_S or 90s.
	FirstOutput time.Duration
	// OnSession, if set, is called whenever the Claude session id changes:
	// with the new id after a successful turn, or "" when the conversation
	// is dropped. Agents use it to save the id so a restart can resume.
	OnSession func(id string)
}

// Result summarises one turn.
type Result struct {
	CostUSD    float64
	DurationMs int64
	Turns      int
	IsError    bool
	ErrorText  string
	// failedToStart: the CLI failed before any turn ran (e.g. --resume of an unknown session).
	failedToStart bool
}

// ErrBusy is returned when a turn is already running in this session.
var ErrBusy = errors.New("session is already working on a prompt")

// Session is one Claude Code conversation. Its methods are safe to call from
// several goroutines; mu guards the fields below it.
type Session struct {
	opts Options

	mu        sync.Mutex
	busy      bool
	sessionID string
	totalCost float64
	turns     int
}

// NewSession creates a session; nothing runs until Run.
func NewSession(opts Options) *Session {
	if opts.Bin == "" {
		opts.Bin = envOr("CLAUDE_BIN", "claude")
	}
	if opts.FirstOutput == 0 {
		s, _ := strconv.Atoi(envOr("CLAUDE_FIRST_OUTPUT_S", "90"))
		opts.FirstOutput = time.Duration(s) * time.Second
	}
	return &Session{opts: opts}
}

// Stats returns a consistent snapshot of the session's counters.
func (s *Session) Stats() (busy bool, turns int, totalCost float64, sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.busy, s.turns, s.totalCost, s.sessionID
}

// SessionID is the Claude conversation the next turn will continue ("" = new).
func (s *Session) SessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionID
}

// Resume makes the next turn continue an existing Claude conversation, for
// example one saved before the agent restarted. If Claude no longer has it,
// Run falls back to a fresh conversation by itself.
func (s *Session) Resume(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionID = id
}

// Forget makes the next turn start a new conversation.
func (s *Session) Forget() { s.setSessionID("") }

// setSessionID updates the id and, if it changed, tells OnSession. The
// callback runs after the lock is released so it may take its time (and
// could even call back into the Session without deadlocking).
func (s *Session) setSessionID(id string) {
	s.mu.Lock()
	changed := s.sessionID != id
	s.sessionID = id
	s.mu.Unlock()
	if changed && s.opts.OnSession != nil {
		s.opts.OnSession(id)
	}
}

// Run executes one turn, streaming output into sink.
func (s *Session) Run(ctx context.Context, prompt string, sink Sink) (Result, error) {
	s.mu.Lock()
	if s.busy {
		s.mu.Unlock()
		return Result{}, ErrBusy
	}
	s.busy = true
	resumed := s.sessionID != ""
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.busy = false
		s.mu.Unlock()
	}()

	r, err := s.exec(ctx, prompt, sink)
	// A conversation that can't be resumed (never saved, deleted, other
	// machine) fails before any turn runs: start a fresh one instead.
	if err == nil && resumed && r.failedToStart && ctx.Err() == nil {
		s.setSessionID("")
		sink.Raw("status_note", map[string]string{"message": "previous conversation unavailable; starting a new one"})
		r, err = s.exec(ctx, prompt, sink)
	}
	if err == nil && r.failedToStart {
		sink.Send("[claude error] " + r.ErrorText)
	}
	return r, err
}

// nestingEnv matches variables that make a child `claude` think it is
// running inside another Claude session.
var nestingEnv = regexp.MustCompile(`^(CLAUDECODE|CLAUDE_CODE_.*|CLAUDE_AGENT_SDK_.*|CLAUDE_PID|CLAUDE_EFFORT)$`)

func cleanEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if !nestingEnv.MatchString(k) {
			env = append(env, kv)
		}
	}
	return env
}

// event is the subset of Claude's stream-json lines we care about. Fields
// that don't appear in a given line are simply left at their zero value.
type event struct {
	Type      string   `json:"type"`
	Subtype   string   `json:"subtype"`
	SessionID string   `json:"session_id"`
	IsError   bool     `json:"is_error"`
	Result    string   `json:"result"`
	CostUSD   float64  `json:"total_cost_usd"`
	Duration  int64    `json:"duration_ms"`
	NumTurns  int      `json:"num_turns"`
	Errors    []string `json:"errors"`
	Event     struct {
		Type  string `json:"type"`
		Delta struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
	} `json:"event"`
	Message struct {
		Content []struct {
			Type  string         `json:"type"`
			Text  string         `json:"text"`
			Name  string         `json:"name"`
			Input map[string]any `json:"input"`
		} `json:"content"`
	} `json:"message"`
}

func (s *Session) exec(ctx context.Context, prompt string, sink Sink) (Result, error) {
	s.mu.Lock()
	resume := s.sessionID
	s.mu.Unlock()

	args := []string{"-p", prompt, "--output-format", "stream-json", "--verbose", "--include-partial-messages"}
	if resume != "" {
		args = append(args, "--resume", resume)
	}
	if s.opts.Model != "" {
		args = append(args, "--model", s.opts.Model)
	}
	if s.opts.PermissionMode != "" {
		args = append(args, "--permission-mode", s.opts.PermissionMode)
	}
	if len(s.opts.AllowedTools) > 0 {
		args = append(args, "--allowedTools", strings.Join(s.opts.AllowedTools, ","))
	}

	// CommandContext stops the process if ctx is cancelled (e.g. the host
	// stops); Cancel says how: kill the whole process group.
	cmd := exec.CommandContext(ctx, s.opts.Bin, args...)
	cmd.Dir = s.opts.Dir
	cmd.Env = cleanEnv()
	setProcessGroup(cmd)
	cmd.Cancel = func() error { return killTree(cmd) }
	cmd.WaitDelay = 5 * time.Second // after a kill, stop waiting on pipes held by stragglers
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, err
	}
	var stderr tailBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("cannot start claude CLI: %w", err)
	}

	// Watchdog: kill the process if the model never starts answering.
	stalled := false
	var stallMu sync.Mutex
	watchdog := time.AfterFunc(s.opts.FirstOutput, func() {
		stallMu.Lock()
		stalled = true
		stallMu.Unlock()
		_ = killTree(cmd)
	})
	defer watchdog.Stop()

	var (
		res            Result
		sawDelta       bool
		sent           strings.Builder // everything we've sent, to avoid repeating an error
		pendingSession string
		gotResult      bool
	)
	send := func(t string) {
		sent.WriteString(t)
		sink.Send(t)
	}

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20) // lines can be large (tool results)
	for sc.Scan() {
		var ev event
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		if ev.Type != "system" {
			watchdog.Stop() // the model is answering; no more stall risk
		}
		switch ev.Type {
		case "system":
			// Adopted only when the turn succeeds; a failed turn may never be saved.
			if ev.Subtype == "init" && ev.SessionID != "" {
				pendingSession = ev.SessionID
			}
		case "stream_event": // token-level streaming
			if ev.Event.Type == "message_start" {
				sawDelta = false
			}
			if ev.Event.Type == "content_block_delta" && ev.Event.Delta.Type == "text_delta" {
				sawDelta = true
				send(ev.Event.Delta.Text)
			}
		case "assistant":
			for _, b := range ev.Message.Content {
				switch {
				case b.Type == "tool_use":
					// Not a standard chunk type: protocol callers that don't know it skip it (§6.6).
					sink.Raw("tool", map[string]any{"name": b.Name, "input": summarize(b.Input)})
				case b.Type == "text" && !sawDelta && b.Text != "":
					send(b.Text) // older CLIs, and the CLI's own error messages
				}
			}
		case "result":
			gotResult = true
			res.IsError, res.CostUSD, res.DurationMs, res.Turns = ev.IsError, ev.CostUSD, ev.Duration, ev.NumTurns
			if !ev.IsError {
				s.mu.Lock()
				newID := firstNonEmpty(ev.SessionID, pendingSession, s.sessionID)
				s.totalCost += ev.CostUSD
				s.mu.Unlock()
				s.setSessionID(newID)
				break
			}
			// Never end an errored turn silently: fall back to the subtype and stderr.
			res.ErrorText = firstNonEmpty(ev.Result, strings.Join(ev.Errors, "; "), stderr.lastLine(), ev.Subtype, "unknown error")
			if ev.NumTurns == 0 && sent.Len() == 0 && ev.Subtype == "error_during_execution" {
				res.failedToStart = true // Run retries or reports it
			} else if !strings.Contains(sent.String(), res.ErrorText) {
				prefix := ""
				if sent.Len() > 0 {
					prefix = "\n"
				}
				sink.Send(prefix + "[claude error] " + res.ErrorText)
			}
		}
	}

	waitErr := cmd.Wait()
	s.mu.Lock()
	s.turns++
	s.mu.Unlock()

	stallMu.Lock()
	wasStalled := stalled
	stallMu.Unlock()
	switch {
	case wasStalled:
		return res, fmt.Errorf("claude produced no response within %s. Usually its login has expired: "+
			"run `claude` in a terminal and use /login, or test with `claude -p hi --debug`", s.opts.FirstOutput)
	case ctx.Err() != nil:
		return res, ctx.Err()
	case waitErr != nil && !gotResult:
		return res, fmt.Errorf("claude exited: %v: %s", waitErr, stderr.lastLine())
	}
	return res, nil
}

// Text runs a one-shot, tool-less completion (used by the meta-agent for
// routing and synthesis) and returns Claude's final answer.
func Text(ctx context.Context, prompt, model string) (string, error) {
	args := []string{"-p", prompt, "--output-format", "json", "--tools", ""}
	if model != "" {
		args = append(args, "--model", model)
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, envOr("CLAUDE_BIN", "claude"), args...)
	cmd.Env = cleanEnv()
	setProcessGroup(cmd)
	cmd.Cancel = func() error { return killTree(cmd) }
	cmd.WaitDelay = 5 * time.Second
	out, err := cmd.Output()
	var r struct {
		IsError bool   `json:"is_error"`
		Result  string `json:"result"`
	}
	if jerr := json.Unmarshal(out, &r); jerr != nil {
		if err == nil {
			err = jerr
		}
		return "", fmt.Errorf("claude: %w", err)
	}
	if r.IsError {
		return "", errors.New(r.Result)
	}
	return r.Result, nil
}

// ---------------------------------------------------------------------------
// small helpers

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// summarize shortens long string inputs (e.g. file contents) for display.
func summarize(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if s, ok := v.(string); ok && len(s) > 200 { // v.(string) is a type assertion
			v = s[:200] + "…"
		}
		out[k] = v
	}
	return out
}

// tailBuffer keeps the last 4 KB written to it. Implementing Write makes it
// an io.Writer, so it can be assigned to cmd.Stderr.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > 4096 {
		t.buf = t.buf[len(t.buf)-4096:]
	}
	return len(p), nil
}

func (t *tailBuffer) lastLine() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	lines := strings.Split(strings.TrimSpace(string(t.buf)), "\n")
	return lines[len(lines)-1]
}

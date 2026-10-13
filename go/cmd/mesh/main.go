// Command mesh is the operator console for every agent on the fabric.
//
//	mesh ls                                 discover agents + liveness
//	mesh watch                              live heartbeats, joins and departures
//	mesh ask <target> <prompt...>           prompt one agent, or fan out if several match
//	     [-attach file] [-answer yes]       (-answer auto-replies to mid-stream queries)
//	mesh spawn <dir> [-name n] [-model m] [-lifetime 900] [-tools Read,Edit] [-mode acceptEdits]
//	mesh sessions                           list sessions on the cc-pool controller
//	mesh stop <session>
//	mesh chat                               REPL: "@target msg", "@a,@b msg", "@all msg", /ls, /use <target>
//
// <target> matches an agent's token, name or label prefix: echo, cc, cc/srini, shell,cc, all.
// Flags may appear before or after the positional arguments.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"agentmesh/internal/env"
	"agentmesh/mesh"
	"agentmesh/protocol"
)

// ---------------------------------------------------------------------------
// terminal colours (only when stdout is a terminal)

var tty = func() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}()

func color(code int) func(string) string {
	return func(s string) string { // a closure: it remembers `code`
		if !tty {
			return s
		}
		return fmt.Sprintf("\x1b[%dm%s\x1b[0m", code, s)
	}
}

var (
	dim, bold, red, green, yellow = color(2), color(1), color(31), color(32), color(33)
	palette                       = []int{36, 35, 34, 33, 32, 96, 95, 94}
)

// ---------------------------------------------------------------------------
// input: one goroutine reads stdin and hands lines over a channel, so the chat
// loop and query prompts share it without losing lines.

var lines = func() <-chan string {
	ch := make(chan string)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			ch <- sc.Text()
		}
		close(ch)
	}()
	return ch
}

var input <-chan string

func readLine(prompt string) (string, bool) {
	if input == nil {
		input = lines() // start the reader on first use
	}
	fmt.Print(prompt)
	line, ok := <-input // ok is false once the channel is closed (EOF)
	if ok && !tty {
		fmt.Println(line) // echo scripted input so transcripts read naturally
	}
	return line, ok
}

// ---------------------------------------------------------------------------

type app struct {
	mesh   *mesh.Mesh
	ctx    context.Context
	answer string // -answer: automatic reply to queries
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "help" || os.Args[1] == "-h" {
		usage()
		return
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, red("error: "+err.Error()))
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`mesh ls | watch | ask <target> <prompt> | spawn <dir> | sessions | stop <session> | chat
  ask flags:   -attach FILE   -answer TEXT
  spawn flags: -name N  -model M  -mode MODE  -tools A,B  -lifetime SECONDS`)
}

// parseAnywhere lets flags appear after positional arguments, which Go's
// flag package normally stops at. It returns the positional arguments.
func parseAnywhere(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos, args = append(pos, args[0]), args[1:]
	}
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	attach := fs.String("attach", "", "file to attach (ask)")
	answer := fs.String("answer", "", "auto-reply to mid-stream queries (ask)")
	name := fs.String("name", "", "session name (spawn)")
	model := fs.String("model", "", "model (spawn)")
	mode := fs.String("mode", "", "permission mode (spawn)")
	tools := fs.String("tools", "", "allowed tools, comma separated (spawn)")
	lifetime := fs.Int("lifetime", 0, "max lifetime in seconds (spawn)")
	pos, err := parseAnywhere(fs, args)
	if err != nil {
		return err
	}

	nc, err := env.Connect("mesh-cli")
	if err != nil {
		return err
	}
	defer nc.Drain()
	m := mesh.New(nc)
	if err := m.Watch(); err != nil { // before discovery (§8.5)
		return err
	}
	ctx, stop := env.SignalContext()
	defer stop()
	a := &app{mesh: m, ctx: ctx, answer: *answer}

	switch cmd {
	case "ls", "agents":
		return a.list()
	case "watch":
		return a.watch()
	case "ask":
		if len(pos) < 2 {
			return errors.New("usage: mesh ask <target> <prompt>")
		}
		return a.ask(pos[0], strings.Join(pos[1:], " "), *attach)
	case "spawn":
		if len(pos) < 1 {
			return errors.New("usage: mesh spawn <dir> [-name n] [-model m] [-lifetime s]")
		}
		return a.spawn(pos[0], *name, *model, *mode, *tools, *lifetime)
	case "sessions":
		return a.sessions()
	case "stop":
		if len(pos) < 1 {
			return errors.New("usage: mesh stop <session>")
		}
		return a.stop(pos[0])
	case "chat":
		return a.chat()
	}
	return fmt.Errorf("unknown command %q (try: mesh help)", cmd)
}

func (a *app) discover() ([]mesh.Agent, error) { return a.mesh.Discover(750 * time.Millisecond) }

func (a *app) list() error {
	all, err := a.discover()
	if err != nil {
		return err
	}
	if len(all) == 0 {
		fmt.Println(dim("no agents on the fabric"))
		return nil
	}
	unique := mesh.UniqueBySubject(all)
	count := map[string]int{}
	for _, ag := range all {
		count[ag.PromptSubject()]++
	}

	// Ask every agent for its status concurrently; each goroutine writes its own slot.
	statuses := make([]*protocol.Heartbeat, len(unique))
	done := make(chan struct{})
	for i, ag := range unique {
		go func() {
			if hb, err := a.mesh.Status(ag, 1500*time.Millisecond); err == nil {
				statuses[i] = &hb
			}
			done <- struct{}{}
		}()
	}
	for range unique {
		<-done
	}

	width := 0
	for _, ag := range unique {
		width = max(width, len(ag.Label()))
	}
	for i, ag := range unique {
		live, extra := red("○"), ""
		if statuses[i] != nil {
			live = green("●")
			if statuses[i].Busy > 0 {
				extra += yellow(fmt.Sprintf(" busy:%d", statuses[i].Busy))
			}
		}
		if n := count[ag.PromptSubject()]; n > 1 {
			extra += yellow(fmt.Sprintf(" ×%d", n))
		}
		var verbs []string
		for v := range ag.Endpoints {
			if v != "prompt" && v != "status" {
				verbs = append(verbs, v)
			}
		}
		desc := ag.Description
		if len(verbs) > 0 {
			desc += dim("  [+" + strings.Join(verbs, ",") + "]")
		}
		fmt.Printf("%s %-*s%s  %s\n", live, width, bold(ag.Label()), extra, desc)
	}
	fmt.Println(dim(fmt.Sprintf("\n%d agent(s), %d instance(s)", len(unique), len(all))))
	return nil
}

func (a *app) watch() error {
	// beat is what the heartbeat callback sends to the loop below.
	type beat struct {
		id, label string
		interval  time.Duration
	}
	type entry struct {
		label    string
		at       time.Time
		interval time.Duration
	}
	beats := make(chan beat, 64)
	seen := map[string]entry{} // only this goroutine touches it, so no mutex is needed

	fmt.Println(dim("watching agents.hb.*.*.* (Ctrl-C to exit)"))
	a.mesh.OnHeartbeat(func(hb protocol.Heartbeat, subject string) {
		p, _ := protocol.ParseSubject(subject)
		beats <- beat{hb.InstanceID, p.Agent + "/" + p.Owner + "/" + p.Name, time.Duration(hb.IntervalS * float64(time.Second))}
	})
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case b := <-beats:
			if _, ok := seen[b.id]; !ok {
				fmt.Printf("%s  %s  %s\n", green("+ join"), bold(b.label), dim(b.id))
			}
			seen[b.id] = entry{label: b.label, at: time.Now(), interval: b.interval}
		case <-tick.C:
			for id, e := range seen {
				if time.Since(e.at) > 3*e.interval {
					fmt.Printf("%s  %s  %s\n", red("- gone"), bold(e.label), dim(id))
					delete(seen, id) // deleting while ranging over a map is allowed in Go
				}
			}
		case <-a.ctx.Done():
			return nil
		}
	}
}

func (a *app) ask(target, prompt, attach string) error {
	all, err := a.discover()
	if err != nil {
		return err
	}
	agents := mesh.MatchTargets(all, target)
	if len(agents) == 0 {
		return fmt.Errorf("no agent matches %q (try: mesh ls)", target)
	}
	var opts mesh.PromptOptions
	if attach != "" {
		data, err := os.ReadFile(attach)
		if err != nil {
			return err
		}
		opts.Attachments = []protocol.Attachment{{Filename: filepath.Base(attach), Content: base64.StdEncoding.EncodeToString(data)}}
	}
	a.render(agents, prompt, opts)
	return nil
}

// render streams one or many agents to the terminal. One agent: raw stream.
// Several: line-prefixed and coloured per agent.
func (a *app) render(agents []mesh.Agent, prompt string, opts mesh.PromptOptions) {
	single := len(agents) == 1
	colorOf := map[string]func(string) string{}
	for i, ag := range agents {
		colorOf[ag.Label()] = color(palette[i%len(palette)])
	}
	bufs := map[string]*strings.Builder{}
	atLineStart := true
	tag := func(ag *mesh.Agent) string { return colorOf[ag.Label()]("[" + ag.Label() + "]") }

	out := func(ag *mesh.Agent, s string) {
		if single {
			fmt.Print(s)
			if s != "" {
				atLineStart = strings.HasSuffix(s, "\n")
			}
			return
		}
		b := bufs[ag.Label()]
		if b == nil {
			b = &strings.Builder{}
			bufs[ag.Label()] = b
		}
		b.WriteString(s)
		parts := strings.Split(b.String(), "\n")
		for _, l := range parts[:len(parts)-1] {
			fmt.Println(tag(ag), l)
		}
		b.Reset()
		b.WriteString(parts[len(parts)-1])
	}
	// Notes flush the agent's partial line first so order is preserved.
	note := func(ag *mesh.Agent, s string) {
		if single {
			if !atLineStart {
				fmt.Println()
			}
			fmt.Println(dim(s))
			atLineStart = true
			return
		}
		if b := bufs[ag.Label()]; b != nil && b.Len() > 0 {
			fmt.Println(tag(ag), b.String())
			b.Reset()
		}
		fmt.Println(tag(ag), dim(s))
	}

	if !single {
		labels := make([]string, len(agents))
		for i, ag := range agents {
			labels[i] = ag.Label()
		}
		fmt.Println(dim(fmt.Sprintf("fan-out to %d agents: %s", len(agents), strings.Join(labels, ", "))))
	}
	start := time.Now()
	ackAt := map[string]time.Duration{}
	if opts.Inactivity == 0 {
		opts.Inactivity = 2 * time.Minute
	}

	for ev := range a.mesh.Fanout(a.ctx, agents, prompt, opts) {
		ag := ev.From
		switch ev.Type {
		case mesh.EvAck:
			if _, ok := ackAt[ag.Label()]; !ok {
				ackAt[ag.Label()] = time.Since(start)
			}
		case mesh.EvText:
			out(ag, ev.Text)
		case mesh.EvQuery:
			q := ev.Query.Prompt
			if !single {
				q = "[" + ag.Label() + "] " + q
			}
			reply := a.answer
			if reply == "" {
				line, ok := readLine(yellow("? " + q + " "))
				reply = line
				if !ok {
					reply = "no"
				}
			}
			ev.Answer(reply)
		case mesh.EvError:
			note(ag, red(fmt.Sprintf("error %d: %s", ev.Code, ev.Message)))
		case mesh.EvTimeout:
			note(ag, red("no chunk within inactivity timeout; giving up"))
		case mesh.EvUnknown:
			note(ag, describeChunk(ev.Chunk))
		case mesh.EvDone:
			note(ag, fmt.Sprintf("done in %.1fs (ack %dms)", time.Since(start).Seconds(), ackAt[ag.Label()].Milliseconds()))
		}
	}
}

// describeChunk renders our non-standard chunk types (tool, usage, plan, status_note).
func describeChunk(c *protocol.Chunk) string {
	var d map[string]any
	_ = jsonUnmarshal(c.Data, &d)
	switch c.Type {
	case "tool":
		prefix := ""
		if ag, ok := d["agent"].(string); ok {
			prefix = "[" + ag + "] "
		}
		in := string(mustJSON(d["input"]))
		if len(in) > 120 {
			in = in[:120]
		}
		return fmt.Sprintf("⚙ %s%v %s", prefix, d["name"], in)
	case "usage":
		if _, ok := d["output_tokens"]; ok { // local models report tokens, not dollars
			return fmt.Sprintf("%v: %.0f tokens in, %.0f out, %.1f tok/s, remembers %.0f messages",
				d["model"], num(d["prompt_tokens"]), num(d["output_tokens"]), num(d["tokens_per_sec"]), num(d["remembered"]))
		}
		s := fmt.Sprintf("$%.4f this turn, $%.4f session", num(d["cost_usd"]), num(d["total_cost_usd"]))
		if resume, ok := d["resume"].(string); ok {
			s += "\ncontinue in Claude Code: " + resume
		}
		return s
	case "plan":
		var p struct {
			Targets []struct{ Agent string } `json:"targets"`
		}
		_ = jsonUnmarshal(c.Data, &p)
		var names []string
		for _, t := range p.Targets {
			names = append(names, t.Agent)
		}
		return "plan: " + strings.Join(names, ", ")
	case "status_note":
		return fmt.Sprint(d["message"])
	}
	return c.Type
}

func num(v any) float64 {
	f, _ := v.(float64) // JSON numbers decode to float64
	return f
}

func (a *app) controller() (mesh.Agent, error) {
	all, err := a.discover()
	if err != nil {
		return mesh.Agent{}, err
	}
	for _, ag := range all {
		if _, ok := ag.Endpoints["spawn"]; ok && ag.Metadata["role"] == "controller" {
			return ag, nil
		}
	}
	return mesh.Agent{}, errors.New("no controller on the fabric (start one: go run ./cmd/controller)")
}

func (a *app) spawn(dir, name, model, mode, tools string, lifetime int) error {
	ctl, err := a.controller()
	if err != nil {
		return err
	}
	abs, _ := filepath.Abs(dir)
	body := map[string]any{"cwd": abs}
	if name != "" {
		body["name"] = name
	}
	if model != "" {
		body["model"] = model
	}
	if mode != "" {
		body["permission_mode"] = mode
	}
	if tools != "" {
		body["tools"] = strings.Split(tools, ",")
	}
	if lifetime > 0 {
		body["max_lifetime_s"] = lifetime
	}
	var r struct {
		SessionID string `json:"session_id"`
		Subject   string `json:"subject"`
	}
	if err := a.mesh.Call(ctl, "spawn", body, &r, 15*time.Second); err != nil {
		return err
	}
	fmt.Println(green("spawned"), bold(r.SessionID), dim(r.Subject))
	return nil
}

func (a *app) sessions() error {
	ctl, err := a.controller()
	if err != nil {
		return err
	}
	var r struct {
		Sessions []struct {
			SessionID          string  `json:"session_id"`
			Cwd                string  `json:"cwd"`
			Busy               bool    `json:"busy"`
			Turns              int     `json:"turns"`
			TotalCostUSD       float64 `json:"total_cost_usd"`
			RemainingLifetimeS *int    `json:"remaining_lifetime_s"`
		} `json:"sessions"`
	}
	if err := a.mesh.Call(ctl, "list", struct{}{}, &r, 5*time.Second); err != nil {
		return err
	}
	if len(r.Sessions) == 0 {
		fmt.Println(dim("no sessions"))
	}
	for _, s := range r.Sessions {
		dot := green("●")
		if s.Busy {
			dot = yellow("●")
		}
		life := ""
		if s.RemainingLifetimeS != nil {
			life = dim(fmt.Sprintf(" expires in %ds", *s.RemainingLifetimeS))
		}
		fmt.Printf("%s %s  %s  %s%s\n", dot, bold(s.SessionID), s.Cwd, dim(fmt.Sprintf("turns %d, $%.4f", s.Turns, s.TotalCostUSD)), life)
	}
	return nil
}

func (a *app) stop(id string) error {
	ctl, err := a.controller()
	if err != nil {
		return err
	}
	if err := a.mesh.Call(ctl, "stop", map[string]string{"session_id": id}, nil, 5*time.Second); err != nil {
		return err
	}
	fmt.Println(red("stopped"), id)
	return nil
}

var mentionPrefix = regexp.MustCompile(`^((?:@[\w/*,-]+[\s,]*)+)(.*)$`)

func (a *app) chat() error {
	current := ""
	fmt.Println(dim(`mesh chat — "@target message" routes, "@all message" fans out, /ls lists, /use <target> sets default, /quit exits`))
	for {
		p := "mesh> "
		if current != "" {
			p = current + "> "
		}
		raw, ok := readLine(bold(p))
		if !ok {
			return nil // EOF
		}
		line := strings.TrimSpace(raw)
		switch {
		case line == "":
			continue
		case line == "/quit" || line == "/exit":
			return nil
		case line == "/ls":
			if err := a.list(); err != nil {
				fmt.Println(red(err.Error()))
			}
			continue
		case strings.HasPrefix(line, "/use "):
			current = strings.TrimSpace(line[5:])
			continue
		}

		target, prompt := current, line
		if m := mentionPrefix.FindStringSubmatch(line); m != nil {
			target, prompt = m[1], strings.TrimSpace(m[2])
		}
		if target == "" {
			fmt.Println(dim("no target: use @name or /use <target>"))
			continue
		}
		all, err := a.discover()
		if err != nil {
			return err
		}
		agents := mesh.MatchTargets(all, target)
		if len(agents) == 0 {
			fmt.Println(red("no agent matches " + target))
			continue
		}
		a.render(agents, prompt, mesh.PromptOptions{})
		fmt.Println()
	}
}

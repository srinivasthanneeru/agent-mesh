// Command meta is an agent that orchestrates the other agents on the fabric.
// Because it is itself a protocol agent, anything that can prompt an agent
// (the mesh console, `nats req`, another meta-agent) can use it.
//
// Routing, in order:
//
//	"@cc @shell do X"   fan out to the matching agents (see mesh.MatchTargets)
//	"@all do X"         every worker agent
//	"do X"              ask Claude which agents to use and what to tell each
//	                    (disable with -no-llm)
//
// Sub-agent questions (§7 queries) are relayed to whoever prompted the
// meta-agent, and the answer is passed back down. With -synthesize, Claude
// merges the answers into one.
//
//	go run ./cmd/meta [-name main] [-synthesize] [-no-llm] [-model haiku]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"agentmesh/agent"
	"agentmesh/claude"
	"agentmesh/internal/env"
	"agentmesh/mesh"
	"agentmesh/protocol"
)

type target struct {
	Agent  string `json:"agent"`
	Prompt string `json:"prompt"`
}

type plan struct {
	Targets []target `json:"targets"`
}

var mentionRE = regexp.MustCompile(`(^|\s)@([\w/*-]+)`)

func main() {
	name := flag.String("name", "main", "instance name")
	owner := flag.String("owner", env.Owner(), "owner")
	noLLM := flag.Bool("no-llm", false, "only route by @mention")
	synthesize := flag.Bool("synthesize", false, "merge answers with Claude")
	model := flag.String("model", "", "model for routing/synthesis")
	flag.Parse()

	nc, err := env.Connect("meta-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()
	m := mesh.New(nc)
	if err := m.Watch(); err != nil {
		log.Fatal(err)
	}

	o := &orchestrator{mesh: m, useLLM: !*noLLM, synthesize: *synthesize, model: *model}
	h, err := agent.New(agent.Options{
		Conn: nc, Agent: "meta", Owner: *owner, Name: *name,
		Description: "Routes prompts to other agents (@mention or LLM routing), relays their questions",
		Metadata:    map[string]string{"role": "orchestrator"},
		Heartbeat:   5 * time.Second,
		KeepAlive:   15 * time.Second,
		OnPrompt:    o.run,
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

type orchestrator struct {
	mesh       *mesh.Mesh
	useLLM     bool
	synthesize bool
	model      string
}

func describe(agents []mesh.Agent) string {
	var b strings.Builder
	for _, a := range agents {
		fmt.Fprintf(&b, "- %s: %s\n", a.Label(), a.Description)
	}
	return b.String()
}

func (o *orchestrator) plan(ctx context.Context, prompt string, workers []mesh.Agent) (plan, error) {
	var mentions []string
	for _, mm := range mentionRE.FindAllStringSubmatch(prompt, -1) {
		mentions = append(mentions, mm[2])
	}
	body := strings.TrimSpace(mentionRE.ReplaceAllString(prompt, " "))
	if len(mentions) > 0 {
		var p plan
		for _, a := range mesh.MatchTargets(workers, strings.Join(mentions, ",")) {
			p.Targets = append(p.Targets, target{Agent: a.Label(), Prompt: body})
		}
		return p, nil
	}
	if !o.useLLM {
		return plan{}, nil
	}

	answer, err := claude.Text(ctx, "You route a user's request to one or more agents. Available agents:\n"+
		describe(workers)+"\nUser request: "+prompt+"\n\n"+
		`Reply with ONLY JSON: {"targets":[{"agent":"<exact label>","prompt":"<instruction for that agent>"}]}.`+
		"\nPick the fewest agents that can do the job. Use an empty list if none fit.", o.model)
	if err != nil {
		return plan{}, err
	}
	start, end := strings.Index(answer, "{"), strings.LastIndex(answer, "}")
	if start < 0 || end < start {
		return plan{}, errors.New("router returned no JSON")
	}
	var p plan
	if err := json.Unmarshal([]byte(answer[start:end+1]), &p); err != nil {
		return plan{}, err
	}
	known := map[string]bool{}
	for _, a := range workers {
		known[a.Label()] = true
	}
	valid := p.Targets[:0] // filter in place, reusing the slice's memory
	for _, t := range p.Targets {
		if known[t.Agent] {
			valid = append(valid, t)
		}
	}
	p.Targets = valid
	return p, nil
}

func (o *orchestrator) run(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
	all, err := o.mesh.Discover(500 * time.Millisecond)
	if err != nil {
		return err
	}
	workers := mesh.MatchTargets(all, "all") // no controllers, no orchestrators (incl. ourselves)
	if len(workers) == 0 {
		res.Send("No worker agents are online.")
		return nil
	}
	p, err := o.plan(ctx, e.Prompt, workers)
	if err != nil {
		return protocol.Errorf(500, "meta-agent routing failed: %v", err)
	}
	if len(p.Targets) == 0 {
		res.Send("No agent selected. Mention one with @name. Online:\n" + describe(workers))
		return nil
	}
	res.Raw("plan", p)
	labels := make([]string, len(p.Targets))
	for i, t := range p.Targets {
		labels[i] = t.Agent
	}
	res.Send("Routing to " + strings.Join(labels, ", ") + "\n")

	byLabel := map[string]mesh.Agent{}
	for _, a := range workers {
		byLabel[a.Label()] = a
	}

	// One goroutine per target. Relayed questions go through askMu so only
	// one is outstanding with our caller at a time.
	var (
		wg      sync.WaitGroup
		askMu   sync.Mutex
		ansMu   sync.Mutex
		answers = map[string]string{}
	)
	for _, t := range p.Targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var line, full strings.Builder
			flush := func(final bool) { // line-buffered, so agents interleave by line
				s := line.String()
				parts := strings.Split(s, "\n")
				for _, l := range parts[:len(parts)-1] {
					res.Send("[" + t.Agent + "] " + l + "\n")
				}
				line.Reset()
				line.WriteString(parts[len(parts)-1])
				if final && line.Len() > 0 {
					res.Send("[" + t.Agent + "] " + line.String() + "\n")
				}
			}
			for ev := range o.mesh.Prompt(ctx, byLabel[t.Agent], t.Prompt, mesh.PromptOptions{Inactivity: 2 * time.Minute}) {
				switch ev.Type {
				case mesh.EvText:
					line.WriteString(ev.Text)
					full.WriteString(ev.Text)
					flush(false)
				case mesh.EvQuery:
					askMu.Lock()
					reply, ok := res.Ask(ctx, "["+t.Agent+"] "+ev.Query.Prompt, 2*time.Minute)
					askMu.Unlock()
					if !ok {
						reply = "no"
					}
					ev.Answer(reply)
				case mesh.EvUnknown:
					if ev.Chunk.Type == "tool" {
						var d map[string]any
						_ = json.Unmarshal(ev.Chunk.Data, &d)
						if d == nil { // writing to a nil map panics, so make one
							d = map[string]any{}
						}
						d["agent"] = t.Agent
						res.Raw("tool", d)
					}
				case mesh.EvError:
					res.Send(fmt.Sprintf("[%s] error %d: %s\n", t.Agent, ev.Code, ev.Message))
				case mesh.EvTimeout:
					res.Send("[" + t.Agent + "] timed out\n")
				}
			}
			flush(true)
			ansMu.Lock()
			answers[t.Agent] = full.String()
			ansMu.Unlock()
		}()
	}
	wg.Wait()

	if o.synthesize && len(answers) > 1 {
		var merged strings.Builder
		for k, v := range answers {
			fmt.Fprintf(&merged, "### %s\n%s\n\n", k, v)
		}
		out, err := claude.Text(ctx, "Request: "+e.Prompt+"\n\nAgent answers:\n"+merged.String()+"\nWrite one concise, combined answer.", o.model)
		if err != nil {
			return protocol.Errorf(500, "synthesis failed: %v", err)
		}
		res.Send("\n── synthesis ──\n" + out)
	}
	return nil
}

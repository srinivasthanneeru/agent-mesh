package claude

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Turn is one message in a Claude Code conversation, simplified for display.
type Turn struct {
	Role  string   `json:"role"` // "user" or "assistant"
	Text  string   `json:"text"`
	Tools []string `json:"tools,omitempty"` // tools Claude used in this turn, e.g. "Read"
}

// Transcript is a Claude Code session as stored on disk.
type Transcript struct {
	SessionID string `json:"session_id"`
	Dir       string `json:"dir"` // the folder it ran in; `claude --resume` must run there
	Turns     []Turn `json:"turns"`
}

// ErrNoTranscript means no session file with that id exists on this machine.
var ErrNoTranscript = errors.New("no Claude Code transcript found for that session id")

// ReadTranscript finds and reads a session saved by Claude Code. Claude Code
// keeps one JSON-lines file per session under ~/.claude/projects/<folder>/.
// The format is Claude Code's own and undocumented, so this only reads it,
// skips anything it doesn't recognise, and never writes.
func ReadTranscript(sessionID string) (Transcript, error) {
	t := Transcript{SessionID: sessionID}
	if sessionID == "" || strings.ContainsAny(sessionID, `/\`) {
		return t, ErrNoTranscript
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return t, err
	}
	matches, _ := filepath.Glob(filepath.Join(home, ".claude", "projects", "*", sessionID+".jsonl"))
	if len(matches) == 0 {
		return t, ErrNoTranscript
	}
	f, err := os.Open(matches[0])
	if err != nil {
		return t, err
	}
	defer f.Close()

	// The subset of each line we use. Content is either a string or a list
	// of blocks, so it stays raw until we know which.
	type line struct {
		Type        string `json:"type"`
		Cwd         string `json:"cwd"`
		IsMeta      bool   `json:"isMeta"`
		IsSidechain bool   `json:"isSidechain"`
		Message     struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	type block struct {
		Type string `json:"type"`
		Text string `json:"text"`
		Name string `json:"name"`
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var l line
		if json.Unmarshal(sc.Bytes(), &l) != nil {
			continue
		}
		if l.Cwd != "" && t.Dir == "" {
			t.Dir = l.Cwd
		}
		if (l.Type != "user" && l.Type != "assistant") || l.IsMeta || l.IsSidechain {
			continue
		}

		var turn Turn
		turn.Role = l.Type
		var s string
		var blocks []block
		switch {
		case json.Unmarshal(l.Message.Content, &s) == nil:
			turn.Text = s
		case json.Unmarshal(l.Message.Content, &blocks) == nil:
			var texts []string
			for _, b := range blocks {
				switch b.Type {
				case "text":
					texts = append(texts, b.Text)
				case "tool_use":
					turn.Tools = append(turn.Tools, b.Name)
				}
			}
			turn.Text = strings.Join(texts, "\n")
		}
		if turn.Text == "" && len(turn.Tools) == 0 {
			continue // e.g. a tool result or a thinking block: not part of the visible conversation
		}

		// Claude Code writes one line per content block, so consecutive
		// assistant lines belong to the same reply: merge them.
		if n := len(t.Turns); n > 0 && t.Turns[n-1].Role == turn.Role && turn.Role == "assistant" {
			last := &t.Turns[n-1]
			if turn.Text != "" {
				if last.Text != "" {
					last.Text += "\n"
				}
				last.Text += turn.Text
			}
			last.Tools = append(last.Tools, turn.Tools...)
			continue
		}
		t.Turns = append(t.Turns, turn)
	}
	return t, sc.Err()
}

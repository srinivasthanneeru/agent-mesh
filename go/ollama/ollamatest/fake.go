// Package ollamatest provides a fake Ollama server for tests and offline
// demos, in the spirit of the standard library's net/http/httptest.
//
// It speaks the same wire format as Ollama's /api/chat and /api/tags, and
// replies deterministically so tests can check memory: the answer says how
// many earlier user messages it was sent and echoes the newest one.
package ollamatest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// Server is a running fake. Close it when done.
type Server struct {
	*httptest.Server // embedding: Server gets URL, Close, ... for free

	Models     []string      // what /api/tags reports; requests for other models get 404
	TokenDelay time.Duration // pause between tokens, to make streaming visible

	mu       sync.Mutex
	requests [][]map[string]string // the messages of every /api/chat request
	body     []byte                // raw body of the latest request
}

func (s *Server) lastBody(*http.Request) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body
}

// New starts a fake with one installed model.
func New(models ...string) *Server {
	if len(models) == 0 {
		models = []string{"llama3.2:latest"}
	}
	s := &Server{Models: models}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/tags", s.tags) // method + path patterns: Go 1.22+
	mux.HandleFunc("POST /api/chat", s.chat)
	s.Server = httptest.NewServer(mux)
	return s
}

// Requests returns the messages sent in each chat request so far.
func (s *Server) Requests() [][]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]map[string]string(nil), s.requests...)
}

func (s *Server) tags(w http.ResponseWriter, _ *http.Request) {
	var models []map[string]string
	for _, m := range s.Models {
		models = append(models, map[string]string{"name": m})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"models": models})
}

func (s *Server) known(model string) bool {
	for _, m := range s.Models {
		if m == model || m == model+":latest" {
			return true
		}
	}
	return false
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Model    string              `json:"model"`
		Messages []map[string]string `json:"messages"`
	}
	raw, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.body = raw
	s.mu.Unlock()
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, req.Messages)
	s.mu.Unlock()

	if !s.known(req.Model) {
		w.WriteHeader(http.StatusNotFound)
		// Encode, don't Sprintf: json.Encoder escapes the quotes inside the message.
		_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("model %q not found, try pulling it first", req.Model)})
		return
	}

	last, earlier := "", 0
	for i, m := range req.Messages {
		if m["role"] == "user" {
			if i == len(req.Messages)-1 {
				last = m["content"]
			} else {
				earlier++
			}
		}
	}

	flusher, _ := w.(http.Flusher) // type assertion: can this writer flush?
	w.Header().Set("Content-Type", "application/x-ndjson")
	enc := json.NewEncoder(w) // Encode writes one JSON object plus "\n": exactly NDJSON

	switch last {
	case "STALL": // never answer; the client's watchdog must give up
		<-r.Context().Done()
		return
	case "FAIL": // an error in the middle of the stream
		_ = enc.Encode(map[string]any{"message": map[string]string{"role": "assistant", "content": "partial"}, "done": false})
		_ = enc.Encode(map[string]string{"error": "model ran out of memory"})
		return
	}

	var req2 struct {
		Think *bool `json:"think"` // a pointer tells "absent" apart from false
	}
	_ = json.Unmarshal(s.lastBody(r), &req2)
	if strings.Contains(req.Model, "qwen3") && (req2.Think == nil || *req2.Think) {
		for _, t := range []string{"Let", " me", " think", "."} {
			_ = enc.Encode(map[string]any{"message": map[string]string{"role": "assistant", "content": "", "thinking": t}, "done": false})
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	answer := fmt.Sprintf("I remember %d earlier messages. You said: %s", earlier, last)
	words := strings.SplitAfter(answer, " ")
	for _, word := range words {
		_ = enc.Encode(map[string]any{"message": map[string]string{"role": "assistant", "content": word}, "done": false})
		if flusher != nil {
			flusher.Flush() // push this token to the client now
		}
		time.Sleep(s.TokenDelay)
	}
	_ = enc.Encode(map[string]any{
		"done": true, "done_reason": "stop",
		"prompt_eval_count": 10 * len(req.Messages), "eval_count": len(words),
		"eval_duration": int64(len(words)) * int64(10*time.Millisecond), "total_duration": int64(50 * time.Millisecond),
	})
}

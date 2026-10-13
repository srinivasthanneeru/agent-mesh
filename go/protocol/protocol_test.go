package protocol

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestSubjectRoundTrip(t *testing.T) {
	s := Subject("prompt", "cc", "srini", "proj")
	if s != "agents.prompt.cc.srini.proj" {
		t.Fatalf("got %q", s)
	}
	p, ok := ParseSubject(s)
	if !ok || p != (SubjectParts{"prompt", "cc", "srini", "proj"}) { // structs compare with ==
		t.Fatalf("parse = %+v, %v", p, ok)
	}
	if _, ok := ParseSubject("agents.cc.srini.proj"); ok { // the v0.2 shape isn't ours
		t.Fatal("4-token subject should not parse")
	}
}

func TestDecodeEnvelope(t *testing.T) {
	att := func(name, content string) string {
		b, _ := json.Marshal(map[string]any{"prompt": "p", "attachments": []Attachment{{name, content}}})
		return string(b)
	}
	// A table-driven test: each row is one case; the loop runs them all as subtests.
	tests := []struct {
		name          string
		in            string
		attachmentsOK bool
		wantPrompt    string
		wantCode      int // 0 = no error expected
	}{
		{"plain text", "hello", false, "hello", 0},
		{"json after whitespace", "  \n{\"prompt\":\"hi\",\"trace\":\"x\"}", false, "hi", 0},
		{"empty", "", true, "", 400},
		{"whitespace only", "   ", true, "", 400},
		{"broken json", "{nope", true, "", 400},
		{"missing prompt", `{"x":1}`, true, "", 400},
		{"blank prompt", `{"prompt":"  "}`, true, "", 400},
		{"attachments refused", att("a.txt", "aGk="), false, "", 400},
		{"url-safe base64", att("a.txt", "a-b_"), true, "", 400},
		{"unpadded base64", att("a.txt", "aGk"), true, "", 400},
		{"path in filename", att("../a.txt", "aGk="), true, "", 400},
		{"good attachment", att("a.txt", "aGk="), true, "p", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := DecodeEnvelope([]byte(tc.in), tc.attachmentsOK)
			if tc.wantCode != 0 {
				var pe *Error
				if !errors.As(err, &pe) || pe.Code != tc.wantCode {
					t.Fatalf("want code %d, got %v", tc.wantCode, err)
				}
				return
			}
			if err != nil || env.Prompt != tc.wantPrompt {
				t.Fatalf("got %q, %v", env.Prompt, err)
			}
		})
	}
}

func TestUnknownFieldsSurvive(t *testing.T) {
	env, err := DecodeEnvelope([]byte(`{"prompt":"hi","trace":"abc"}`), false)
	if err != nil || string(env.Extra["trace"]) != `"abc"` {
		t.Fatalf("extra = %v, err = %v", env.Extra, err)
	}
}

func TestSizesAndTokens(t *testing.T) {
	for in, want := range map[string]int{"1MB": 1 << 20, "512KB": 512 << 10, "10B": 10, "2GB": 2 << 30} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v", in, got, err)
		}
	}
	if FormatSize(8<<20) != "8MB" {
		t.Errorf("FormatSize = %s", FormatSize(8<<20))
	}
	if got := SanitizeToken("My Project.v2!"); got != "my-project-v2" {
		t.Errorf("SanitizeToken = %q", got)
	}
}

package analyze

import (
	"encoding/json"
	"regexp"
	"testing"
)

func TestTopicForTool(t *testing.T) {
	tests := []struct {
		tool     string
		expected string
	}{
		{"pt-variable-advisor", "scout.pt-variable-advisor"},
		{"pt-duplicate-key-checker", "scout.pt-schema-checks"},
		{"pt-mysql-summary", "scout.pt-mysql-summary"},
		{"unknown", ""},
		{"", ""},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			got := TopicForTool(tc.tool)
			if got != tc.expected {
				t.Errorf("TopicForTool(%q) = %q, want %q", tc.tool, got, tc.expected)
			}
		})
	}
}

func TestNewUUID_Format(t *testing.T) {
	uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	for i := 0; i < 100; i++ {
		id := newUUID()
		if !uuidRegex.MatchString(id) {
			t.Fatalf("newUUID() = %q does not match UUID v4 format", id)
		}
	}
}

func TestNewUUID_Unique(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := newUUID()
		if seen[id] {
			t.Fatalf("newUUID() produced duplicate: %q", id)
		}
		seen[id] = true
	}
}

func TestMarshalPayload(t *testing.T) {
	input := map[string]any{"key": "value", "count": 42}
	got, err := marshalPayload(input)
	if err != nil {
		t.Fatalf("marshalPayload returned error: %v", err)
	}

	// Verify it's valid JSON.
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("marshalPayload output is not valid JSON: %v", err)
	}

	if parsed["key"] != "value" {
		t.Errorf("expected key='value', got %v", parsed["key"])
	}
}

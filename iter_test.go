package buildkitelogs

import (
	"strings"
	"testing"
)

func TestIterSeq2(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output\n" +
		"\x1b_bk;t=1745322209924\x07--- :package: Build job checkout directory"

	reader := strings.NewReader(testData)

	expectedContents := []string{
		"~~~ Running global environment hook",
		"$ /buildkite/agent/hooks/environment",
		"Some regular output",
		"--- :package: Build job checkout directory",
	}

	i := 0
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if i >= len(expectedContents) {
			t.Fatalf("More entries than expected")
		}

		if entry.Content != expectedContents[i] {
			t.Errorf("Expected content %q, got %q", expectedContents[i], entry.Content)
		}

		i++
	}

	if i != len(expectedContents) {
		t.Errorf("Expected %d entries, got %d", len(expectedContents), i)
	}
}

func TestIterSeq2EarlyExit(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output\n" +
		"\x1b_bk;t=1745322209924\x07--- :package: Build job checkout directory"

	reader := strings.NewReader(testData)

	count := 0
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		count++

		if entry == nil {
			t.Error("Entry should not be nil")
		}

		// Exit early after 2 entries
		if count >= 2 {
			break
		}
	}

	if count != 2 {
		t.Errorf("Expected to process exactly 2 entries, got %d", count)
	}
}

func TestIterSeq2GroupTracking(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07--- Build phase\n" +
		"\x1b_bk;t=1745322209924\x07Some build output"

	reader := strings.NewReader(testData)

	expectedGroups := []string{
		"~~~ Running global environment hook",
		"~~~ Running global environment hook",
		"--- Build phase",
		"--- Build phase",
	}

	i := 0
	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if i >= len(expectedGroups) {
			t.Fatalf("More entries than expected")
		}

		if entry.Group != expectedGroups[i] {
			t.Errorf("Entry %d: expected group %q, got %q", i+1, expectedGroups[i], entry.Group)
		}

		i++
	}
}

func TestIterSeq2WithFiltering(t *testing.T) {
	parser := NewParser()

	testData := "\x1b_bk;t=1745322209921\x07~~~ Running global environment hook\n" +
		"\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment\n" +
		"\x1b_bk;t=1745322209923\x07Some regular output\n" +
		"\x1b_bk;t=1745322209924\x07$ git clone repo\n" +
		"\x1b_bk;t=1745322209925\x07More output"

	reader := strings.NewReader(testData)

	// Filter for commands only
	commands := []string{}

	for entry, err := range parser.All(reader) {
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if entry.IsCommand() {
			commands = append(commands, entry.Content)
		}
	}

	expectedCommands := []string{
		"$ /buildkite/agent/hooks/environment",
		"$ git clone repo",
	}

	if len(commands) != len(expectedCommands) {
		t.Fatalf("Expected %d commands, got %d", len(expectedCommands), len(commands))
	}

	for i, cmd := range commands {
		if cmd != expectedCommands[i] {
			t.Errorf("Command %d: expected %q, got %q", i, expectedCommands[i], cmd)
		}
	}
}

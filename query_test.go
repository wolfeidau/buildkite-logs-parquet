package buildkitelogs

import (
	"os"
	"testing"
	"time"
)

func TestParquetReader(t *testing.T) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	t.Run("NewParquetReader", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		if reader == nil {
			t.Fatal("NewParquetReader returned nil")
		}
		if reader.filename != testFile {
			t.Errorf("Expected filename %s, got %s", testFile, reader.filename)
		}
	})

	t.Run("ReadEntries", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		entries, err := reader.ReadEntries()
		if err != nil {
			t.Fatalf("ReadEntries failed: %v", err)
		}

		if len(entries) == 0 {
			t.Fatal("No entries read from Parquet file")
		}

		// Verify structure of first entry
		entry := entries[0]
		if entry.Timestamp == 0 {
			t.Error("Expected non-zero timestamp")
		}
		if entry.Content == "" {
			t.Error("Expected non-empty content")
		}
	})

	t.Run("ListGroups", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		groups, err := reader.ListGroups()
		if err != nil {
			t.Fatalf("ListGroups failed: %v", err)
		}

		if len(groups) == 0 {
			t.Fatal("No groups found")
		}

		// Verify group structure
		group := groups[0]
		if group.Name == "" {
			t.Error("Expected non-empty group name")
		}
		if group.EntryCount == 0 {
			t.Error("Expected non-zero entry count")
		}
		if group.FirstSeen.IsZero() {
			t.Error("Expected non-zero FirstSeen time")
		}
		if group.LastSeen.IsZero() {
			t.Error("Expected non-zero LastSeen time")
		}
	})

	t.Run("FilterByGroup", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		entries, err := reader.FilterByGroup("environment")
		if err != nil {
			t.Fatalf("FilterByGroup failed: %v", err)
		}

		// Should find entries with "environment" in the group name
		if len(entries) == 0 {
			t.Error("Expected to find entries matching 'environment'")
		}

		// Verify all returned entries match the filter
		for _, entry := range entries {
			if entry.Group == "" {
				continue // Allow empty groups (they get treated as "<no group>")
			}
			if !containsIgnoreCase(entry.Group, "environment") {
				t.Errorf("Entry group '%s' does not contain 'environment'", entry.Group)
			}
		}
	})

	t.Run("Query_ListGroups", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		result, err := reader.Query("list-groups", "")
		if err != nil {
			t.Fatalf("Query list-groups failed: %v", err)
		}

		if len(result.Groups) == 0 {
			t.Fatal("No groups in query result")
		}
		if result.Stats.TotalEntries == 0 {
			t.Error("Expected non-zero total entries in stats")
		}
		if result.Stats.TotalGroups == 0 {
			t.Error("Expected non-zero total groups in stats")
		}
		if result.Stats.QueryTime == 0 {
			t.Error("Expected non-zero query time in stats")
		}
	})

	t.Run("Query_ByGroup", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		result, err := reader.Query("by-group", "environment")
		if err != nil {
			t.Fatalf("Query by-group failed: %v", err)
		}

		if len(result.Entries) == 0 {
			t.Error("Expected entries in by-group result")
		}
		if result.Stats.MatchedEntries == 0 {
			t.Error("Expected non-zero matched entries in stats")
		}
	})

	t.Run("Query_InvalidOperation", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		_, err := reader.Query("invalid-operation", "")
		if err == nil {
			t.Error("Expected error for invalid operation")
		}
	})

	t.Run("Query_ByGroupMissingPattern", func(t *testing.T) {
		reader := NewParquetReader(testFile)
		_, err := reader.Query("by-group", "")
		if err == nil {
			t.Error("Expected error for by-group without pattern")
		}
	})
}

func TestListGroups(t *testing.T) {
	// Create test data
	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC).UnixMilli()
	entries := []ParquetLogEntry{
		{
			Timestamp: baseTime,
			Content:   "~~~ Running tests",
			Group:     "~~~ Running tests",
			IsGroup:   true,
		},
		{
			Timestamp: baseTime + 100,
			Content:   "$ npm test",
			Group:     "~~~ Running tests",
			IsCommand: true,
		},
		{
			Timestamp: baseTime + 1000,
			Content:   "--- Build complete",
			Group:     "--- Build complete",
			IsGroup:   true,
		},
	}

	groups := ListGroups(entries)

	if len(groups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(groups))
	}

	// Check first group
	group1 := groups[0]
	if group1.Name != "~~~ Running tests" {
		t.Errorf("Expected group name '~~~ Running tests', got '%s'", group1.Name)
	}
	if group1.EntryCount != 2 {
		t.Errorf("Expected entry count 2, got %d", group1.EntryCount)
	}
	if group1.Commands != 1 {
		t.Errorf("Expected 1 command, got %d", group1.Commands)
	}
}

func TestFilterByGroup(t *testing.T) {
	entries := []ParquetLogEntry{
		{
			Content: "Environment setup",
			Group:   "~~~ Running global environment hook",
		},
		{
			Content: "Test command",
			Group:   "~~~ Running tests",
		},
		{
			Content: "Another environment task",
			Group:   "~~~ Pre-environment cleanup",
		},
	}

	filtered := FilterByGroup(entries, "environment")

	if len(filtered) != 2 {
		t.Errorf("Expected 2 filtered entries, got %d", len(filtered))
	}

	// Verify the correct entries were filtered
	expectedContents := []string{"Environment setup", "Another environment task"}
	for i, entry := range filtered {
		if entry.Content != expectedContents[i] {
			t.Errorf("Expected content '%s', got '%s'", expectedContents[i], entry.Content)
		}
	}
}

func TestReadParquetFile(t *testing.T) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	entries, err := ReadParquetFile(testFile)
	if err != nil {
		t.Fatalf("ReadParquetFile failed: %v", err)
	}

	if len(entries) == 0 {
		t.Fatal("No entries read from Parquet file")
	}

	// Verify first entry has expected structure
	entry := entries[0]
	if entry.Timestamp == 0 {
		t.Error("Expected non-zero timestamp")
	}
	if entry.Content == "" {
		t.Error("Expected non-empty content")
	}
}

func TestReadParquetFileNotFound(t *testing.T) {
	_, err := ReadParquetFile("nonexistent.parquet")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// Helper function for case-insensitive string contains check
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && 
		   len(substr) > 0 && 
		   (s == substr || 
		    (len(s) > len(substr) && 
		     (stringContains(toLowerCase(s), toLowerCase(substr)))))
}

func toLowerCase(s string) string {
	result := make([]rune, len([]rune(s)))
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			result[i] = r + 32
		} else {
			result[i] = r
		}
	}
	return string(result)
}

func stringContains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}
	
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
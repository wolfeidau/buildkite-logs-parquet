package buildkitelogs

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParquetExport(t *testing.T) {
	// Create test entries
	entries := []*LogEntry{
		{
			Timestamp: time.Unix(0, 1745322209921*int64(time.Millisecond)),
			Content:   "~~~ Running global environment hook",
			RawLine:   []byte("\\x1b_bk;t=1745322209921\\x07~~~ Running global environment hook"),
			Group:     "~~~ Running global environment hook",
		},
		{
			Timestamp: time.Unix(0, 1745322209922*int64(time.Millisecond)),
			Content:   "$ /buildkite/agent/hooks/environment", 
			RawLine:   []byte("\\x1b_bk;t=1745322209922\\x07$ /buildkite/agent/hooks/environment"),
			Group:     "~~~ Running global environment hook",
		},
	}

	// Export to Parquet
	filename := "test_output.parquet"
	err := ExportToParquet(entries, filename)
	if err != nil {
		t.Fatalf("ExportToParquet() error = %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("Parquet file was not created")
	}

	// Cleanup
	defer os.Remove(filename)

	// Check file is not empty
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}
	
	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}

func TestParquetIteratorExport(t *testing.T) {
	// Create test data
	testData := `\x1b_bk;t=1745322209921\x07~~~ Running global environment hook
\x1b_bk;t=1745322209922\x07$ /buildkite/agent/hooks/environment
\x1b_bk;t=1745322209923\x07Some regular output`

	parser := NewParser()
	reader := strings.NewReader(testData)
	iterator := parser.NewIterator(reader)

	// Export to Parquet using iterator
	filename := "test_iterator_output.parquet"
	err := ExportIteratorToParquet(iterator, filename)
	if err != nil {
		t.Fatalf("ExportIteratorToParquet() error = %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatalf("Parquet file was not created")
	}

	// Cleanup
	defer os.Remove(filename)

	// Check file is not empty
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}
	
	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}

func TestParquetWriter(t *testing.T) {
	// Create test file
	filename := "test_writer.parquet"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer os.Remove(filename)

	// Create writer
	writer := NewParquetWriter(file)
	defer writer.Close()

	// Create test entries
	entries := []*LogEntry{
		{
			Timestamp: time.Unix(0, 1745322209921*int64(time.Millisecond)),
			Content:   "test content",
			RawLine:   []byte("test raw line"),
			Group:     "test group",
		},
	}

	// Write batch
	err = writer.WriteBatch(entries)
	if err != nil {
		t.Fatalf("WriteBatch() error = %v", err)
	}

	// Close writer and file
	writer.Close()
	file.Close()

	// Check file was written
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("Failed to stat parquet file: %v", err)
	}
	
	if info.Size() == 0 {
		t.Error("Parquet file is empty")
	}
}
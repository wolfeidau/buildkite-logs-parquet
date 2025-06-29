package buildkitelogs

import (
	"fmt"
	"iter"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
)

// ParquetLogEntry represents a log entry in Parquet format
type ParquetLogEntry struct {
	Timestamp   int64  `parquet:"timestamp"`
	Content     string `parquet:"content"`
	Group       string `parquet:"group"`
	HasTime     bool   `parquet:"has_timestamp"`
	IsCommand   bool   `parquet:"is_command"`
	IsGroup     bool   `parquet:"is_group"`
	IsProgress  bool   `parquet:"is_progress"`
	RawLineSize int32  `parquet:"raw_line_size"`
}

// ExportToParquet exports log entries to a Parquet file
func ExportToParquet(entries []*LogEntry, filename string) error {
	// Convert to Parquet format
	parquetEntries := make([]ParquetLogEntry, len(entries))
	for i, entry := range entries {
		parquetEntries[i] = ParquetLogEntry{
			Timestamp:   entry.Timestamp.UnixMilli(),
			Content:     entry.Content,
			Group:       entry.Group,
			HasTime:     entry.HasTimestamp(),
			IsCommand:   entry.IsCommand(),
			IsGroup:     entry.IsGroup(),
			IsProgress:  entry.IsProgress(),
			RawLineSize: int32(len(entry.RawLine)),
		}
	}

	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write to Parquet
	writer := parquet.NewGenericWriter[ParquetLogEntry](file)
	defer writer.Close()

	_, err = writer.Write(parquetEntries)
	return err
}

// ExportIteratorToParquet exports log entries from an iterator to a Parquet file
func ExportIteratorToParquet(iterator *LogIterator, filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Parquet writer
	writer := parquet.NewGenericWriter[ParquetLogEntry](file)
	defer writer.Close()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]ParquetLogEntry, 0, batchSize)

	for iterator.Next() {
		entry := iterator.Entry()
		
		parquetEntry := ParquetLogEntry{
			Timestamp:   entry.Timestamp.UnixMilli(),
			Content:     entry.Content,
			Group:       entry.Group,
			HasTime:     entry.HasTimestamp(),
			IsCommand:   entry.IsCommand(),
			IsGroup:     entry.IsGroup(),
			IsProgress:  entry.IsProgress(),
			RawLineSize: int32(len(entry.RawLine)),
		}
		
		batch = append(batch, parquetEntry)
		
		// Write batch when it's full
		if len(batch) >= batchSize {
			_, err := writer.Write(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Write remaining entries
	if len(batch) > 0 {
		_, err := writer.Write(batch)
		if err != nil {
			return err
		}
	}

	return iterator.Err()
}

// ParseTimeFromMillis converts milliseconds since epoch to time.Time
func ParseTimeFromMillis(millis int64) time.Time {
	return time.Unix(0, millis*int64(time.Millisecond))
}

// ParquetWriter wraps a Parquet writer for streaming log entries
type ParquetWriter struct {
	writer *parquet.GenericWriter[ParquetLogEntry]
}

// NewParquetWriter creates a new Parquet writer for log entries
func NewParquetWriter(file *os.File) *ParquetWriter {
	writer := parquet.NewGenericWriter[ParquetLogEntry](file)
	return &ParquetWriter{
		writer: writer,
	}
}

// WriteBatch writes a batch of log entries to the Parquet file
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error {
	// Convert to Parquet format
	parquetEntries := make([]ParquetLogEntry, len(entries))
	for i, entry := range entries {
		parquetEntries[i] = ParquetLogEntry{
			Timestamp:   entry.Timestamp.UnixMilli(),
			Content:     entry.Content,
			Group:       entry.Group,
			HasTime:     entry.HasTimestamp(),
			IsCommand:   entry.IsCommand(),
			IsGroup:     entry.IsGroup(),
			IsProgress:  entry.IsProgress(),
			RawLineSize: int32(len(entry.RawLine)),
		}
	}

	_, err := pw.writer.Write(parquetEntries)
	return err
}

// Close closes the Parquet writer
func (pw *ParquetWriter) Close() error {
	return pw.writer.Close()
}

// ExportSeq2ToParquet exports log entries using Go 1.23+ iter.Seq2 for efficient iteration
func ExportSeq2ToParquet(seq iter.Seq2[*LogEntry, error], filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Parquet writer
	writer := parquet.NewGenericWriter[ParquetLogEntry](file)
	defer writer.Close()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]ParquetLogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}
		parquetEntry := ParquetLogEntry{
			Timestamp:   entry.Timestamp.UnixMilli(),
			Content:     entry.Content,
			Group:       entry.Group,
			HasTime:     entry.HasTimestamp(),
			IsCommand:   entry.IsCommand(),
			IsGroup:     entry.IsGroup(),
			IsProgress:  entry.IsProgress(),
			RawLineSize: int32(len(entry.RawLine)),
		}
		
		batch = append(batch, parquetEntry)
		
		// Write batch when it's full
		if len(batch) >= batchSize {
			_, err := writer.Write(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Write remaining entries
	if len(batch) > 0 {
		_, err := writer.Write(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

// ExportSeq2ToParquetWithFilter exports filtered log entries using iter.Seq2
func ExportSeq2ToParquetWithFilter(seq iter.Seq2[*LogEntry, error], filename string, filterFunc func(*LogEntry) bool) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Parquet writer
	writer := parquet.NewGenericWriter[ParquetLogEntry](file)
	defer writer.Close()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]ParquetLogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}
		
		// Apply filter if provided
		if filterFunc != nil && !filterFunc(entry) {
			continue
		}

		parquetEntry := ParquetLogEntry{
			Timestamp:   entry.Timestamp.UnixMilli(),
			Content:     entry.Content,
			Group:       entry.Group,
			HasTime:     entry.HasTimestamp(),
			IsCommand:   entry.IsCommand(),
			IsGroup:     entry.IsGroup(),
			IsProgress:  entry.IsProgress(),
			RawLineSize: int32(len(entry.RawLine)),
		}
		
		batch = append(batch, parquetEntry)
		
		// Write batch when it's full
		if len(batch) >= batchSize {
			_, err := writer.Write(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Write remaining entries
	if len(batch) > 0 {
		_, err := writer.Write(batch)
		if err != nil {
			return err
		}
	}

	return nil
}
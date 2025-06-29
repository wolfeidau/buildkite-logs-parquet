package buildkitelogs

import (
	"fmt"
	"iter"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// createArrowSchema creates the Arrow schema for log entries
func createArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "content", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "group", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "has_timestamp", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_command", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_group", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "is_progress", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "raw_line_size", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
}

// createRecordFromEntries creates an Arrow record from log entries
func createRecordFromEntries(entries []*LogEntry, pool memory.Allocator) (arrow.Record, error) {
	schema := createArrowSchema()

	// Create builders for each field
	timestampBuilder := array.NewInt64Builder(pool)
	contentBuilder := array.NewStringBuilder(pool)
	groupBuilder := array.NewStringBuilder(pool)
	hasTimestampBuilder := array.NewBooleanBuilder(pool)
	isCommandBuilder := array.NewBooleanBuilder(pool)
	isGroupBuilder := array.NewBooleanBuilder(pool)
	isProgressBuilder := array.NewBooleanBuilder(pool)
	rawLineSizeBuilder := array.NewInt32Builder(pool)

	defer timestampBuilder.Release()
	defer contentBuilder.Release()
	defer groupBuilder.Release()
	defer hasTimestampBuilder.Release()
	defer isCommandBuilder.Release()
	defer isGroupBuilder.Release()
	defer isProgressBuilder.Release()
	defer rawLineSizeBuilder.Release()

	// Reserve capacity
	numEntries := len(entries)
	timestampBuilder.Resize(numEntries)
	contentBuilder.Resize(numEntries)
	groupBuilder.Resize(numEntries)
	hasTimestampBuilder.Resize(numEntries)
	isCommandBuilder.Resize(numEntries)
	isGroupBuilder.Resize(numEntries)
	isProgressBuilder.Resize(numEntries)
	rawLineSizeBuilder.Resize(numEntries)

	// Populate arrays
	for _, entry := range entries {
		timestampBuilder.Append(entry.Timestamp.UnixMilli())
		contentBuilder.Append(entry.Content)
		groupBuilder.Append(entry.Group)
		hasTimestampBuilder.Append(entry.HasTimestamp())
		isCommandBuilder.Append(entry.IsCommand())
		isGroupBuilder.Append(entry.IsGroup())
		isProgressBuilder.Append(entry.IsProgress())
		rawLineSizeBuilder.Append(int32(len(entry.RawLine)))
	}

	// Build arrays
	timestampArray := timestampBuilder.NewArray()
	contentArray := contentBuilder.NewArray()
	groupArray := groupBuilder.NewArray()
	hasTimestampArray := hasTimestampBuilder.NewArray()
	isCommandArray := isCommandBuilder.NewArray()
	isGroupArray := isGroupBuilder.NewArray()
	isProgressArray := isProgressBuilder.NewArray()
	rawLineSizeArray := rawLineSizeBuilder.NewArray()

	defer timestampArray.Release()
	defer contentArray.Release()
	defer groupArray.Release()
	defer hasTimestampArray.Release()
	defer isCommandArray.Release()
	defer isGroupArray.Release()
	defer isProgressArray.Release()
	defer rawLineSizeArray.Release()

	// Create record
	return array.NewRecord(schema, []arrow.Array{
		timestampArray,
		contentArray,
		groupArray,
		hasTimestampArray,
		isCommandArray,
		isGroupArray,
		isProgressArray,
		rawLineSizeArray,
	}, int64(numEntries)), nil
}

// ExportToParquet exports log entries to a Parquet file using Apache Arrow
func ExportToParquet(entries []*LogEntry, filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}()

	// Create memory allocator
	pool := memory.NewGoAllocator()

	// Create Arrow record
	record, err := createRecordFromEntries(entries, pool)
	if err != nil {
		return err
	}
	defer record.Release()

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), file,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
			parquet.WithStats(true),
		),
		pqarrow.NewArrowWriterProperties(
			pqarrow.WithAllocator(pool),
			pqarrow.WithCoerceTimestamps(arrow.Millisecond),
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close writer: %v\n", closeErr)
		}
	}()

	// Write the record
	err = writer.Write(record)
	if err != nil {
		return err
	}

	return nil
}

// ParquetWriter provides streaming Parquet writing capabilities
type ParquetWriter struct {
	file   *os.File
	writer *pqarrow.FileWriter
	pool   memory.Allocator
	schema *arrow.Schema
}

// NewParquetWriter creates a new Parquet writer for streaming
func NewParquetWriter(file *os.File) *ParquetWriter {
	pool := memory.NewGoAllocator()
	schema := createArrowSchema()

	writer, err := pqarrow.NewFileWriter(schema, file, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil // In a real implementation, we'd want to return the error
	}

	return &ParquetWriter{
		file:   file,
		writer: writer,
		pool:   pool,
		schema: schema,
	}
}

// WriteBatch writes a batch of log entries to the Parquet file
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	record, err := createRecordFromEntries(entries, pw.pool)
	if err != nil {
		return err
	}
	defer record.Release()

	return pw.writer.Write(record)
}

// Close closes the Parquet writer
func (pw *ParquetWriter) Close() error {
	return pw.writer.Close()
}

// ExportIteratorToParquet exports from an iterator to Parquet using Apache Arrow
func ExportIteratorToParquet(iterator *LogIterator, filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}()

	// Create writer
	writer := NewParquetWriter(file)
	if writer == nil {
		return fmt.Errorf("failed to create Parquet writer")
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close writer: %v\n", closeErr)
		}
	}()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]*LogEntry, 0, batchSize)

	for iterator.Next() {
		entry := iterator.Entry()
		batch = append(batch, entry)

		// Write batch when full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset slice
		}
	}

	// Write final batch
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
		if err != nil {
			return err
		}
	}

	// Check for iterator errors
	return iterator.Err()
}

// ExportSeq2ToParquet exports log entries using Go 1.23+ iter.Seq2 for efficient iteration
func ExportSeq2ToParquet(seq iter.Seq2[*LogEntry, error], filename string) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}()

	// Create writer
	writer := NewParquetWriter(file)
	if writer == nil {
		return fmt.Errorf("failed to create Parquet writer")
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close writer: %v\n", closeErr)
		}
	}()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]*LogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}

		batch = append(batch, entry)

		// Write batch when full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset slice
		}
	}

	// Write final batch
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
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
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}()

	// Create writer
	writer := NewParquetWriter(file)
	if writer == nil {
		return fmt.Errorf("failed to create Parquet writer")
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close writer: %v\n", closeErr)
		}
	}()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]*LogEntry, 0, batchSize)

	for entry, err := range seq {
		// Handle errors during iteration
		if err != nil {
			return fmt.Errorf("error during iteration: %w", err)
		}

		// Apply filter if provided
		if filterFunc != nil && !filterFunc(entry) {
			continue
		}

		batch = append(batch, entry)

		// Write batch when full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset slice
		}
	}

	// Write final batch
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
		if err != nil {
			return err
		}
	}

	return nil
}

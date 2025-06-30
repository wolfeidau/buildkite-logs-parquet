package buildkitelogs

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetLogEntry represents a log entry read from a Parquet file
type ParquetLogEntry struct {
	Timestamp   int64  `json:"timestamp"`
	Content     string `json:"content"`
	Group       string `json:"group"`
	HasTime     bool   `json:"has_timestamp"`
	IsCommand   bool   `json:"is_command"`
	IsGroup     bool   `json:"is_group"`
	IsProgress  bool   `json:"is_progress"`
	RawLineSize int32  `json:"raw_line_size"`
}

// GroupInfo contains statistical information about a log group
type GroupInfo struct {
	Name       string    `json:"name"`
	EntryCount int       `json:"entry_count"`
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	Commands   int       `json:"commands"`
	Progress   int       `json:"progress"`
}

// QueryStats contains performance and result statistics for queries
type QueryStats struct {
	TotalEntries   int     `json:"total_entries"`
	MatchedEntries int     `json:"matched_entries"`
	TotalGroups    int     `json:"total_groups"`
	QueryTime      float64 `json:"query_time_ms"`
}

// QueryResult holds the results of a query operation
type QueryResult struct {
	Groups  []GroupInfo       `json:"groups,omitempty"`
	Entries []ParquetLogEntry `json:"entries,omitempty"`
	Stats   QueryStats        `json:"stats,omitempty"`
}

// ParquetReader provides functionality to read and query Parquet log files
type ParquetReader struct {
	filename string
}

// NewParquetReader creates a new ParquetReader for the specified file
func NewParquetReader(filename string) *ParquetReader {
	return &ParquetReader{
		filename: filename,
	}
}

// ReadEntries reads all log entries from the Parquet file
func (pr *ParquetReader) ReadEntries() ([]ParquetLogEntry, error) {
	return readParquetFile(pr.filename)
}

// ListGroups returns information about all groups in the Parquet file
func (pr *ParquetReader) ListGroups() ([]GroupInfo, error) {
	entries, err := pr.ReadEntries()
	if err != nil {
		return nil, err
	}
	return ListGroups(entries), nil
}

// FilterByGroup returns entries that belong to groups matching the specified name pattern
func (pr *ParquetReader) FilterByGroup(groupPattern string) ([]ParquetLogEntry, error) {
	entries, err := pr.ReadEntries()
	if err != nil {
		return nil, err
	}
	return FilterByGroup(entries, groupPattern), nil
}

// Query performs a complete query operation and returns formatted results
func (pr *ParquetReader) Query(operation, groupPattern string) (*QueryResult, error) {
	start := time.Now()

	entries, err := pr.ReadEntries()
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet file: %w", err)
	}

	result := &QueryResult{
		Stats: QueryStats{
			TotalEntries: len(entries),
		},
	}

	switch operation {
	case "list-groups":
		groups := ListGroups(entries)
		result.Groups = groups
		result.Stats.TotalGroups = len(groups)
		result.Stats.MatchedEntries = len(entries)

	case "by-group":
		if groupPattern == "" {
			return nil, fmt.Errorf("group pattern is required for by-group operation")
		}
		filtered := FilterByGroup(entries, groupPattern)
		result.Entries = filtered
		result.Stats.MatchedEntries = len(filtered)

	default:
		return nil, fmt.Errorf("unknown operation: %s", operation)
	}

	result.Stats.QueryTime = float64(time.Since(start).Nanoseconds()) / 1e6 // Convert to milliseconds

	return result, nil
}

// readParquetFile reads a Parquet file and returns the log entries
func readParquetFile(filename string) ([]ParquetLogEntry, error) {
	// Open the Parquet file
	osFile, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = osFile.Close() }()

	// Get file info for size (not currently used but may be needed for optimization)
	_, err = osFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// Create a memory pool
	pool := memory.NewGoAllocator()

	// Create a Parquet file reader using Arrow v18 API
	pf, err := file.NewParquetReader(osFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer func() { _ = pf.Close() }()

	// Create an Arrow file reader
	ctx := context.Background()
	arrowReader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read the entire table
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Convert table to entries
	entries, err := convertTableToEntries(table)
	if err != nil {
		return nil, fmt.Errorf("failed to convert table to entries: %w", err)
	}

	return entries, nil
}

// convertTableToEntries converts an Arrow table to ParquetLogEntry slice
func convertTableToEntries(table arrow.Table) ([]ParquetLogEntry, error) {
	schema := table.Schema()

	// Find column indices
	var timestampIdx, contentIdx, groupIdx, hasTimeIdx, isCmdIdx, isGroupIdx, isProgIdx, rawSizeIdx = -1, -1, -1, -1, -1, -1, -1, -1

	for i, field := range schema.Fields() {
		switch field.Name {
		case "timestamp":
			timestampIdx = i
		case "content":
			contentIdx = i
		case "group":
			groupIdx = i
		case "has_timestamp":
			hasTimeIdx = i
		case "is_command":
			isCmdIdx = i
		case "is_group":
			isGroupIdx = i
		case "is_progress":
			isProgIdx = i
		case "raw_line_size":
			rawSizeIdx = i
		}
	}

	if timestampIdx == -1 || contentIdx == -1 {
		return nil, fmt.Errorf("required columns 'timestamp' and 'content' not found")
	}

	var entries []ParquetLogEntry

	// Create a table reader to iterate through records
	tr := array.NewTableReader(table, 1000) // 1000 rows per batch
	defer tr.Release()

	for tr.Next() {
		record := tr.Record()
		batchEntries, err := convertRecordToEntries(record, timestampIdx, contentIdx, groupIdx, hasTimeIdx, isCmdIdx, isGroupIdx, isProgIdx, rawSizeIdx)
		if err != nil {
			return nil, err
		}
		entries = append(entries, batchEntries...)
	}

	if err := tr.Err(); err != nil {
		return nil, fmt.Errorf("error reading table: %w", err)
	}

	return entries, nil
}

// convertRecordToEntries converts an Arrow record to ParquetLogEntry slice
func convertRecordToEntries(record arrow.Record, timestampIdx, contentIdx, groupIdx, hasTimeIdx, isCmdIdx, isGroupIdx, isProgIdx, rawSizeIdx int) ([]ParquetLogEntry, error) {
	numRows := int(record.NumRows())
	entries := make([]ParquetLogEntry, numRows)

	// Get column arrays
	timestampCol := record.Column(timestampIdx)
	contentCol := record.Column(contentIdx)

	var groupCol, hasTimeCol, isCmdCol, isGroupCol, isProgCol, rawSizeCol arrow.Array
	if groupIdx >= 0 {
		groupCol = record.Column(groupIdx)
	}
	if hasTimeIdx >= 0 {
		hasTimeCol = record.Column(hasTimeIdx)
	}
	if isCmdIdx >= 0 {
		isCmdCol = record.Column(isCmdIdx)
	}
	if isGroupIdx >= 0 {
		isGroupCol = record.Column(isGroupIdx)
	}
	if isProgIdx >= 0 {
		isProgCol = record.Column(isProgIdx)
	}
	if rawSizeIdx >= 0 {
		rawSizeCol = record.Column(rawSizeIdx)
	}

	// Convert each row
	for i := 0; i < numRows; i++ {
		entry := ParquetLogEntry{}

		// Timestamp (required)
		if timestampCol.IsNull(i) {
			entry.Timestamp = 0
		} else {
			switch ts := timestampCol.(type) {
			case *array.Int64:
				entry.Timestamp = ts.Value(i)
			default:
				return nil, fmt.Errorf("unexpected timestamp column type: %T", timestampCol)
			}
		}

		// Content (required)
		if contentCol.IsNull(i) {
			entry.Content = ""
		} else {
			switch content := contentCol.(type) {
			case *array.String:
				entry.Content = content.Value(i)
			case *array.Binary:
				entry.Content = string(content.Value(i))
			default:
				return nil, fmt.Errorf("unexpected content column type: %T", contentCol)
			}
		}

		// Group (optional)
		if groupCol != nil && !groupCol.IsNull(i) {
			switch group := groupCol.(type) {
			case *array.String:
				entry.Group = group.Value(i)
			case *array.Binary:
				entry.Group = string(group.Value(i))
			}
		}

		// Boolean fields (optional)
		if hasTimeCol != nil && !hasTimeCol.IsNull(i) {
			if boolCol, ok := hasTimeCol.(*array.Boolean); ok {
				entry.HasTime = boolCol.Value(i)
			}
		}
		if isCmdCol != nil && !isCmdCol.IsNull(i) {
			if boolCol, ok := isCmdCol.(*array.Boolean); ok {
				entry.IsCommand = boolCol.Value(i)
			}
		}
		if isGroupCol != nil && !isGroupCol.IsNull(i) {
			if boolCol, ok := isGroupCol.(*array.Boolean); ok {
				entry.IsGroup = boolCol.Value(i)
			}
		}
		if isProgCol != nil && !isProgCol.IsNull(i) {
			if boolCol, ok := isProgCol.(*array.Boolean); ok {
				entry.IsProgress = boolCol.Value(i)
			}
		}

		// Raw size (optional)
		if rawSizeCol != nil && !rawSizeCol.IsNull(i) {
			switch size := rawSizeCol.(type) {
			case *array.Int32:
				entry.RawLineSize = size.Value(i)
			}
		}

		entries[i] = entry
	}

	return entries, nil
}

// ListGroups returns information about all groups in the provided entries
func ListGroups(entries []ParquetLogEntry) []GroupInfo {
	groupMap := make(map[string]*GroupInfo)

	for _, entry := range entries {
		groupName := entry.Group
		if groupName == "" {
			groupName = "<no group>"
		}

		info, exists := groupMap[groupName]
		if !exists {
			info = &GroupInfo{
				Name:      groupName,
				FirstSeen: time.Unix(0, entry.Timestamp*int64(time.Millisecond)),
				LastSeen:  time.Unix(0, entry.Timestamp*int64(time.Millisecond)),
			}
			groupMap[groupName] = info
		}

		info.EntryCount++

		entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
		if entryTime.Before(info.FirstSeen) {
			info.FirstSeen = entryTime
		}
		if entryTime.After(info.LastSeen) {
			info.LastSeen = entryTime
		}

		if entry.IsCommand {
			info.Commands++
		}
		if entry.IsProgress {
			info.Progress++
		}
	}

	// Convert to slice and sort by first seen time
	groups := make([]GroupInfo, 0, len(groupMap))
	for _, info := range groupMap {
		groups = append(groups, *info)
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].FirstSeen.Before(groups[j].FirstSeen)
	})

	return groups
}

// FilterByGroup returns entries that belong to groups matching the specified pattern
func FilterByGroup(entries []ParquetLogEntry, groupPattern string) []ParquetLogEntry {
	var filtered []ParquetLogEntry

	for _, entry := range entries {
		entryGroup := entry.Group
		if entryGroup == "" {
			entryGroup = "<no group>"
		}

		if strings.Contains(strings.ToLower(entryGroup), strings.ToLower(groupPattern)) {
			filtered = append(filtered, entry)
		}
	}

	return filtered
}

// ReadParquetFile is a convenience function to read entries from a Parquet file
func ReadParquetFile(filename string) ([]ParquetLogEntry, error) {
	return readParquetFile(filename)
}

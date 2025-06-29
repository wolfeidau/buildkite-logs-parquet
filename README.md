# Buildkite Logs Parser

A Go library and CLI tool for parsing Buildkite log files that contain OSC (Operating System Command) sequences with timestamps.

## Overview

Buildkite logs use a special format where each log entry is prefixed with an OSC sequence containing a timestamp:
```
\x1b_bk;t=1745322209921\x07~~~ Running global environment hook
```

This parser extracts the timestamps and content, providing both a Go library API and a command-line tool for processing these logs.

## Features

- **OSC Sequence Parsing**: Correctly handles Buildkite's `\x1b_bk;t=timestamp\x07content` format
- **Timestamp Extraction**: Converts millisecond timestamps to Go `time.Time` objects
- **ANSI Code Handling**: Optional stripping of ANSI escape sequences for clean text output
- **Content Classification**: Automatically identifies different types of log entries:
  - Commands (lines starting with `$`)
  - Section headers (lines starting with `~~~`, `---`, or `+++`)
  - Progress updates (git operation progress)
- **Multiple Data Sources**: Local files and Buildkite API integration
- **Buildkite API**: Fetch logs directly from Buildkite jobs via REST API
- **Multiple Output Formats**: Text, JSON, and Parquet export
- **Filtering**: Filter logs by entry type (command, group, progress)
- **Stream Processing**: Parse from any `io.Reader`
- **Group Tracking**: Automatically associate entries with build groups/sections
- **Parquet Export**: Efficient columnar storage for analytics and data processing
- **Parquet Query**: Fast querying of exported Parquet files with Apache Arrow Go v18

## Library Usage

### Basic Usage

```go
package main

import (
    "fmt"
    "os"
    
    buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
    parser := buildkitelogs.NewParser()
    
    // Parse a single line
    entry, err := parser.ParseLine("\x1b_bk;t=1745322209921\x07~~~ Running tests")
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Timestamp: %s\n", entry.Timestamp)
    fmt.Printf("Content: %s\n", entry.Content)
    fmt.Printf("Is Section: %t\n", entry.IsSection())
}
```

### Memory-Efficient Processing (Recommended)

For large log files, use the iterator interface to keep memory usage low:

#### Traditional Iterator (Go 1.22 and earlier)

```go
package main

import (
    "fmt"
    "os"
    
    buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
    parser := buildkitelogs.NewParser()
    
    file, err := os.Open("buildkite.log")
    if err != nil {
        panic(err)
    }
    defer file.Close()
    
    // Create iterator for memory-efficient processing
    iterator := parser.NewIterator(file)
    
    for iterator.Next() {
        entry := iterator.Entry()
        
        // Filter only commands
        if entry.IsCommand() {
            fmt.Printf("[%s] %s\n", entry.Timestamp, entry.CleanContent())
        }
    }
    
    // Check for errors
    if err := iterator.Err(); err != nil {
        panic(err)
    }
}
```

#### Modern Seq2 Iterator (Go 1.23+)

```go
package main

import (
    "fmt"
    "os"
    
    buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
    parser := buildkitelogs.NewParser()
    
    file, err := os.Open("buildkite.log")
    if err != nil {
        panic(err)
    }
    defer file.Close()
    
    // Use the modern iter.Seq2 pattern with proper error handling
    lineNum := 0
    for entry, err := range parser.All(file) {
        if err != nil {
            panic(err)
        }
        
        lineNum++
        
        // Process entries
        if entry.IsCommand() {
            fmt.Printf("Line %d: [%s] %s\n", lineNum, entry.Timestamp, entry.CleanContent())
        }
        
        // Early exit example - stop after 100 lines
        if lineNum >= 100 {
            break
        }
    }
}
```

### Querying Parquet Files

The library provides fast query capabilities for Parquet files using Apache Arrow Go v18:

```go
package main

import (
    "fmt"
    "log"
    
    buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
    // Create a Parquet reader
    reader := buildkitelogs.NewParquetReader("buildkite_logs.parquet")
    
    // List all groups with statistics
    groups, err := reader.ListGroups()
    if err != nil {
        log.Fatal(err)
    }
    
    for _, group := range groups {
        fmt.Printf("Group: %s (%d entries, %d commands)\n", 
            group.Name, group.EntryCount, group.Commands)
    }
    
    // Filter entries by group pattern
    entries, err := reader.FilterByGroup("environment")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Found %d entries containing 'environment'\n", len(entries))
    
    // Complete query with timing statistics
    result, err := reader.Query("list-groups", "")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Query completed in %.2f ms\n", result.Stats.QueryTime)
}
```

#### Query Operations

**List Groups**: Get statistical information about all log groups
```go
groups, err := reader.ListGroups()
// Returns []GroupInfo with Name, EntryCount, Commands, Progress, FirstSeen, LastSeen
```

**Filter by Group**: Find entries matching a group pattern
```go
entries, err := reader.FilterByGroup("test")  // Case-insensitive partial matching
// Returns []ParquetLogEntry for matching groups
```

**Complete Query**: Full query with statistics
```go
result, err := reader.Query("list-groups", "")           // List all groups
result, err := reader.Query("by-group", "environment")   // Filter by pattern
// Returns *QueryResult with Groups/Entries and Stats (timing, counts)
```

#### Direct File Reading

```go
// Read all entries from a Parquet file
entries, err := buildkitelogs.ReadParquetFile("logs.parquet")
if err != nil {
    log.Fatal(err)
}

// Use standalone functions for processing
groups := buildkitelogs.ListGroups(entries)
filtered := buildkitelogs.FilterByGroup(entries, "test")
```

### Legacy API (Loads All Entries)

⚠️ **Note**: This method loads all entries into memory and is not recommended for large files.

```go
// Parse from file (legacy - loads all entries into memory)
file, _ := os.Open("buildkite.log")
defer file.Close()

entries, err := parser.ParseReader(file)
if err != nil {
    panic(err)
}

for _, entry := range entries {
    if entry.HasTimestamp() {
        fmt.Printf("[%s] %s\n", entry.Timestamp, entry.CleanContent())
    }
}
```

## CLI Usage

### Installation

**Using Make (recommended):**
```bash
# Build with tests and linting
make all

# Quick development build
make dev

# Build with specific version
make build VERSION=v1.2.3

# Other useful targets
make clean test lint help
```

**Manual build:**
```bash
go build -o build/bklog ./cmd/bklog
```

**Build with version information:**
```bash
go build -ldflags "-X main.version=v1.2.3" -o build/bklog ./cmd/bklog
```

**Check version:**
```bash
./build/bklog version
# or
./build/bklog -v
# or  
./build/bklog --version
```

### Examples

#### Local File Processing

**Parse a log file with timestamps:**
```bash
./build/bklog parse -file buildkite.log -strip-ansi
```

**Output only commands:**
```bash
./build/bklog parse -file buildkite.log -filter command -strip-ansi
```

**Output only group headers:**
```bash
./build/bklog parse -file buildkite.log -filter group -strip-ansi
```

**JSON output:**
```bash
./build/bklog parse -file buildkite.log -json -strip-ansi
```

#### Buildkite API Integration

**Fetch logs directly from Buildkite API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -strip-ansi
```

**Export API logs to Parquet:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -parquet logs.parquet -summary
```

**Filter and export only commands from API:**
```bash
export BUILDKITE_API_TOKEN="bkua_your_token_here"
./build/bklog parse -org myorg -pipeline mypipeline -build 123 -job abc-def-456 -filter command -json
```

**Show processing statistics:**
```bash
./build/bklog parse -file buildkite.log -summary -strip-ansi
```
Output:
```
--- Processing Summary ---
Bytes processed: 24.4 KB
Total entries: 212
Entries with timestamps: 212
Commands: 15
Sections: 13
Progress updates: 5
Regular output: 179
```

**Show group/section information:**
```bash
./build/bklog -file buildkite.log -groups -strip-ansi | head -5
```
Output:
```
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] ~~~ Running global environment hook
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] $ /buildkite/agent/hooks/environment
[2025-04-22 21:43:29.948] [~~~ Running global pre-checkout hook] ~~~ Running global pre-checkout hook
[2025-04-22 21:43:29.949] [~~~ Running global pre-checkout hook] $ /buildkite/agent/hooks/pre-checkout
[2025-04-22 21:43:29.975] [~~~ Preparing working directory] ~~~ Preparing working directory
```

**Export to Parquet format:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet -summary
```
Output:
```
--- Processing Summary ---
Bytes processed: 24.4 KB
Total entries: 212
Entries with timestamps: 212
Commands: 15
Sections: 13
Progress updates: 4
Regular output: 180
Exported 212 entries to output.parquet
```

**Export filtered data to Parquet:**
```bash
./build/bklog -file buildkite.log -parquet commands.parquet -filter command -summary
```
This exports only command entries to a smaller Parquet file for analysis.

### Querying Parquet Files

The CLI provides fast query operations on previously exported Parquet files:

**List all groups with statistics:**
```bash
./build/bklog query -file output.parquet -op list-groups
```
Output:
```
Groups found: 5

GROUP NAME                                ENTRIES COMMANDS PROGRESS          FIRST SEEN           LAST SEEN
------------------------------------------------------------------------------------------------------------------------
~~~ Running global environment hook             2        1        0 2025-04-22 21:43:29 2025-04-22 21:43:29
~~~ Running global pre-checkout hook            2        1        0 2025-04-22 21:43:29 2025-04-22 21:43:29
--- :package: Build job checkout dire...        2        1        0 2025-04-22 21:43:30 2025-04-22 21:43:30

--- Query Statistics ---
Total entries: 10
Matched entries: 10
Total groups: 5
Query time: 2.36 ms
```

**Filter entries by group pattern:**
```bash
./build/bklog query -file output.parquet -op by-group -group "environment"
```
Output:
```
Entries in group matching 'environment': 2

[2025-04-22 21:43:29.921] [GRP] ~~~ Running global environment hook
[2025-04-22 21:43:29.922] [CMD] $ /buildkite/agent/hooks/environment

--- Query Statistics ---
Total entries: 10
Matched entries: 2
Query time: 0.36 ms
```

**JSON output for programmatic use:**
```bash
./build/bklog query -file output.parquet -op list-groups -format json
```

**Query without statistics:**
```bash
./build/bklog query -file output.parquet -op list-groups -stats=false
```

### CLI Options

#### Parse Command
```bash
./build/bklog parse [options]
```

- `-file <path>`: Path to Buildkite log file (required)
- `-json`: Output as JSON instead of text
- `-strip-ansi`: Remove ANSI escape sequences from output
- `-filter <type>`: Filter entries by type (`command`, `group`, `progress`)
- `-summary`: Show processing summary at the end
- `-groups`: Show group/section information for each entry
- `-parquet <path>`: Export to Parquet file (e.g., output.parquet)
- `-use-seq2`: Use Go 1.23+ iter.Seq2 for iteration (experimental)

#### Query Command
```bash
./build/bklog query [options]
```

- `-file <path>`: Path to Parquet log file (required)
- `-op <operation>`: Query operation (`list-groups`, `by-group`)
- `-group <pattern>`: Group name pattern to filter by (for `by-group` operation)
- `-format <format>`: Output format (`text`, `json`)
- `-stats`: Show query statistics (default: true)

## Log Entry Types

The parser can classify log entries into different types:

### Commands
Lines that represent shell commands being executed:
```
[2025-04-22 21:43:29.975] $ git clone -v -- https://github.com/buildkite/bash-example.git .
```

### Groups
Headers that mark different phases of the build (collapsible in Buildkite UI):
```
[2025-04-22 21:43:29.921] ~~~ Running global environment hook
[2025-04-22 21:43:30.694] --- :package: Build job checkout directory
[2025-04-22 21:43:30.699] +++ :hammer: Example tests
```

### Progress Updates
Progress indicators from git operations, identified by `[K` (erase-in-line) sequences:
```
[2025-04-22 21:43:30.213] remote: Counting objects:  50% (27/54)[K...
[2025-04-22 21:43:30.213] remote: Compressing objects: 100% (17/17), done.[K...
```

Progress lines contain the `[K` ANSI escape sequence, which indicates they were meant to overwrite each other in a terminal. The parser conservatively requires both the `[K` sequence and progress-related content (objects, deltas, or percentages) to avoid false positives.

### Groups/Sections

The parser automatically tracks which section or group each log entry belongs to:

```
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] ~~~ Running global environment hook
[2025-04-22 21:43:29.921] [~~~ Running global environment hook] $ /buildkite/agent/hooks/environment
[2025-04-22 21:43:29.948] [~~~ Running global pre-checkout hook] ~~~ Running global pre-checkout hook
```

Each entry is automatically associated with the most recent group header (`~~~`, `---`, or `+++`). This allows you to:
- **Group related log entries** by build phase
- **Filter logs by group** for focused analysis  
- **Understand build structure** and timing relationships
- **Export structured data** with group context preserved

## Parquet Export

The parser can export log entries to [Apache Parquet](https://parquet.apache.org/) format using the official [Apache Arrow Go](https://github.com/apache/arrow/tree/main/go) implementation for efficient storage and analysis:

### Benefits of Parquet Format

- **Columnar storage**: Efficient compression and query performance
- **Schema preservation**: Maintains data types and structure
- **Analytics ready**: Compatible with Pandas, Apache Spark, DuckDB, and other data tools
- **Compact size**: Typically 70-90% smaller than JSON for log data
- **Fast queries**: Optimized for analytical workloads and filtering

### Parquet Schema

The exported Parquet files contain the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | int64 | Unix timestamp in milliseconds |
| `content` | string | Log content after OSC sequence |
| `group` | string | Current build group/section |
| `has_timestamp` | bool | Whether entry has a valid timestamp |
| `is_command` | bool | Whether entry is a shell command |
| `is_group` | bool | Whether entry is a group header |
| `is_progress` | bool | Whether entry is a progress update |
| `raw_line_size` | int32 | Size of original raw line in bytes |

### Usage Examples

**Basic export:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet
```

**Export with filtering:**
```bash
./build/bklog -file buildkite.log -parquet commands.parquet -filter command
```

**Export using Go 1.23+ iter.Seq2:**
```bash
./build/bklog -file buildkite.log -parquet output.parquet -use-seq2 -summary
```
This uses the modern `iter.Seq2[int, *LogEntry]` iterator pattern introduced in Go 1.23.

**Analyze with Python/Pandas:**
```python
import pandas as pd

# Load Parquet file
df = pd.read_parquet('output.parquet')

# Filter commands by group
commands = df[(df['is_command'] == True) & (df['group'].str.contains('tests'))]

# Analyze build timing
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df.groupby('group')['datetime'].agg(['min', 'max', 'count']))
```

## API Reference

### Types

```go
type LogEntry struct {
    Timestamp time.Time  // Parsed timestamp (zero if no timestamp)
    Content   string     // Log content after OSC sequence
    RawLine   []byte     // Original raw log line as bytes
    Group     string     // Current section/group this entry belongs to
}

type Parser struct {
    // Internal regex patterns
}

type LogIterator struct {
    // Internal state for iteration
}
```

### Methods

#### Parser Methods
```go
// Create a new parser
func NewParser() *Parser

// Parse a single log line
func (p *Parser) ParseLine(line string) (*LogEntry, error)

// Create memory-efficient iterator (recommended)
func (p *Parser) NewIterator(reader io.Reader) *LogIterator

// Create Go 1.23+ iter.Seq2 iterator with proper error handling (modern approach)
func (p *Parser) All(reader io.Reader) iter.Seq2[*LogEntry, error]

// Parse multiple lines from a reader (legacy - loads all into memory)
func (p *Parser) ParseReader(reader io.Reader) ([]*LogEntry, error)

// Strip ANSI escape sequences
func (p *Parser) StripANSI(content string) string
```

#### Iterator Methods
```go
// Advance to next entry (returns false when done or on error)
func (iter *LogIterator) Next() bool

// Get current entry (only valid after successful Next())
func (iter *LogIterator) Entry() *LogEntry

// Get any error that occurred during iteration
func (iter *LogIterator) Err() error
```

#### LogEntry Methods
```go
func (entry *LogEntry) HasTimestamp() bool
func (entry *LogEntry) CleanContent() string  // Content with ANSI stripped
func (entry *LogEntry) IsCommand() bool
func (entry *LogEntry) IsGroup() bool         // Check if entry is a group header (~~~, ---, +++)
func (entry *LogEntry) IsSection() bool       // Deprecated: use IsGroup() instead  
func (entry *LogEntry) IsProgress() bool
```

#### Parquet Export Functions
```go
// Export log entries to Parquet file
func ExportToParquet(entries []*LogEntry, filename string) error

// Export from iterator to Parquet file (memory-efficient)
func ExportIteratorToParquet(iterator *LogIterator, filename string) error

// Export using Go 1.23+ iter.Seq2 (modern approach)
func ExportSeq2ToParquet(seq iter.Seq2[*LogEntry, error], filename string) error

// Export using iter.Seq2 with filtering
func ExportSeq2ToParquetWithFilter(seq iter.Seq2[*LogEntry, error], filename string, filterFunc func(*LogEntry) bool) error

// Create a new Parquet writer for streaming
func NewParquetWriter(file *os.File) *ParquetWriter

// Write a batch of entries to Parquet
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error

// Close the Parquet writer
func (pw *ParquetWriter) Close() error
```

#### Parquet Query Functions
```go
// Create a new Parquet reader
func NewParquetReader(filename string) *ParquetReader

// Read all entries from a Parquet file
func ReadParquetFile(filename string) ([]ParquetLogEntry, error)

// Process entries into group information  
func ListGroups(entries []ParquetLogEntry) []GroupInfo

// Filter entries by group pattern (case-insensitive)
func FilterByGroup(entries []ParquetLogEntry, groupPattern string) []ParquetLogEntry
```

#### ParquetReader Methods
```go
// Read all log entries from the Parquet file
func (pr *ParquetReader) ReadEntries() ([]ParquetLogEntry, error)

// List all groups with statistics
func (pr *ParquetReader) ListGroups() ([]GroupInfo, error)

// Filter entries by group pattern
func (pr *ParquetReader) FilterByGroup(groupPattern string) ([]ParquetLogEntry, error)

// Execute a complete query with timing stats
func (pr *ParquetReader) Query(operation, groupPattern string) (*QueryResult, error)
```

#### Query Result Types
```go
type ParquetLogEntry struct {
    Timestamp   int64  `json:"timestamp"`      // Unix timestamp in milliseconds
    Content     string `json:"content"`        // Log content
    Group       string `json:"group"`          // Associated group/section
    HasTime     bool   `json:"has_timestamp"`  // Whether entry has timestamp
    IsCommand   bool   `json:"is_command"`     // Whether entry is a command
    IsGroup     bool   `json:"is_group"`       // Whether entry is a group header
    IsProgress  bool   `json:"is_progress"`    // Whether entry is progress update
    RawLineSize int32  `json:"raw_line_size"`  // Original line size in bytes
}

type GroupInfo struct {
    Name       string    `json:"name"`          // Group/section name
    EntryCount int       `json:"entry_count"`   // Number of entries in group
    FirstSeen  time.Time `json:"first_seen"`    // Timestamp of first entry
    LastSeen   time.Time `json:"last_seen"`     // Timestamp of last entry
    Commands   int       `json:"commands"`      // Number of command entries
    Progress   int       `json:"progress"`      // Number of progress entries
}

type QueryResult struct {
    Groups  []GroupInfo       `json:"groups,omitempty"`   // Group results (list-groups)
    Entries []ParquetLogEntry `json:"entries,omitempty"`  // Entry results (by-group)
    Stats   QueryStats        `json:"stats,omitempty"`    // Performance statistics
}

type QueryStats struct {
    TotalEntries   int     `json:"total_entries"`   // Total entries in file
    MatchedEntries int     `json:"matched_entries"` // Entries matching query
    TotalGroups    int     `json:"total_groups"`    // Total groups found
    QueryTime      float64 `json:"query_time_ms"`   // Query execution time (ms)
}
```

## Performance

### Benchmarks

The parser includes comprehensive benchmarks to measure performance. Run them with:

```bash
go test -bench=. -benchmem
```

#### Key Results (Apple M3 Pro)

**Single Line Parsing (Byte-based):**
- OSC sequence with timestamp: ~64 ns/op, 192 B/op, 3 allocs/op
- Regular line (no timestamp): ~29 ns/op, 128 B/op, 2 allocs/op
- ANSI-heavy line: ~68 ns/op, 224 B/op, 3 allocs/op
- Progress line: ~65 ns/op, 192 B/op, 3 allocs/op

**Memory Usage Comparison (10,000 lines):**
- **Iterator approach**: ~3.5 MB allocated, 64,007 allocations
- **Legacy ParseReader**: ~3.8 MB allocated, 64,025 allocations
- **Seq2 Iterator**: ~3.5 MB allocated, 64,006 allocations
- **Memory savings**: ~8% reduction with iterators vs ParseReader

**Throughput:**
- **100 lines**: ~50,000 ops/sec (iterator), ~51,000 ops/sec (Seq2)
- **1,000 lines**: ~5,200 ops/sec (iterator), ~5,200 ops/sec (Seq2)
- **10,000 lines**: ~510 ops/sec (iterator), ~510 ops/sec (Seq2)
- **100,000 lines**: ~54 ops/sec (iterator), ~54 ops/sec (Seq2)

**ANSI Stripping**: ~7.7M ops/sec, 160 B/op, 2 allocs/op

**Parquet Export Performance (1,000 lines, Apache Arrow):**
- **Slice export**: ~1,500 ops/sec, 780 KB allocated
- **Iterator export**: ~1,100 ops/sec, 1.1 MB allocated
- **Seq2 export**: ~1,100 ops/sec, 1.2 MB allocated

**Content Classification Performance (1,000 entries):**
- **IsCommand()**: ~15,000 ops/sec, 84 KB allocated
- **IsGroup()**: ~14,000 ops/sec, 84 KB allocated
- **IsProgress()**: ~64,000 ops/sec, 9.5 KB allocated
- **CleanContent()**: ~15,000 ops/sec, 84 KB allocated

**Parquet Query Performance (Apache Arrow Go v18, 10 entries):**
- **ReadEntries**: ~175μs, 370 KB allocated, 1,626 allocs/op
- **ListGroups**: ~173μs, 370 KB allocated, 1,635 allocs/op  
- **FilterByGroup**: ~175μs, 370 KB allocated, 1,638 allocs/op
- **Complete Query**: ~174μs, 371 KB allocated, 1,636 allocs/op
- **Throughput**: ~5,700 operations/sec

**Query Scalability (in-memory processing):**
- **ListGroups (1,000 entries)**: ~13.5μs, 1.7 KB allocated, 12 allocs/op
- **FilterByGroup (1,000 entries)**: ~77μs, 100 KB allocated, 1,010 allocs/op
- **ListGroups (10,000 entries)**: ~127μs, 1.7 KB allocated, 12 allocs/op
- **FilterByGroup (10,000 entries)**: ~805μs, 1.0 MB allocated, 10,015 allocs/op

### Performance Improvements

**Byte-based Parser vs Regex:**
- **10x faster** OSC sequence parsing (~46ns vs ~477ns)
- **10x faster** ANSI stripping (~127ns vs ~1311ns)  
- **Fewer allocations** (2 vs 5 for ANSI stripping)
- **Better memory efficiency** for complex lines

**Iterator Memory Efficiency:**
- **13% memory reduction** compared to loading all entries
- **Constant memory footprint** regardless of file size
- **Streaming processing** for large files
- **Early termination** capability

## Testing

Run the test suite:
```bash
go test -v
```

Run benchmarks:
```bash
go test -bench=. -benchmem
```

The tests cover:
- OSC sequence parsing
- Timestamp extraction
- ANSI code stripping
- Content classification
- Stream processing
- Iterator functionality
- Memory usage patterns

## Acknowledgments

This library was developed with assistance from Claude (Anthropic) for parsing, query functionality, and performance optimization.

## License

This project is licensed under the MIT License.
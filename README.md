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
- **Multiple Output Formats**: Text, JSON, and Parquet export
- **Filtering**: Filter logs by entry type (command, group, progress)
- **Stream Processing**: Parse from any `io.Reader`
- **Group Tracking**: Automatically associate entries with build groups/sections
- **Parquet Export**: Efficient columnar storage for analytics and data processing

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

```bash
go build -o bklog ./cmd/bklog
```

### Examples

**Parse a log file with timestamps:**
```bash
./bklog -file buildkite.log -strip-ansi
```

**Output only commands:**
```bash
./bklog -file buildkite.log -filter command -strip-ansi
```

**Output only group headers:**
```bash
./bklog -file buildkite.log -filter group -strip-ansi
```

**JSON output:**
```bash
./bklog -file buildkite.log -json -strip-ansi
```

**Show processing statistics:**
```bash
./bklog -file buildkite.log -summary -strip-ansi
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
./bklog -file buildkite.log -groups -strip-ansi | head -5
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
./bklog -file buildkite.log -parquet output.parquet -summary
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
./bklog -file buildkite.log -parquet commands.parquet -filter command -summary
```
This exports only command entries to a smaller Parquet file for analysis.

### CLI Options

- `-file <path>`: Path to Buildkite log file (required)
- `-json`: Output as JSON instead of text
- `-strip-ansi`: Remove ANSI escape sequences from output
- `-filter <type>`: Filter entries by type (`command`, `group`, `progress`)
- `-summary`: Show processing summary at the end
- `-groups`: Show group/section information for each entry
- `-parquet <path>`: Export to Parquet file (e.g., output.parquet)

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

The parser can export log entries to [Apache Parquet](https://parquet.apache.org/) format for efficient storage and analysis:

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
./bklog -file buildkite.log -parquet output.parquet
```

**Export with filtering:**
```bash
./bklog -file buildkite.log -parquet commands.parquet -filter command
```

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

// Create a new Parquet writer for streaming
func NewParquetWriter(file *os.File) *ParquetWriter

// Write a batch of entries to Parquet
func (pw *ParquetWriter) WriteBatch(entries []*LogEntry) error

// Close the Parquet writer
func (pw *ParquetWriter) Close() error
```

## Performance

### Benchmarks

The parser includes comprehensive benchmarks to measure performance. Run them with:

```bash
go test -bench=. -benchmem
```

#### Key Results (Apple M3 Pro)

**Single Line Parsing (Byte-based):**
- OSC sequence with timestamp: ~46 ns/op, 112 B/op, 2 allocs/op
- Regular line (no timestamp): ~14 ns/op, 64 B/op, 1 allocs/op

**Memory Usage Comparison (10,000 lines):**
- **Iterator approach**: ~2.3 MB allocated, 38,673 allocations
- **Legacy ParseReader**: ~2.6 MB allocated, 38,691 allocations
- **Memory savings**: ~13% reduction with iterator

**Throughput:**
- **100 lines**: ~25,000 ops/sec
- **1,000 lines**: ~2,500 ops/sec  
- **10,000 lines**: ~250 ops/sec
- **100,000 lines**: ~25 ops/sec

**ANSI Stripping (Byte-based)**: ~8.3M ops/sec, 160 B/op, 2 allocs/op

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

## License

This project is licensed under the MIT License.
package buildkitelogs

import (
	"bufio"
	"io"
	"strings"
	"time"
)

// LogEntry represents a parsed Buildkite log entry
type LogEntry struct {
	Timestamp time.Time
	Content   string
	RawLine   []byte
	Group     string // The current section/group this entry belongs to
}

// Parser handles parsing of Buildkite log files
type Parser struct {
	byteParser   *ByteParser
	currentGroup string
}

// LogIterator provides an iterator interface for processing log entries
type LogIterator struct {
	scanner *bufio.Scanner
	parser  *Parser
	current *LogEntry
	err     error
}

// NewParser creates a new Buildkite log parser
func NewParser() *Parser {
	return &Parser{
		byteParser: NewByteParser(),
	}
}

// ParseLine parses a single log line
func (p *Parser) ParseLine(line string) (*LogEntry, error) {
	entry, err := p.byteParser.ParseLine(line)
	if err != nil {
		return nil, err
	}
	
	// Update current group if this is a group header
	if entry.IsGroup() {
		p.currentGroup = entry.CleanContent()
	}
	
	// Set the group for this entry
	entry.Group = p.currentGroup
	
	return entry, nil
}

// ParseReader parses log entries from an io.Reader
// Deprecated: Use NewIterator for memory-efficient processing of large files
func (p *Parser) ParseReader(reader io.Reader) ([]*LogEntry, error) {
	var entries []*LogEntry
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()
		entry, err := p.ParseLine(line)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// NewIterator creates a new LogIterator for memory-efficient processing
func (p *Parser) NewIterator(reader io.Reader) *LogIterator {
	return &LogIterator{
		scanner: bufio.NewScanner(reader),
		parser:  p,
	}
}

// Next advances the iterator to the next log entry
// Returns true if there is a next entry, false if EOF or error
func (iter *LogIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	if !iter.scanner.Scan() {
		iter.err = iter.scanner.Err()
		return false
	}

	line := iter.scanner.Text()
	entry, err := iter.parser.ParseLine(line)
	if err != nil {
		iter.err = err
		return false
	}

	iter.current = entry
	return true
}

// Entry returns the current log entry
// Only valid after a successful call to Next()
func (iter *LogIterator) Entry() *LogEntry {
	return iter.current
}

// Err returns any error encountered during iteration
func (iter *LogIterator) Err() error {
	return iter.err
}

// StripANSI removes ANSI escape sequences from content
func (p *Parser) StripANSI(content string) string {
	return p.byteParser.StripANSI(content)
}

// CleanContent returns the content with ANSI codes stripped
func (entry *LogEntry) CleanContent() string {
	parser := NewParser()
	return parser.StripANSI(entry.Content)
}

// HasTimestamp returns true if the log entry has a valid timestamp
func (entry *LogEntry) HasTimestamp() bool {
	return !entry.Timestamp.IsZero()
}

// IsCommand returns true if the log entry appears to be a command execution
func (entry *LogEntry) IsCommand() bool {
	clean := entry.CleanContent()
	return strings.HasPrefix(clean, "$ ")
}

// IsProgress returns true if the log entry appears to be a progress update
// Progress lines are identified by [K (erase-in-line) sequences anywhere in the content,
// but more conservatively than before - looking for the specific pattern where [K
// appears in contexts that indicate terminal progress updates
func (entry *LogEntry) IsProgress() bool {
	content := entry.Content

	// Look for [K sequences in the content
	if !strings.Contains(content, "[K") {
		return false
	}

	// Additional validation: should be git progress-related content
	cleanContent := entry.CleanContent()
	return strings.Contains(cleanContent, "objects") ||
		strings.Contains(cleanContent, "deltas") ||
		strings.Contains(cleanContent, "%")
}

// IsGroup returns true if the log entry appears to be a group header
func (entry *LogEntry) IsGroup() bool {
	content := entry.CleanContent()
	return strings.HasPrefix(content, "~~~") || strings.HasPrefix(content, "---") || strings.HasPrefix(content, "+++")
}

// IsSection is deprecated, use IsGroup instead
func (entry *LogEntry) IsSection() bool {
	return entry.IsGroup()
}

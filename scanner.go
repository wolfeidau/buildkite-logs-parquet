package buildkitelogs

import (
	"bytes"
	"strconv"
	"time"
)

// ByteParser handles byte-level parsing of Buildkite log files
type ByteParser struct{}

// NewByteParser creates a new byte-based parser
func NewByteParser() *ByteParser {
	return &ByteParser{}
}

// ParseLine parses a single log line using byte scanning
func (p *ByteParser) ParseLine(line string) (*LogEntry, error) {
	data := []byte(line)

	// Check for OSC sequence: ESC_bk;t=timestamp BEL content
	if len(data) < 10 { // Minimum: \x1b_bk;t=1\x07
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Look for OSC start sequence: \x1b_bk;t=
	if !hasOSCStart(data) {
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Find the timestamp and content
	timestampStart := 7 // After \x1b_bk;t=
	timestampEnd := findBEL(data, timestampStart)
	if timestampEnd == -1 {
		return &LogEntry{
			Timestamp: time.Time{},
			Content:   line,
			RawLine:   data,
			Group:     "",
		}, nil
	}

	// Extract timestamp
	timestampBytes := data[timestampStart:timestampEnd]
	timestampMs, err := strconv.ParseInt(string(timestampBytes), 10, 64)
	if err != nil {
		return nil, err
	}

	timestamp := time.Unix(0, timestampMs*int64(time.Millisecond))

	// Extract content (after BEL)
	content := string(data[timestampEnd+1:])

	return &LogEntry{
		Timestamp: timestamp,
		Content:   content,
		RawLine:   data,
		Group:     "",
	}, nil
}

// hasOSCStart checks if data starts with \x1b_bk;t=
func hasOSCStart(data []byte) bool {
	if len(data) < 7 {
		return false
	}

	return data[0] == 0x1b && // ESC
		bytes.HasPrefix(data[1:], []byte("_bk;t="))
}

// findBEL finds the position of the BEL character (\x07) starting from offset
func findBEL(data []byte, start int) int {
	for i := start; i < len(data); i++ {
		if data[i] == 0x07 {
			return i
		}
	}
	return -1
}

// StripANSI removes ANSI escape sequences using byte scanning
func (p *ByteParser) StripANSI(content string) string {
	data := []byte(content)
	result := make([]byte, 0, len(data))

	i := 0
	for i < len(data) {
		// Check for ANSI escape sequence
		if i < len(data)-1 && data[i] == 0x1b && data[i+1] == '[' {
			// Skip ESC[
			i += 2
			// Skip until we find the final character (letter)
			for i < len(data) && !isANSIFinalChar(data[i]) {
				i++
			}
			// Skip the final character
			if i < len(data) {
				i++
			}
		} else if i < len(data)-1 && data[i] == '[' {
			// Handle sequences that might be missing ESC
			j := i + 1
			hasValidANSI := false

			// Look ahead to see if this looks like an ANSI sequence
			for j < len(data) && j < i+10 { // Limit lookahead
				if data[j] >= '0' && data[j] <= '9' || data[j] == ';' {
					j++
				} else if isANSIFinalChar(data[j]) {
					hasValidANSI = true
					break
				} else {
					break
				}
			}

			if hasValidANSI {
				// Skip the ANSI sequence
				i = j + 1
			} else {
				// Not an ANSI sequence, keep the character
				result = append(result, data[i])
				i++
			}
		} else {
			// Regular character
			result = append(result, data[i])
			i++
		}
	}

	return string(result)
}

// isANSIFinalChar checks if a byte is a valid ANSI sequence final character
func isANSIFinalChar(b byte) bool {
	// ANSI sequences end with letters, typically m, K, H, etc.
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

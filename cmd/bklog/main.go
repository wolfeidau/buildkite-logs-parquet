package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

type Config struct {
	FilePath     string
	OutputJSON   bool
	StripANSI    bool
	Filter       string
	ShowSummary  bool
	ShowGroups   bool
	ParquetFile  string
	UseSeq2      bool
}

type ProcessingSummary struct {
	TotalEntries    int
	FilteredEntries int
	BytesProcessed  int64
	EntriesWithTime int
	Commands        int
	Sections        int
	Progress        int
}

func main() {
	config := &Config{}
	
	flag.StringVar(&config.FilePath, "file", "", "Path to Buildkite log file")
	flag.BoolVar(&config.OutputJSON, "json", false, "Output as JSON")
	flag.BoolVar(&config.StripANSI, "strip-ansi", false, "Strip ANSI escape sequences from output")
	flag.StringVar(&config.Filter, "filter", "", "Filter entries by type: command, progress, group")
	flag.BoolVar(&config.ShowSummary, "summary", false, "Show processing summary at the end")
	flag.BoolVar(&config.ShowGroups, "groups", false, "Show group/section information")
	flag.StringVar(&config.ParquetFile, "parquet", "", "Export to Parquet file (e.g., output.parquet)")
	flag.BoolVar(&config.UseSeq2, "use-seq2", false, "Use Go 1.23+ iter.Seq2 for iteration (experimental)")
	flag.Parse()

	if config.FilePath == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -file <log-file> [options]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if err := run(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(config *Config) error {
	file, err := os.Open(config.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size for bytes processed calculation
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	parser := buildkitelogs.NewParser()
	iterator := parser.NewIterator(file)

	summary := &ProcessingSummary{
		BytesProcessed: fileInfo.Size(),
	}

	// If exporting to Parquet, handle that separately
	if config.ParquetFile != "" {
		if config.UseSeq2 {
			// Use the new Seq2 iterator approach
			err = exportToParquetSeq2(file, parser, config.ParquetFile, config.Filter, summary)
		} else {
			// Use the traditional iterator approach
			err = exportToParquet(iterator, config.ParquetFile, config.Filter, summary)
		}
		
		if err != nil {
			return err
		}
		
		if config.ShowSummary {
			printSummary(summary)
		}
		
		iteratorType := "iterator"
		if config.UseSeq2 {
			iteratorType = "Seq2"
		}
		fmt.Fprintf(os.Stderr, "Exported %d entries to %s (using %s)\n", summary.FilteredEntries, config.ParquetFile, iteratorType)
		return nil
	}

	// Regular output (text or JSON)
	if config.OutputJSON {
		err = outputJSONIterator(iterator, config.Filter, config.StripANSI, config.ShowGroups, summary)
	} else {
		err = outputTextIterator(iterator, config.Filter, config.StripANSI, config.ShowGroups, summary)
	}

	if err != nil {
		return err
	}

	if config.ShowSummary {
		printSummary(summary)
	}

	return nil
}

func shouldIncludeEntry(entry *buildkitelogs.LogEntry, filter string) bool {
	if filter == "" {
		return true
	}

	switch filter {
	case "command":
		return entry.IsCommand()
	case "progress":
		return entry.IsProgress()
	case "group":
		return entry.IsGroup()
	case "section": // deprecated alias for group
		return entry.IsGroup()
	default:
		return true
	}
}

func outputJSONIterator(iterator *buildkitelogs.LogIterator, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
	type JSONEntry struct {
		Timestamp string `json:"timestamp,omitempty"`
		Content   string `json:"content"`
		HasTime   bool   `json:"has_timestamp"`
		Group     string `json:"group,omitempty"`
	}

	var jsonEntries []JSONEntry
	
	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++
		
		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}
		
		if !shouldIncludeEntry(entry, filter) {
			continue
		}
		
		summary.FilteredEntries++
		
		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}
		
		jsonEntry := JSONEntry{
			Content: content,
			HasTime: entry.HasTimestamp(),
		}
		
		if entry.HasTimestamp() {
			jsonEntry.Timestamp = entry.Timestamp.Format("2006-01-02T15:04:05.000Z07:00")
		}
		
		if showGroups && entry.Group != "" {
			jsonEntry.Group = entry.Group
		}
		
		jsonEntries = append(jsonEntries, jsonEntry)
	}
	
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("error reading log: %w", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jsonEntries)
}

func outputTextIterator(iterator *buildkitelogs.LogIterator, filter string, stripANSI bool, showGroups bool, summary *ProcessingSummary) error {
	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++
		
		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}
		
		if !shouldIncludeEntry(entry, filter) {
			continue
		}
		
		summary.FilteredEntries++
		
		content := entry.Content
		if stripANSI {
			content = entry.CleanContent()
		}

		if showGroups && entry.Group != "" {
			if entry.HasTimestamp() {
				fmt.Printf("[%s] [%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05.000"), entry.Group, content)
			} else {
				fmt.Printf("[%s] %s\n", entry.Group, content)
			}
		} else {
			if entry.HasTimestamp() {
				fmt.Printf("[%s] %s\n", entry.Timestamp.Format("2006-01-02 15:04:05.000"), content)
			} else {
				fmt.Printf("%s\n", content)
			}
		}
	}
	
	return iterator.Err()
}

func printSummary(summary *ProcessingSummary) {
	fmt.Fprintf(os.Stderr, "\n--- Processing Summary ---\n")
	fmt.Fprintf(os.Stderr, "Bytes processed: %s\n", formatBytes(summary.BytesProcessed))
	fmt.Fprintf(os.Stderr, "Total entries: %d\n", summary.TotalEntries)
	
	if summary.FilteredEntries != summary.TotalEntries {
		fmt.Fprintf(os.Stderr, "Filtered entries: %d\n", summary.FilteredEntries)
	}
	
	fmt.Fprintf(os.Stderr, "Entries with timestamps: %d\n", summary.EntriesWithTime)
	fmt.Fprintf(os.Stderr, "Commands: %d\n", summary.Commands)
	fmt.Fprintf(os.Stderr, "Sections: %d\n", summary.Sections)
	fmt.Fprintf(os.Stderr, "Progress updates: %d\n", summary.Progress)
	
	regularEntries := summary.TotalEntries - summary.Commands - summary.Sections - summary.Progress
	fmt.Fprintf(os.Stderr, "Regular output: %d\n", regularEntries)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func exportToParquet(iterator *buildkitelogs.LogIterator, filename string, filter string, summary *ProcessingSummary) error {
	// Create output file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Parquet writer
	writer := buildkitelogs.NewParquetWriter(file)
	defer writer.Close()

	// Process entries in batches for memory efficiency
	const batchSize = 1000
	batch := make([]*buildkitelogs.LogEntry, 0, batchSize)

	for iterator.Next() {
		entry := iterator.Entry()
		summary.TotalEntries++
		
		// Update entry type counts
		if entry.HasTimestamp() {
			summary.EntriesWithTime++
		}
		if entry.IsCommand() {
			summary.Commands++
		}
		if entry.IsGroup() {
			summary.Sections++
		}
		if entry.IsProgress() {
			summary.Progress++
		}
		
		if !shouldIncludeEntry(entry, filter) {
			continue
		}
		
		summary.FilteredEntries++
		batch = append(batch, entry)
		
		// Write batch when it's full
		if len(batch) >= batchSize {
			err := writer.WriteBatch(batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Write remaining entries
	if len(batch) > 0 {
		err := writer.WriteBatch(batch)
		if err != nil {
			return err
		}
	}

	return iterator.Err()
}

func exportToParquetSeq2(file *os.File, parser *buildkitelogs.Parser, filename string, filter string, summary *ProcessingSummary) error {
	// Create filter function based on filter string
	var filterFunc func(*buildkitelogs.LogEntry) bool
	if filter != "" {
		filterFunc = func(entry *buildkitelogs.LogEntry) bool {
			return shouldIncludeEntry(entry, filter)
		}
	}

	// Create a sequence that counts entries for summary and handles errors
	countingSeq := func(yield func(*buildkitelogs.LogEntry, error) bool) {
		// Reset file position to beginning
		file.Seek(0, 0)
		
		lineNum := 0
		for entry, err := range parser.All(file) {
			lineNum++
			
			// Handle parse errors - still count them but log warnings
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Error parsing line %d: %v\n", lineNum, err)
				if !yield(nil, err) {
					return
				}
				continue
			}
			
			summary.TotalEntries++
			
			// Update entry type counts
			if entry.HasTimestamp() {
				summary.EntriesWithTime++
			}
			if entry.IsCommand() {
				summary.Commands++
			}
			if entry.IsGroup() {
				summary.Sections++
			}
			if entry.IsProgress() {
				summary.Progress++
			}
			
			// Apply filter if specified
			if filterFunc == nil || filterFunc(entry) {
				summary.FilteredEntries++
			}
			
			// Always yield the entry for export consideration
			if !yield(entry, nil) {
				return
			}
		}
	}

	// Export using the Seq2 iterator with filtering
	return buildkitelogs.ExportSeq2ToParquetWithFilter(countingSeq, filename, filterFunc)
}
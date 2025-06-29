package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

// QueryConfig holds configuration for CLI query operations
type QueryConfig struct {
	ParquetFile string
	Operation   string // "list-groups", "by-group"
	GroupName   string
	Format      string // "text", "json"
	ShowStats   bool
}

// runQuery executes a query using the library and formats the output
func runQuery(config *QueryConfig) error {
	reader := buildkitelogs.NewParquetReader(config.ParquetFile)
	
	result, err := reader.Query(config.Operation, config.GroupName)
	if err != nil {
		return err
	}

	return formatQueryResult(result, *config)
}

// formatQueryResult formats and prints the query result
func formatQueryResult(result *buildkitelogs.QueryResult, config QueryConfig) error {
	if config.Format == "json" {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	switch config.Operation {
	case "list-groups":
		fmt.Printf("Groups found: %d\n\n", len(result.Groups))
		
		if len(result.Groups) == 0 {
			fmt.Println("No groups found.")
			return nil
		}

		// Print table header
		fmt.Printf("%-40s %8s %8s %8s %19s %19s\n", 
			"GROUP NAME", "ENTRIES", "COMMANDS", "PROGRESS", "FIRST SEEN", "LAST SEEN")
		fmt.Println(strings.Repeat("-", 120))

		for _, group := range result.Groups {
			fmt.Printf("%-40s %8d %8d %8d %19s %19s\n",
				truncateString(group.Name, 40),
				group.EntryCount,
				group.Commands,
				group.Progress,
				group.FirstSeen.Format("2006-01-02 15:04:05"),
				group.LastSeen.Format("2006-01-02 15:04:05"))
		}

	case "by-group":
		fmt.Printf("Entries in group matching '%s': %d\n\n", config.GroupName, len(result.Entries))
		
		if len(result.Entries) == 0 {
			fmt.Println("No entries found for the specified group.")
			return nil
		}

		for _, entry := range result.Entries {
			timestamp := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
			
			var markers []string
			if entry.IsCommand {
				markers = append(markers, "CMD")
			}
			if entry.IsGroup {
				markers = append(markers, "GRP")
			}
			if entry.IsProgress {
				markers = append(markers, "PROG")
			}
			
			markerStr := ""
			if len(markers) > 0 {
				markerStr = fmt.Sprintf(" [%s]", strings.Join(markers, ","))
			}

			fmt.Printf("[%s]%s %s\n", 
				timestamp.Format("2006-01-02 15:04:05.000"), 
				markerStr,
				entry.Content)
		}
	}

	if config.ShowStats {
		fmt.Printf("\n--- Query Statistics ---\n")
		fmt.Printf("Total entries: %d\n", result.Stats.TotalEntries)
		fmt.Printf("Matched entries: %d\n", result.Stats.MatchedEntries)
		if result.Stats.TotalGroups > 0 {
			fmt.Printf("Total groups: %d\n", result.Stats.TotalGroups)
		}
		fmt.Printf("Query time: %.2f ms\n", result.Stats.QueryTime)
	}

	return nil
}

// truncateString truncates a string to the specified length
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
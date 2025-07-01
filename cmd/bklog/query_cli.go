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
	ParquetFile  string
	Operation    string // "list-groups", "by-group"
	GroupName    string
	Format       string // "text", "json"
	ShowStats    bool
	UseStreaming bool // Use streaming iterators for better performance
	LimitEntries int  // Limit output entries (0 = no limit)
}

// runQuery executes a query using the library and formats the output
func runQuery(config *QueryConfig) error {
	reader := buildkitelogs.NewParquetReader(config.ParquetFile)

	if config.UseStreaming {
		return runStreamingQuery(reader, config)
	}

	// Fallback to traditional query approach
	result, err := reader.Query(config.Operation, config.GroupName)
	if err != nil {
		return err
	}

	return formatQueryResult(result, *config)
}

// runStreamingQuery executes streaming queries for better memory efficiency
func runStreamingQuery(reader *buildkitelogs.ParquetReader, config *QueryConfig) error {
	start := time.Now()

	switch config.Operation {
	case "list-groups":
		return streamListGroups(reader, config, start)
	case "by-group":
		if config.GroupName == "" {
			return fmt.Errorf("group pattern is required for by-group operation")
		}
		return streamByGroup(reader, config, start)
	default:
		return fmt.Errorf("unknown operation: %s", config.Operation)
	}
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

// streamListGroups handles list-groups operation using streaming
func streamListGroups(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	// Use streaming iterator to build group statistics
	groupMap := make(map[string]*buildkitelogs.GroupInfo)
	totalEntries := 0

	for entry, err := range reader.ReadEntriesIter() {
		if err != nil {
			return fmt.Errorf("error reading entries: %w", err)
		}

		totalEntries++

		groupName := entry.Group
		if groupName == "" {
			groupName = "<no group>"
		}

		info, exists := groupMap[groupName]
		if !exists {
			entryTime := time.Unix(0, entry.Timestamp*int64(time.Millisecond))
			info = &buildkitelogs.GroupInfo{
				Name:      groupName,
				FirstSeen: entryTime,
				LastSeen:  entryTime,
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

	// Convert to slice and sort
	groups := make([]buildkitelogs.GroupInfo, 0, len(groupMap))
	for _, info := range groupMap {
		groups = append(groups, *info)
	}

	// Sort by first seen time (simple sorting)
	for i := 0; i < len(groups)-1; i++ {
		for j := i + 1; j < len(groups); j++ {
			if groups[j].FirstSeen.Before(groups[i].FirstSeen) {
				groups[i], groups[j] = groups[j], groups[i]
			}
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatStreamingGroupsResult(groups, totalEntries, queryTime, config)
}

// streamByGroup handles by-group operation using streaming with optional limiting
func streamByGroup(reader *buildkitelogs.ParquetReader, config *QueryConfig, start time.Time) error {
	var entries []buildkitelogs.ParquetLogEntry
	totalEntries := 0
	matchedEntries := 0

	for entry, err := range reader.FilterByGroupIter(config.GroupName) {
		if err != nil {
			return fmt.Errorf("error filtering entries: %w", err)
		}

		totalEntries++
		matchedEntries++
		entries = append(entries, entry)

		// Apply limit if specified (early termination advantage)
		if config.LimitEntries > 0 && matchedEntries >= config.LimitEntries {
			break
		}
	}

	// Count total entries for stats if needed (requires separate iteration)
	if config.ShowStats {
		for _, _ = range reader.ReadEntriesIter() {
			totalEntries++
		}
	}

	// Format output
	queryTime := float64(time.Since(start).Nanoseconds()) / 1e6
	return formatStreamingEntriesResult(entries, totalEntries, matchedEntries, queryTime, config)
}

// formatStreamingGroupsResult formats groups output from streaming query
func formatStreamingGroupsResult(groups []buildkitelogs.GroupInfo, totalEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		result := struct {
			Groups []buildkitelogs.GroupInfo `json:"groups"`
			Stats  struct {
				TotalEntries int     `json:"total_entries"`
				TotalGroups  int     `json:"total_groups"`
				QueryTime    float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Groups: groups,
		}

		if config.ShowStats {
			result.Stats.TotalEntries = totalEntries
			result.Stats.TotalGroups = len(groups)
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	fmt.Printf("Groups found: %d\n\n", len(groups))

	if len(groups) == 0 {
		fmt.Println("No groups found.")
		return nil
	}

	// Print table header
	fmt.Printf("%-40s %8s %8s %8s %19s %19s\n",
		"GROUP NAME", "ENTRIES", "COMMANDS", "PROGRESS", "FIRST SEEN", "LAST SEEN")
	fmt.Println(strings.Repeat("-", 120))

	for _, group := range groups {
		fmt.Printf("%-40s %8d %8d %8d %19s %19s\n",
			truncateString(group.Name, 40),
			group.EntryCount,
			group.Commands,
			group.Progress,
			group.FirstSeen.Format("2006-01-02 15:04:05"),
			group.LastSeen.Format("2006-01-02 15:04:05"))
	}

	if config.ShowStats {
		fmt.Printf("\n--- Query Statistics (Streaming) ---\n")
		fmt.Printf("Total entries: %d\n", totalEntries)
		fmt.Printf("Total groups: %d\n", len(groups))
		fmt.Printf("Query time: %.2f ms\n", queryTime)
	}

	return nil
}

// formatStreamingEntriesResult formats entries output from streaming query
func formatStreamingEntriesResult(entries []buildkitelogs.ParquetLogEntry, totalEntries, matchedEntries int, queryTime float64, config *QueryConfig) error {
	if config.Format == "json" {
		result := struct {
			Entries []buildkitelogs.ParquetLogEntry `json:"entries"`
			Stats   struct {
				TotalEntries   int     `json:"total_entries"`
				MatchedEntries int     `json:"matched_entries"`
				QueryTime      float64 `json:"query_time_ms"`
			} `json:"stats,omitempty"`
		}{
			Entries: entries,
		}

		if config.ShowStats {
			result.Stats.TotalEntries = totalEntries
			result.Stats.MatchedEntries = matchedEntries
			result.Stats.QueryTime = queryTime
		}

		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}

	// Text format
	limitText := ""
	if config.LimitEntries > 0 && matchedEntries >= config.LimitEntries {
		limitText = fmt.Sprintf(" (limited to %d)", config.LimitEntries)
	}
	fmt.Printf("Entries in group matching '%s': %d%s\n\n", config.GroupName, matchedEntries, limitText)

	if len(entries) == 0 {
		fmt.Println("No entries found for the specified group.")
		return nil
	}

	for _, entry := range entries {
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

	if config.ShowStats {
		fmt.Printf("\n--- Query Statistics (Streaming) ---\n")
		if totalEntries > 0 {
			fmt.Printf("Total entries: %d\n", totalEntries)
		}
		fmt.Printf("Matched entries: %d\n", matchedEntries)
		fmt.Printf("Query time: %.2f ms\n", queryTime)
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

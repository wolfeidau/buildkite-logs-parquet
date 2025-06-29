package main

import (
	"fmt"
	"log"
	"strings"

	buildkitelogs "github.com/wolfeidau/buildkite-logs-parquet"
)

func main() {
	// Example: Using the ParquetReader to query Buildkite logs
	
	// Create a new reader for your Parquet file
	reader := buildkitelogs.NewParquetReader("../test_logs.parquet")
	
	fmt.Println("🔍 Buildkite Logs Parquet Query Example")
	fmt.Println(strings.Repeat("=", 50))
	
	// Example 1: List all groups
	fmt.Println("\n📊 Listing all log groups:")
	groups, err := reader.ListGroups()
	if err != nil {
		log.Fatalf("Failed to list groups: %v", err)
	}
	
	for i, group := range groups {
		fmt.Printf("%d. %s (%d entries, %d commands)\n", 
			i+1, group.Name, group.EntryCount, group.Commands)
	}
	
	// Example 2: Filter by group pattern
	fmt.Println("\n🔎 Filtering entries containing 'environment':")
	entries, err := reader.FilterByGroup("environment")
	if err != nil {
		log.Fatalf("Failed to filter by group: %v", err)
	}
	
	for _, entry := range entries {
		fmt.Printf("  [%s] %s\n", entry.Group, entry.Content)
	}
	
	// Example 3: Complete query with timing
	fmt.Println("\n⚡ Running complete query operation:")
	result, err := reader.Query("list-groups", "")
	if err != nil {
		log.Fatalf("Failed to run query: %v", err)
	}
	
	fmt.Printf("Found %d groups in %.2f ms\n", 
		len(result.Groups), result.Stats.QueryTime)
	
	// Example 4: Direct file reading
	fmt.Println("\n📖 Reading entries directly:")
	allEntries, err := buildkitelogs.ReadParquetFile("../test_logs.parquet")
	if err != nil {
		log.Fatalf("Failed to read Parquet file: %v", err)
	}
	
	fmt.Printf("Read %d entries from Parquet file\n", len(allEntries))
	
	// Example 5: Using standalone functions
	fmt.Println("\n🔧 Using standalone functions:")
	groups = buildkitelogs.ListGroups(allEntries)
	filtered := buildkitelogs.FilterByGroup(allEntries, "test")
	
	fmt.Printf("Groups: %d, Filtered entries: %d\n", len(groups), len(filtered))
	
	fmt.Println("\n✅ Example completed successfully!")
}
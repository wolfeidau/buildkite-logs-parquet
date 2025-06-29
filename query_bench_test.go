package buildkitelogs

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// BenchmarkParquetReader_ListGroups benchmarks the ListGroups method
func BenchmarkParquetReader_ListGroups(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	reader := NewParquetReader(testFile)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		groups, err := reader.ListGroups()
		if err != nil {
			b.Fatalf("ListGroups failed: %v", err)
		}
		if len(groups) == 0 {
			b.Fatal("No groups found")
		}
	}
}

// BenchmarkParquetReader_FilterByGroup benchmarks the FilterByGroup method
func BenchmarkParquetReader_FilterByGroup(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	reader := NewParquetReader(testFile)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entries, err := reader.FilterByGroup("environment")
		if err != nil {
			b.Fatalf("FilterByGroup failed: %v", err)
		}
		if len(entries) == 0 {
			b.Fatal("No entries found")
		}
	}
}

// BenchmarkParquetReader_ReadEntries benchmarks the ReadEntries method
func BenchmarkParquetReader_ReadEntries(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found - run parse command first to generate test data")
	}

	reader := NewParquetReader(testFile)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entries, err := reader.ReadEntries()
		if err != nil {
			b.Fatalf("ReadEntries failed: %v", err)
		}
		if len(entries) == 0 {
			b.Fatal("No entries found")
		}
	}
}

// BenchmarkParquetReader_Query benchmarks the Query method with different operations
func BenchmarkParquetReader_Query(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("ListGroups", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			result, err := reader.Query("list-groups", "")
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			if len(result.Groups) == 0 {
				b.Fatal("No groups found")
			}
		}
	})

	b.Run("FilterByGroup", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			result, err := reader.Query("by-group", "environment")
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			if len(result.Entries) == 0 {
				b.Fatal("No entries found")
			}
		}
	})
}

// BenchmarkReadParquetFile benchmarks the standalone ReadParquetFile function
func BenchmarkReadParquetFile(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entries, err := ReadParquetFile(testFile)
		if err != nil {
			b.Fatalf("ReadParquetFile failed: %v", err)
		}
		if len(entries) == 0 {
			b.Fatal("No entries found")
		}
	}
}

// BenchmarkListGroups benchmarks the standalone ListGroups function
func BenchmarkListGroups(b *testing.B) {
	// Create mock data for consistent benchmarking
	mockEntries := createMockParquetEntries(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		groups := ListGroups(mockEntries)
		if len(groups) == 0 {
			b.Fatal("No groups found")
		}
	}
}

// BenchmarkFilterByGroup benchmarks the standalone FilterByGroup function
func BenchmarkFilterByGroup(b *testing.B) {
	// Create mock data for consistent benchmarking
	mockEntries := createMockParquetEntries(1000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		filtered := FilterByGroup(mockEntries, "test")
		_ = filtered // Use the result to prevent optimization
	}
}

// BenchmarkParquetReaderOperations compares different operations on the same reader
func BenchmarkParquetReaderOperations(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.Run("ReadEntries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entries, err := reader.ReadEntries()
			if err != nil {
				b.Fatalf("ReadEntries failed: %v", err)
			}
			_ = entries
		}
	})

	b.Run("ListGroups", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			groups, err := reader.ListGroups()
			if err != nil {
				b.Fatalf("ListGroups failed: %v", err)
			}
			_ = groups
		}
	})

	b.Run("FilterByGroup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			entries, err := reader.FilterByGroup("environment")
			if err != nil {
				b.Fatalf("FilterByGroup failed: %v", err)
			}
			_ = entries
		}
	})
}

// BenchmarkMemoryEfficiency tests memory allocation patterns
func BenchmarkMemoryEfficiency(b *testing.B) {
	testFile := "test_logs.parquet"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("test_logs.parquet not found")
	}

	reader := NewParquetReader(testFile)

	b.ResetTimer()
	b.ReportAllocs()

	var result *QueryResult
	for i := 0; i < b.N; i++ {
		var err error
		result, err = reader.Query("list-groups", "")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
	
	// Use result to prevent optimization
	_ = result
}

// createMockParquetEntries creates mock ParquetLogEntry data for benchmarking
func createMockParquetEntries(count int) []ParquetLogEntry {
	baseTime := time.Date(2025, 4, 22, 21, 43, 29, 0, time.UTC).UnixMilli()
	entries := make([]ParquetLogEntry, count)
	
	groups := []string{
		"~~~ Running global environment hook",
		"~~~ Running global pre-checkout hook", 
		"~~~ Preparing working directory",
		"--- :package: Build job checkout directory",
		"+++ :hammer: Example tests",
		"~~~ Running tests",
		"--- Build and Deploy",
		"+++ Testing Phase",
	}

	for i := 0; i < count; i++ {
		groupIdx := i % len(groups)
		entries[i] = ParquetLogEntry{
			Timestamp:   baseTime + int64(i*10),
			Content:     fmt.Sprintf("Log entry %d content for testing", i),
			Group:       groups[groupIdx],
			HasTime:     true,
			IsCommand:   i%4 == 0, // Every 4th entry is a command
			IsGroup:     i%10 == 0, // Every 10th entry starts a group
			IsProgress:  i%20 == 0, // Every 20th entry is progress
			RawLineSize: int32(20 + (i % 100)), // Variable line sizes
		}
	}
	
	return entries
}

// BenchmarkScalability tests performance with different data sizes
func BenchmarkScalability(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("ListGroups_%d_entries", size), func(b *testing.B) {
			entries := createMockParquetEntries(size)
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				groups := ListGroups(entries)
				_ = groups
			}
		})
		
		b.Run(fmt.Sprintf("FilterByGroup_%d_entries", size), func(b *testing.B) {
			entries := createMockParquetEntries(size)
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				filtered := FilterByGroup(entries, "test")
				_ = filtered
			}
		})
	}
}
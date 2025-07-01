package buildkitelogs

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRealWorldPerformance tests performance with actual parquet files
func TestRealWorldPerformance(t *testing.T) {
	testFiles := []struct {
		name string
		path string
	}{
		{"small", "testdata/bash-example.parquet"},
		{"medium", "testdata/bazel-bazel_build_32517_rocky-rocky-linux-8.parquet"},
		{"large", "testdata/bun_build_19487_windows-x64-build-cpp.parquet"},
	}

	for _, tf := range testFiles {
		t.Run(tf.name, func(t *testing.T) {
			if _, err := os.Stat(tf.path); os.IsNotExist(err) {
				t.Skipf("Test file %s not found", tf.path)
			}

			reader := NewParquetReader(tf.path)

			// Test array approach
			start := time.Now()
			entries, err := reader.ReadEntries()
			if err != nil {
				t.Fatalf("ReadEntries failed: %v", err)
			}
			arrayTime := time.Since(start)
			arrayCount := len(entries)

			// Test iterator approach
			start = time.Now()
			iterCount := 0
			for entry, err := range reader.ReadEntriesIter() {
				if err != nil {
					t.Fatalf("ReadEntriesIter failed: %v", err)
				}
				iterCount++
				_ = entry
			}
			iterTime := time.Since(start)

			// Report results
			t.Logf("File: %s", tf.name)
			t.Logf("Array approach: %d entries in %v", arrayCount, arrayTime)
			t.Logf("Iterator approach: %d entries in %v", iterCount, iterTime)
			t.Logf("Performance ratio: %.2fx", float64(arrayTime)/float64(iterTime))

			if arrayCount != iterCount {
				t.Errorf("Entry count mismatch: array=%d, iterator=%d", arrayCount, iterCount)
			}
		})
	}
}

// BenchmarkRealWorldFiles benchmarks with actual parquet files
func BenchmarkRealWorldFiles(b *testing.B) {
	testFiles := []struct {
		name string
		path string
	}{
		{"large", "testdata/bun_build_19487_windows-x64-build-cpp.parquet"},
	}

	for _, tf := range testFiles {
		if _, err := os.Stat(tf.path); os.IsNotExist(err) {
			b.Skipf("Test file %s not found", tf.path)
		}

		reader := NewParquetReader(tf.path)

		b.Run(fmt.Sprintf("%s_Array", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				entries, err := reader.ReadEntries()
				if err != nil {
					b.Fatalf("ReadEntries failed: %v", err)
				}
				_ = entries
			}
		})

		b.Run(fmt.Sprintf("%s_Iterator", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				count := 0
				for entry, err := range reader.ReadEntriesIter() {
					if err != nil {
						b.Fatalf("ReadEntriesIter failed: %v", err)
					}
					count++
					_ = entry
				}
			}
		})

		// Test early termination advantage
		b.Run(fmt.Sprintf("%s_EarlyTermination_Array", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				entries, err := reader.ReadEntries()
				if err != nil {
					b.Fatalf("ReadEntries failed: %v", err)
				}
				
				// Process only first 1000 entries
				for j, entry := range entries {
					if j >= 1000 {
						break
					}
					_ = entry.Content
				}
			}
		})

		b.Run(fmt.Sprintf("%s_EarlyTermination_Iterator", tf.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				count := 0
				for entry, err := range reader.ReadEntriesIter() {
					if err != nil {
						b.Fatalf("ReadEntriesIter failed: %v", err)
					}
					
					_ = entry.Content
					count++
					if count >= 1000 {
						break
					}
				}
			}
		})
	}
}
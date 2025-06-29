package buildkitelogs

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

func TestValidateAPIParams(t *testing.T) {
	tests := []struct {
		name         string
		org          string
		pipeline     string
		build        string
		job          string
		expectError  bool
		errorMessage string
	}{
		{
			name:        "all_params_provided",
			org:         "myorg",
			pipeline:    "mypipeline",
			build:       "123",
			job:         "abc-def",
			expectError: false,
		},
		{
			name:         "missing_org",
			org:          "",
			pipeline:     "mypipeline",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: organization",
		},
		{
			name:         "missing_pipeline",
			org:          "myorg",
			pipeline:     "",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: pipeline",
		},
		{
			name:         "missing_build",
			org:          "myorg",
			pipeline:     "mypipeline",
			build:        "",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: build",
		},
		{
			name:         "missing_job",
			org:          "myorg",
			pipeline:     "mypipeline",
			build:        "123",
			job:          "",
			expectError:  true,
			errorMessage: "missing required API parameters: job",
		},
		{
			name:         "missing_multiple",
			org:          "",
			pipeline:     "",
			build:        "123",
			job:          "abc-def",
			expectError:  true,
			errorMessage: "missing required API parameters: organization, pipeline",
		},
		{
			name:        "all_empty",
			org:         "",
			pipeline:    "",
			build:       "",
			job:         "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAPIParams(tt.org, tt.pipeline, tt.build, tt.job)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if tt.errorMessage != "" && err.Error() != tt.errorMessage {
					t.Errorf("Expected error message %q, got %q", tt.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestNewBuildkiteAPIClient(t *testing.T) {
	token := "test-token"
	version := "v1.2.3"
	client := NewBuildkiteAPIClient(token, version)

	if client.apiToken != token {
		t.Errorf("Expected API token %q, got %q", token, client.apiToken)
	}

	if client.baseURL != "https://api.buildkite.com/v2" {
		t.Errorf("Expected base URL %q, got %q", "https://api.buildkite.com/v2", client.baseURL)
	}

	expectedUserAgent := fmt.Sprintf("buildkite-logs-parquet/v1.2.3 (Go; %s; %s)", runtime.GOOS, runtime.GOARCH)
	if client.userAgent != expectedUserAgent {
		t.Errorf("Expected User-Agent %q, got %q", expectedUserAgent, client.userAgent)
	}

	if client.client == nil {
		t.Error("Expected HTTP client to be initialized")
	}
}

func TestGetJobLog_NoToken(t *testing.T) {
	client := NewBuildkiteAPIClient("", "test")

	_, err := client.GetJobLog("org", "pipeline", "build", "job")
	if err == nil {
		t.Error("Expected error when API token is empty")
	}

	expectedError := "API token is required"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestUserAgentHeaderSet(t *testing.T) {
	// Create a test server that captures the User-Agent header
	var capturedUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUserAgent = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("test log content"))
	}))
	defer server.Close()

	// Create API client with custom version
	client := NewBuildkiteAPIClient("test-token", "v1.2.3")

	// Temporarily override the base URL to point to our test server
	originalBaseURL := client.baseURL
	client.baseURL = server.URL
	defer func() { client.baseURL = originalBaseURL }()

	// Make a request (it will fail with path not found but that's ok - we just want to check headers)
	_, _ = client.GetJobLog("org", "pipeline", "build", "job")

	// Verify the User-Agent header was set correctly
	expectedUserAgent := fmt.Sprintf("buildkite-logs-parquet/v1.2.3 (Go; %s; %s)", runtime.GOOS, runtime.GOARCH)
	if capturedUserAgent != expectedUserAgent {
		t.Errorf("Expected User-Agent %q, got %q", expectedUserAgent, capturedUserAgent)
	}
}

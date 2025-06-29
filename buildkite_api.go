package buildkitelogs

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"
)

// BuildkiteAPIClient provides methods to interact with the Buildkite API
type BuildkiteAPIClient struct {
	apiToken  string
	baseURL   string
	userAgent string
	client    *http.Client
}

// NewBuildkiteAPIClient creates a new Buildkite API client
func NewBuildkiteAPIClient(apiToken, version string) *BuildkiteAPIClient {
	userAgent := fmt.Sprintf("buildkite-logs-parquet/%s (Go; %s; %s)", version, runtime.GOOS, runtime.GOARCH)

	return &BuildkiteAPIClient{
		apiToken:  apiToken,
		baseURL:   "https://api.buildkite.com/v2",
		userAgent: userAgent,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetJobLog fetches the log output for a specific job
// org: organization slug
// pipeline: pipeline slug
// build: build number or UUID
// job: job ID
func (c *BuildkiteAPIClient) GetJobLog(org, pipeline, build, job string) (io.ReadCloser, error) {
	if c.apiToken == "" {
		return nil, fmt.Errorf("API token is required")
	}

	url := fmt.Sprintf("%s/organizations/%s/pipelines/%s/builds/%s/jobs/%s/log",
		c.baseURL, org, pipeline, build, job)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Accept", "text/plain")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, resp.Status)
	}

	return resp.Body, nil
}

// ValidateAPIParams validates that all required API parameters are provided
func ValidateAPIParams(org, pipeline, build, job string) error {
	var missing []string

	if org == "" {
		missing = append(missing, "organization")
	}
	if pipeline == "" {
		missing = append(missing, "pipeline")
	}
	if build == "" {
		missing = append(missing, "build")
	}
	if job == "" {
		missing = append(missing, "job")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required API parameters: %s", strings.Join(missing, ", "))
	}

	return nil
}

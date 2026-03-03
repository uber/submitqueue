package github

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	// DefaultTimeout is the default HTTP request timeout for GitHub API calls.
	// This applies to the entire request/response cycle.
	DefaultTimeout = 30 * time.Second
)

// Config holds configuration for connecting to a GitHub backend.
type Config struct {
	// BaseURL is the GitHub instance base URL (without /graphql suffix).
	// Examples: "https://api.github.com", "https://ghe.company.com"
	BaseURL string

	// Token for authenticating to this GitHub instance.
	// Can be empty for unauthenticated requests.
	Token string

	// Timeout for HTTP requests to this GitHub instance.
	// If zero or negative, defaults to DefaultTimeout (30s).
	// Set this higher for slow GHE instances or flaky networks.
	Timeout time.Duration

	// HTTPClient provides complete control over the HTTP client.
	// If set, BaseURL is still used but Token and Timeout are ignored.
	// Use this for custom transports, connection pooling, or testing.
	HTTPClient *http.Client
}

// Validate checks if the config is valid.
func (c Config) Validate() error {
	if c.BaseURL == "" {
		return fmt.Errorf("BaseURL is required")
	}
	return nil
}

// DefaultConfig returns a Config for github.com from environment.
func DefaultConfig() Config {
	return Config{
		BaseURL: getEnvOrDefault("GITHUB_BASE_URL", "https://api.github.com"),
		Token:   os.Getenv("GITHUB_TOKEN"),
		Timeout: 0, // Will use DefaultTimeout
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

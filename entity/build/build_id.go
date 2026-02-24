package build

import "fmt"

// BuildID uniquely identifies a build across all CI providers.
// Format: "provider://provider-specific-id"
//
// The BuildID uses a URI-like format that combines the provider name and
// provider-specific identifier into a single string for simplicity in storage,
// logging, and serialization.
//
// Examples:
//   - "buildkite://uber/submitqueue-ci/123"
//   - "jenkins://456"
//   - "mock://1"
//
// For database storage, this single string format avoids the complexity of
// composite keys and makes it easy to use as a primary key or index.
type BuildID string

// String returns the BuildID as a string.
// This is the canonical representation for logging, storage, and display.
func (b BuildID) String() string {
	return string(b)
}

// NewBuildID constructs a BuildID from a provider name and provider-specific ID.
// The provider should be a short identifier (e.g., "buildkite", "jenkins", "mock").
// The id should be the provider's build identifier in whatever format they use.
func NewBuildID(provider, id string) BuildID {
	return BuildID(fmt.Sprintf("%s://%s", provider, id))
}

// ParseBuildID extracts the provider and ID components from a BuildID string.
// Returns the provider name, provider-specific ID, and an error if the format is invalid.
//
// Expected format: "provider://id"
func ParseBuildID(buildID BuildID) (provider string, id string, err error) {
	s := string(buildID)

	// Find the separator
	sep := "://"
	idx := len(s)
	for i := 0; i < len(s)-len(sep)+1; i++ {
		if s[i:i+len(sep)] == sep {
			idx = i
			break
		}
	}

	// Check if separator was found
	if idx == len(s) {
		return "", "", fmt.Errorf("invalid BuildID format: missing '://' separator in %q", s)
	}

	provider = s[:idx]
	id = s[idx+len(sep):]

	// Validate components are not empty
	if provider == "" {
		return "", "", fmt.Errorf("invalid BuildID format: empty provider in %q", s)
	}
	if id == "" {
		return "", "", fmt.Errorf("invalid BuildID format: empty ID in %q", s)
	}

	return provider, id, nil
}

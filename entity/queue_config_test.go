package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewQueueConfig(t *testing.T) {
	cfg := NewQueueConfig("uber/submitqueue/main", "uber/submitqueue", "main", []string{"github"})

	assert.Equal(t, "uber/submitqueue/main", cfg.Name)
	assert.Equal(t, "uber/submitqueue", cfg.RepositoryID)
	assert.Equal(t, "main", cfg.DestinationRef)
	assert.Equal(t, []string{"github"}, cfg.ChangeProviderNames)
}

func TestNewQueueConfig_MultipleProviders(t *testing.T) {
	cfg := NewQueueConfig(
		"uber/go-code/main",
		"uber/go-code",
		"main",
		[]string{"github", "phabricator"},
	)

	assert.Equal(t, "uber/go-code/main", cfg.Name)
	assert.Equal(t, []string{"github", "phabricator"}, cfg.ChangeProviderNames)
}


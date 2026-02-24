package queueconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewQueueConfig(t *testing.T) {
	repo := Repository{ID: "uber/submitqueue"}
	dest := Destination{Ref: "main"}

	cfg := NewQueueConfig("uber/submitqueue/main", repo, dest, "github")

	assert.Equal(t, "uber/submitqueue/main", cfg.Name)
	assert.Equal(t, "uber/submitqueue", cfg.Repository.ID)
	assert.Equal(t, "main", cfg.Destination.Ref)
	assert.Equal(t, "github", cfg.ChangeProviderName)
}

func TestNewQueueConfig_DifferentPlatforms(t *testing.T) {
	tests := []struct {
		name        string
		repo        Repository
		destination Destination
		provider    string
	}{
		{
			name:        "github",
			repo:        Repository{ID: "uber/cadence"},
			destination: Destination{Ref: "release/v2"},
			provider:    "github",
		},
		{
			name:        "gerrit",
			repo:        Repository{ID: "platform/build"},
			destination: Destination{Ref: "refs/heads/main"},
			provider:    "gerrit",
		},
		{
			name:        "perforce",
			repo:        Repository{ID: "//depot/project"},
			destination: Destination{Ref: "//depot/main/..."},
			provider:    "perforce",
		},
		{
			name:        "svn",
			repo:        Repository{ID: "https://svn.example.com/repos/project"},
			destination: Destination{Ref: "trunk/"},
			provider:    "svn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewQueueConfig(tt.name, tt.repo, tt.destination, tt.provider)

			assert.Equal(t, tt.repo, cfg.Repository)
			assert.Equal(t, tt.destination, cfg.Destination)
			assert.Equal(t, tt.provider, cfg.ChangeProviderName)
		})
	}
}

func TestQueueConfig_ZeroValue(t *testing.T) {
	var cfg QueueConfig

	assert.Empty(t, cfg.Name)
	assert.Empty(t, cfg.Repository.ID)
	assert.Empty(t, cfg.Destination.Ref)
	assert.Empty(t, cfg.ChangeProviderName)
}

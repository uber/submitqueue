package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueConfig(t *testing.T) {
	cfg := QueueConfig{
		Name:         "uber/submitqueue/main",
		VCSType:      "git",
		VCSAddress:   "git@github.com:uber/submitqueue.git",
		Target:       "main",
		BuildRunner:  "buildkite.com/uber/submitqueue-ci",
	}

	assert.Equal(t, "uber/submitqueue/main", cfg.Name)
	assert.Equal(t, "git", cfg.VCSType)
	assert.Equal(t, "git@github.com:uber/submitqueue.git", cfg.VCSAddress)
	assert.Equal(t, "main", cfg.Target)
	assert.Equal(t, "buildkite.com/uber/submitqueue-ci", cfg.BuildRunner)
}

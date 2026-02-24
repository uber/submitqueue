package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewQueueConfig(t *testing.T) {
	cfg := NewQueueConfig("uber/submitqueue/main", "git", "git@github.com:uber/submitqueue.git", "main")

	assert.Equal(t, "uber/submitqueue/main", cfg.Name)
	assert.Equal(t, "git", cfg.VCSType)
	assert.Equal(t, "git@github.com:uber/submitqueue.git", cfg.VCSAddress)
	assert.Equal(t, "main", cfg.Target)
}

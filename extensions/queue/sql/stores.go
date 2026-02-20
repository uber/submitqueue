package sql

//go:generate mockgen -source=stores.go -destination=mock_stores.go -package=sql

import (
	"context"

	"github.com/uber/submitqueue/entities/queue"
)

// MessageStore handles message table operations
type MessageStore interface {
	// Insert inserts messages into the topic table
	Insert(ctx context.Context, topic string, messages []queue.Message) error
}

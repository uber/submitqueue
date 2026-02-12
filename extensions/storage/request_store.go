package storage

import (
	"context"

	"github.com/uber/submitqueue/entities/storage"
)

// RequestStore is an interface that defines methods for managing storage requests.
type RequestStore interface {
	// Get retrieves a land request by ID
	Get(ctx context.Context, id string) (*storage.LandRequest, error)
}

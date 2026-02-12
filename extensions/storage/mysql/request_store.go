package mysql

import (
	"context"

	entities "github.com/uber/submitqueue/entities/storage"
	"github.com/uber/submitqueue/extensions/storage"
)

type requestStore struct {
}

type RequestParam struct {
}

func NewRequestStore() storage.RequestStore {
	return &requestStore{}
}

// Get retrieves a land request by ID
func (r *requestStore) Get(ctx context.Context, id string) (*entities.LandRequest, error) {
	// TODO: implement GET operation
	panic("not implemented")
}

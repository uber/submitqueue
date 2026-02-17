package mysql

import (
	"context"
	"errors"

	"github.com/uber/submitqueue/entities"
	"github.com/uber/submitqueue/extensions/storage"
)

type requestStore struct {
}

// NewRequestStore creates a new MySQL-backed RequestStore.
func NewRequestStore() storage.RequestStore {
	return &requestStore{}
}

// Get retrieves a land request by ID. Returns ErrNotFound if the request is not found.
func (r *requestStore) Get(ctx context.Context, id string) (entities.Request, error) {
	// TODO: implement GET operation
	return entities.Request{}, errors.New("not implemented")
}

// Create creates a new land request. Returns the created request object with generated sequence number.
func (r *requestStore) Create(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
	// TODO: implement CREATE operation
	return entities.Request{}, errors.New("not implemented")
}

// UpdateState updates the state of a land request if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
// The implementation should increment the version by 1 atomically with the state update.
func (r *requestStore) UpdateState(ctx context.Context, id string, version int32, newState entities.RequestState) error {
	// TODO: implement UPDATE STATE operation
	return errors.New("not implemented")
}

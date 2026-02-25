package storage

//go:generate mockgen -source=build_store.go -destination=mock/build_store.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BuildStore is an interface that defines methods for managing builds in the database.
type BuildStore interface {
	// Get retrieves a build by ID. Returns ErrNotFound if the build is not found.
	Get(ctx context.Context, id string) (entity.Build, error)

	// Create creates a new build. The build must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a build with the same ID already exists.
	Create(ctx context.Context, build entity.Build) error

	// UpdateStatus updates the status of a build.
	UpdateStatus(ctx context.Context, id string, newStatus entity.BuildStatus) error
}

package counter

//go:generate mockgen -source=counter.go -destination=mock/counter.go -package=mock

import "context"

// Counter provides atomic sequential number generation for a given domain.
// Each call to Next returns the next value in the sequence for the specified domain.
// The value is guaranteed to be unique within the domain throughout the system and persisted accordingly.
type Counter interface {
	// Next atomically increments the counter for the given domain and returns the new value.
	// The first call for a new domain returns 1.
	// The implementation should support at least 255 length domains.
	// The function is safe to be called concurrently and will give unique results, but the order of the values is not guaranteed.
	Next(ctx context.Context, domain string) (int64, error)
}

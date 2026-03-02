package entity

// LandEntry pairs a land strategy with the change to land.
// Each entry represents one request's contribution to a batch land operation.
type LandEntry struct {
	// Strategy is the source control integration method for this change.
	Strategy RequestLandStrategy
	// Change is the code change to land.
	Change Change
}

package storage

// Factory is an interface that defines methods for creating different stores.
type Factory interface {
	RequestStore() RequestStore
}

package mysql

import "github.com/uber/submitqueue/extensions/storage"

// MySQLParameters defines the parameters for the MySQL storage factory.
type MySQLParameters struct {
}

// NewFactory creates a new MySQL storage factory
func NewFactory(p MySQLParameters) (storage.StoreFactory, error) {
	return &factory{
		requestStore: NewRequestStore(),
	}, nil
}

type factory struct {
	requestStore storage.RequestStore
}

// GetRequestStore returns the MySQL-backed RequestStore
func (f *factory) GetRequestStore() storage.RequestStore {
	return f.requestStore
}

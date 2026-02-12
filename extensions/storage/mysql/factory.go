package mysql

import "github.com/uber/submitqueue/extensions/storage"

type factory struct {
	requestStore storage.RequestStore
}

type FactoryParams struct {
	requestStore storage.RequestStore
}

// NewFactory creates a new MySQL storage factory
func NewFactory(p FactoryParams) (storage.Factory, error) {
	return &factory{
		requestStore: p.requestStore,
	}, nil
}

// RequestStore returns the MySQL-backed RequestStore
func (f *factory) RequestStore() storage.RequestStore {
	return f.requestStore
}

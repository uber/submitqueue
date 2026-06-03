// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubStorage is a minimal Storage used to verify the factory returns the
// wrapped instance. All accessors return zero values; they are never invoked.
type stubStorage struct{}

func (stubStorage) GetRequestStore() RequestStore                 { return nil }
func (stubStorage) GetChangeProviderStore() ChangeProviderStore   { return nil }
func (stubStorage) GetBatchStore() BatchStore                     { return nil }
func (stubStorage) GetBatchDependentStore() BatchDependentStore   { return nil }
func (stubStorage) GetBuildStore() BuildStore                     { return nil }
func (stubStorage) GetSpeculationTreeStore() SpeculationTreeStore { return nil }
func (stubStorage) GetRequestLogStore() RequestLogStore           { return nil }
func (stubStorage) Close() error                                  { return nil }

func TestStaticFactory_For(t *testing.T) {
	s := stubStorage{}
	f := NewStaticFactory(s)

	t.Run("returns the wrapped storage for any name", func(t *testing.T) {
		for _, name := range []string{"", "queue-a", "queue-b"} {
			got, err := f.For(name)
			require.NoError(t, err)
			assert.Equal(t, s, got)
		}
	})
}

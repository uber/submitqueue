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

package lifecycle

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spy records the order of Start/Stop calls and can be configured to fail.
type spy struct {
	name     string
	startErr error
	stopErr  error
	log      *[]string
}

func (s *spy) Start(_ context.Context) error {
	*s.log = append(*s.log, "start:"+s.name)
	return s.startErr
}

func (s *spy) Stop(_ context.Context) error {
	*s.log = append(*s.log, "stop:"+s.name)
	return s.stopErr
}

func TestGroup_StartStop_HappyPath(t *testing.T) {
	var log []string
	a := &spy{name: "a", log: &log}
	b := &spy{name: "b", log: &log}
	c := &spy{name: "c", log: &log}

	g := NewGroup(a, b, c)

	require.NoError(t, g.Start(context.Background()))
	assert.Equal(t, []string{"start:a", "start:b", "start:c"}, log)

	log = nil
	require.NoError(t, g.Stop(context.Background()))
	assert.Equal(t, []string{"stop:c", "stop:b", "stop:a"}, log)
}

func TestGroup_StartRollback_OnFailure(t *testing.T) {
	var log []string
	a := &spy{name: "a", log: &log}
	b := &spy{name: "b", startErr: fmt.Errorf("b broke"), log: &log}
	c := &spy{name: "c", log: &log}

	g := NewGroup(a, b, c)

	err := g.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "b broke")

	// a was started and then rolled back; b failed; c was never started
	assert.Equal(t, []string{"start:a", "start:b", "stop:a"}, log)
}

func TestGroup_StartRollback_FirstMemberFails(t *testing.T) {
	var log []string
	a := &spy{name: "a", startErr: fmt.Errorf("a broke"), log: &log}
	b := &spy{name: "b", log: &log}

	g := NewGroup(a, b)

	err := g.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a broke")

	// Nothing to roll back — a failed on start, b never started
	assert.Equal(t, []string{"start:a"}, log)
}

func TestGroup_StartRollback_JoinsStopErrors(t *testing.T) {
	var log []string
	a := &spy{name: "a", stopErr: fmt.Errorf("a stop failed"), log: &log}
	b := &spy{name: "b", log: &log}
	c := &spy{name: "c", startErr: fmt.Errorf("c broke"), log: &log}

	g := NewGroup(a, b, c)

	err := g.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "c broke")
	assert.Contains(t, err.Error(), "a stop failed")

	// a and b started, c failed, then b and a rolled back in reverse
	assert.Equal(t, []string{"start:a", "start:b", "start:c", "stop:b", "stop:a"}, log)
}

func TestGroup_Stop_CollectsAllErrors(t *testing.T) {
	var log []string
	a := &spy{name: "a", stopErr: fmt.Errorf("a stop failed"), log: &log}
	b := &spy{name: "b", stopErr: fmt.Errorf("b stop failed"), log: &log}
	c := &spy{name: "c", log: &log}

	g := NewGroup(a, b, c)
	require.NoError(t, g.Start(context.Background()))

	log = nil
	err := g.Stop(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a stop failed")
	assert.Contains(t, err.Error(), "b stop failed")

	// All three stopped in reverse despite errors
	assert.Equal(t, []string{"stop:c", "stop:b", "stop:a"}, log)
}

func TestGroup_NilMembers_Skipped(t *testing.T) {
	var log []string
	a := &spy{name: "a", log: &log}

	g := NewGroup(nil, a, nil)

	require.NoError(t, g.Start(context.Background()))
	assert.Equal(t, []string{"start:a"}, log)

	log = nil
	require.NoError(t, g.Stop(context.Background()))
	assert.Equal(t, []string{"stop:a"}, log)
}

func TestGroup_Empty(t *testing.T) {
	g := NewGroup()
	require.NoError(t, g.Start(context.Background()))
	require.NoError(t, g.Stop(context.Background()))
}

func TestGroup_Nested(t *testing.T) {
	var log []string
	a := &spy{name: "a", log: &log}
	b := &spy{name: "b", log: &log}
	c := &spy{name: "c", log: &log}
	d := &spy{name: "d", log: &log}

	inner := NewGroup(b, c)
	outer := NewGroup(a, inner, d)

	require.NoError(t, outer.Start(context.Background()))
	assert.Equal(t, []string{"start:a", "start:b", "start:c", "start:d"}, log)

	log = nil
	require.NoError(t, outer.Stop(context.Background()))
	assert.Equal(t, []string{"stop:d", "stop:c", "stop:b", "stop:a"}, log)
}

func TestGroup_Nested_RollbackOnInnerFailure(t *testing.T) {
	var log []string
	a := &spy{name: "a", log: &log}
	b := &spy{name: "b", log: &log}
	c := &spy{name: "c", startErr: fmt.Errorf("c broke"), log: &log}
	d := &spy{name: "d", log: &log}

	inner := NewGroup(b, c)
	outer := NewGroup(a, inner, d)

	err := outer.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "c broke")

	// a started, inner started b then c failed, inner rolled back b,
	// then outer rolled back a. d never started.
	assert.Equal(t, []string{"start:a", "start:b", "start:c", "stop:b", "stop:a"}, log)
}

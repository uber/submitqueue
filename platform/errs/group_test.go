// Copyright (c) 2026 Uber Technologies, Inc.
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

package errs

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroup_NoFailures(t *testing.T) {
	tests := map[string][]error{
		"no members": nil,
		"single nil": {nil},
		"all nil":    {nil, nil},
	}

	for name, members := range tests {
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, Group(members...))
		})
	}
}

func TestGroup_DropsNilMembers(t *testing.T) {
	a := errors.New("a")
	b := errors.New("b")

	out := Group(nil, a, nil, b, nil)

	require.Error(t, out)
	assert.Equal(t, "a; b", out.Error())
}

// Every member must stay reachable, because a member the caller cannot find is
// also a member the classifier cannot weigh.
func TestGroup_ReachesEveryMember(t *testing.T) {
	sentinel := errors.New("sentinel")
	other := errors.New("other")

	out := Group(fmt.Errorf("child a: %w", sentinel), other)

	assert.True(t, errors.Is(out, sentinel))
	assert.True(t, errors.Is(out, other))
	assert.False(t, errors.Is(out, errors.New("never grouped")))
}

func TestGroup_ErrorsAsFindsAMemberType(t *testing.T) {
	out := Group(errors.New("plain"), NewUserError(errors.New("bad input")))

	var ue *userError
	require.True(t, errors.As(out, &ue))
	assert.True(t, IsUserError(out))
}

// One log record per failed delivery, not one per member.
func TestGroup_MessageIsSingleLine(t *testing.T) {
	out := Group(errors.New("first"), errors.New("second"), errors.New("third"))

	assert.NotContains(t, out.Error(), "\n")
	for _, want := range []string{"first", "second", "third"} {
		assert.Contains(t, out.Error(), want)
	}
}

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

package client

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyReader(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []action
	}{
		{name: "letters", input: "jkgGq", want: []action{
			actionDown, actionUp, actionTop, actionBottom, actionQuit,
		}},
		{name: "space pages down", input: " ", want: []action{actionPageDown}},
		{name: "ctrl-c quits, since raw mode delivers it as a byte", input: "\x03", want: []action{actionQuit}},
		{name: "arrows", input: "\x1b[A\x1b[B", want: []action{actionUp, actionDown}},
		{name: "page keys", input: "\x1b[5~\x1b[6~", want: []action{actionPageUp, actionPageDown}},
		{name: "home and end", input: "\x1b[H\x1b[F", want: []action{actionTop, actionBottom}},
		{
			// An unknown key must do nothing rather than close the view.
			name:  "unknown keys are ignored",
			input: "xyz\x1b[Zj",
			want:  []action{actionDown},
		},
		{
			// Escape is bound to nothing, so it and whatever it is mistaken for
			// the start of are both ignored.
			name:  "an escape that begins no sequence is ignored",
			input: "\x1bxj",
			want:  []action{actionDown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := newKeyReader(strings.NewReader(tt.input))

			var got []action
			for {
				a, err := keys.next()
				if err != nil {
					break
				}
				if a != actionNone {
					got = append(got, a)
				}
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// An arrow is three bytes and a terminal may deliver them across separate
// reads, which is normal rather than an error.
func TestKeyReaderHandlesASplitSequence(t *testing.T) {
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })
	keys := newKeyReader(pr)

	done := make(chan action, 1)
	go func() {
		a, err := keys.next()
		require.NoError(t, err)
		done <- a
	}()

	_, _ = pw.Write([]byte{0x1b})
	_, _ = pw.Write([]byte{'['})
	_, _ = pw.Write([]byte{'A'})

	assert.Equal(t, actionUp, <-done)
}

func TestScrollApply(t *testing.T) {
	const (
		total    = 40
		capacity = 10
	)

	tests := []struct {
		name       string
		start      scroll
		action     action
		wantOffset int
		wantFollow bool
	}{
		{name: "down one", start: scroll{offset: 0}, action: actionDown, wantOffset: 1},
		{name: "up one", start: scroll{offset: 5}, action: actionUp, wantOffset: 4},
		{name: "page down", start: scroll{offset: 0}, action: actionPageDown, wantOffset: 9},
		{name: "page up", start: scroll{offset: 20}, action: actionPageUp, wantOffset: 11},
		{name: "top", start: scroll{offset: 20}, action: actionTop, wantOffset: 0},
		{
			name: "bottom follows from then on", start: scroll{offset: 0}, action: actionBottom,
			wantOffset: total - capacity, wantFollow: true,
		},
		{
			name: "cannot scroll above the first row", start: scroll{offset: 0}, action: actionPageUp,
			wantOffset: 0,
		},
		{
			// Scrolling past the end lands on it, and being on the end is what
			// following means — however the view got there.
			name: "cannot scroll past the last row", start: scroll{offset: 38}, action: actionPageDown,
			wantOffset: total - capacity, wantFollow: true,
		},
		{
			name: "scrolling up releases follow", start: scroll{offset: total - capacity, follow: true}, action: actionUp,
			wantOffset: total - capacity - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.start
			s.apply(tt.action, total, capacity)
			assert.Equal(t, tt.wantOffset, s.offset)
			assert.Equal(t, tt.wantFollow, s.follow)
		})
	}
}

// Following is what makes a watch usable without touching it: rows arriving
// must not push the end out from under a reader who is already at it.
func TestScrollFollowsRowsAsTheyArrive(t *testing.T) {
	s := scroll{}
	s.apply(actionBottom, 10, 5)
	require.True(t, s.follow)
	require.Equal(t, 5, s.offset)

	s.clamp(30, 5)
	assert.Equal(t, 25, s.offset, "a following view stays at the end as the table grows")

	s.apply(actionUp, 30, 5)
	assert.False(t, s.follow, "scrolling away holds the reader's place")
	s.clamp(60, 5)
	assert.Equal(t, 24, s.offset, "and rows arriving no longer move it")
}

func TestScrollWindow(t *testing.T) {
	rows := make([]*Row, 10)
	for i := range rows {
		rows[i] = &Row{SQID: string(rune('a' + i))}
	}

	t.Run("everything fits", func(t *testing.T) {
		s := scroll{}
		assert.Len(t, s.window(rows, 20), 10)
	})

	t.Run("a window in the middle", func(t *testing.T) {
		s := scroll{offset: 3}
		window := s.window(rows, 4)
		require.Len(t, window, 4)
		assert.Equal(t, "d", window[0].SQID)
		assert.Equal(t, "g", window[3].SQID)
	})

	t.Run("a window at the end is not short", func(t *testing.T) {
		s := scroll{offset: 6}
		assert.Len(t, s.window(rows, 4), 4)
	})
}

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
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

// action is what a keypress asks the view to do.
type action int

const (
	actionNone action = iota
	actionUp
	actionDown
	actionPageUp
	actionPageDown
	actionTop
	actionBottom
	actionQuit
)

// keyReader turns a byte stream into actions.
//
// Terminals send arrows and page keys as escape sequences rather than
// characters, and a sequence arrives one byte at a time, so this reads from a
// buffered stream rather than from fixed-size chunks: a three-byte arrow split
// across two reads is normal, not an error.
type keyReader struct {
	in *bufio.Reader
}

func newKeyReader(r io.Reader) *keyReader {
	return &keyReader{in: bufio.NewReader(r)}
}

// next blocks until it can report one action. An unrecognized key is
// actionNone, which the caller ignores — an unknown key should do nothing
// rather than close the view.
func (k *keyReader) next() (action, error) {
	b, err := k.in.ReadByte()
	if err != nil {
		return actionNone, err
	}

	switch b {
	case 'q', 'Q', 0x03: // 0x03 is Ctrl-C, which raw mode delivers as a byte.
		return actionQuit, nil
	case 'j':
		return actionDown, nil
	case 'k':
		return actionUp, nil
	case 'g':
		return actionTop, nil
	case 'G':
		return actionBottom, nil
	case ' ':
		return actionPageDown, nil
	case 0x1b: // ESC: the start of an arrow or page key, or a bare Escape.
		return k.escape()
	}
	return actionNone, nil
}

// escape reads the remainder of a CSI sequence.
//
// It waits for the next byte rather than checking whether one has already
// arrived. A terminal may deliver an arrow's three bytes across separate reads,
// and treating "nothing buffered yet" as "this was a bare Escape" would turn
// the arrow into a keypress that does nothing — intermittently, under load,
// which is the worst way for it to be wrong.
//
// The cost is that a bare Escape is not seen until another key follows it, and
// swallows that key. Nothing here binds Escape, so the exchange is a good one.
func (k *keyReader) escape() (action, error) {
	b, err := k.in.ReadByte()
	if err != nil || b != '[' {
		return actionNone, err
	}

	b, err = k.in.ReadByte()
	if err != nil {
		return actionNone, err
	}
	switch b {
	case 'A':
		return actionUp, nil
	case 'B':
		return actionDown, nil
	case 'H':
		return actionTop, nil
	case 'F':
		return actionBottom, nil
	case '5', '6': // Page Up and Page Down arrive as "5~" and "6~".
		if tilde, err := k.in.ReadByte(); err != nil || tilde != '~' {
			return actionNone, err
		}
		if b == '5' {
			return actionPageUp, nil
		}
		return actionPageDown, nil
	}
	return actionNone, nil
}

// scroll is which slice of the rows the view is showing.
//
// follow is what makes a watch usable without touching it: while it holds, the
// view stays pinned to the end as rows are added, the way a log tail does.
// Scrolling up drops it, so the reader keeps the place they chose; scrolling
// back to the end restores it, so they can hand control back without a
// dedicated key.
type scroll struct {
	offset int
	follow bool
}

// apply moves the view, given how many rows there are and how many fit.
func (s *scroll) apply(a action, total, capacity int) {
	page := capacity - 1
	if page < 1 {
		page = 1
	}

	switch a {
	case actionUp:
		s.offset--
	case actionDown:
		s.offset++
	case actionPageUp:
		s.offset -= page
	case actionPageDown:
		s.offset += page
	case actionTop:
		s.offset = 0
	case actionBottom:
		s.offset = total
	default:
		return
	}
	// A move is the reader's, so it decides where the view sits — including
	// whether it is still following. Snapping to the end here instead would
	// make scrolling up impossible from a following view.
	s.bound(total, capacity)
}

// bound keeps the window inside the rows and derives whether the view is
// following from where it ended up: being at the end is what following is,
// however the view got there.
func (s *scroll) bound(total, capacity int) {
	max := maxOffset(total, capacity)
	if s.offset > max {
		s.offset = max
	}
	if s.offset < 0 {
		s.offset = 0
	}
	s.follow = s.offset == max
}

// clamp prepares the window for a draw: a following view is carried to the new
// end, so rows arriving do not push it out from under a reader who is watching
// the newest ones, while a reader who scrolled away keeps their place.
func (s *scroll) clamp(total, capacity int) {
	if s.follow {
		s.offset = maxOffset(total, capacity)
	}
	s.bound(total, capacity)
}

// maxOffset is the furthest the window can start and still be full.
func maxOffset(total, capacity int) int {
	if max := total - capacity; max > 0 {
		return max
	}
	return 0
}

// window is the slice of rows the view shows.
func (s *scroll) window(rows []*Row, capacity int) []*Row {
	if capacity >= len(rows) {
		return rows
	}
	end := s.offset + capacity
	if end > len(rows) {
		end = len(rows)
	}
	return rows[s.offset:end]
}

// terminal owns the screen while a watch is running: the alternate buffer it
// draws into, and the raw mode that delivers keystrokes as they are typed
// rather than a line at a time.
//
// Both are global state on the user's terminal, so restore must run on every
// path out — a normal finish, a signal, or a panic. Leaving a terminal in raw
// mode without an echo is the kind of breakage that outlives the process.
type terminal struct {
	fd    int
	state *term.State
	out   io.Writer
}

// enterTerminal takes over the screen, reporting whether it could.
//
// It needs both halves: a screen to draw on and a keyboard to read. A run whose
// input is redirected keeps the plain redraw, because a full-screen view nobody
// can scroll is worse than a table that scrolls with the terminal.
func enterTerminal(out io.Writer) (*terminal, bool) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		return nil, false
	}
	state, err := term.MakeRaw(fd)
	if err != nil {
		return nil, false
	}

	t := &terminal{fd: fd, state: state, out: out}
	// Alternate buffer, cursor hidden: the scrollback the reader had before this
	// started is theirs and comes back untouched on exit.
	fmt.Fprint(out, "\033[?1049h\033[?25l")
	return t, true
}

// restore gives the terminal back. It is safe to call more than once, because
// the paths that have to call it — deferred cleanup and a signal handler — can
// both run.
func (t *terminal) restore() {
	if t == nil || t.state == nil {
		return
	}
	fmt.Fprint(t.out, "\033[?25h\033[?1049l")
	_ = term.Restore(t.fd, t.state)
	t.state = nil
}

// readKeys reports keystrokes until the context ends or the stream does.
//
// The read itself cannot be cancelled — a blocking read on a terminal stays
// blocked until a key arrives — so this outlives a cancelled context by at most
// one keypress and drops what it reads after. That is why it sends on a
// buffered channel and never blocks on the send: the receiver may already be
// gone.
func readKeys(ctx context.Context, r io.Reader, actions chan<- action) {
	keys := newKeyReader(r)
	for {
		a, err := keys.next()
		if err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if a == actionNone {
			continue
		}
		select {
		case actions <- a:
		default:
		}
	}
}

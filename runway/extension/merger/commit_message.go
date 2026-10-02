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

package merger

//go:generate mockgen -source=commit_message.go -destination=mock/commit_message_mock.go -package=mock

import "context"

// CommitMessage is the message and authorship recorded on a squash or merge
// commit.
type CommitMessage struct {
	// Message is the full commit message.
	Message string
	// AuthorName is the display name of the commit author.
	AuthorName string
	// AuthorEmail is the email address of the commit author.
	AuthorEmail string
}

// CommitMessageResolver resolves the commit message and authorship for the
// change named by a change URI. Implementations must be safe for concurrent
// use.
type CommitMessageResolver interface {
	// Resolve returns the commit message for the change identified by uri.
	Resolve(ctx context.Context, uri string) (CommitMessage, error)
}

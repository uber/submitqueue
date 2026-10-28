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

package controller

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"

	"github.com/uber/submitqueue/submitqueue/entity"
	basestorage "github.com/uber/submitqueue/submitqueue/extension/storage"
)

const listPageTokenVersion = 1

type listPageToken struct {
	Version             int    `json:"version"`
	Queue               string `json:"queue"`
	ReceivedAtOrAfterMs int64  `json:"received_at_or_after_ms"`
	ReceivedBeforeMs    int64  `json:"received_before_ms"`
	LastReceivedAtMs    int64  `json:"last_received_at_ms"`
	LastRequestID       string `json:"last_request_id"`
	// Statuses is the canonical status filter; absent in tokens minted without one.
	Statuses []string `json:"statuses,omitempty"`
}

// resolveListRange takes the window and status filter from the page token when one is supplied, so a continuation
// needs only the queue and the token; a bound or filter the caller also supplies must match the token's.
func resolveListRange(req entity.ListRequest) (basestorage.RequestReceiptRange, []entity.RequestStatus, error) {
	statuses, ok := canonicalListStatuses(req.Statuses)
	if !ok {
		return basestorage.RequestReceiptRange{}, nil, fmt.Errorf("statuses must be non-empty and at most %d: %w", maxListStatuses, ErrInvalidRequest)
	}
	var query basestorage.RequestReceiptRange
	if req.PageToken != "" {
		token, err := decodeListPageToken(req.PageToken)
		if err != nil {
			return basestorage.RequestReceiptRange{}, nil, fmt.Errorf("invalid page token: %w", ErrInvalidRequest)
		}
		tokenStatuses, ok := canonicalListStatuses(statusesFromStrings(token.Statuses))
		if !ok || !slices.Equal(statusStrings(tokenStatuses), token.Statuses) {
			return basestorage.RequestReceiptRange{}, nil, fmt.Errorf("invalid page token: %w", ErrInvalidRequest)
		}
		if token.Queue != req.Queue ||
			(req.HasReceivedAtOrAfterMs && req.ReceivedAtOrAfterMs != token.ReceivedAtOrAfterMs) ||
			(req.HasReceivedBeforeMs && req.ReceivedBeforeMs != token.ReceivedBeforeMs) ||
			(len(statuses) > 0 && !slices.Equal(statuses, tokenStatuses)) {
			return basestorage.RequestReceiptRange{}, nil, fmt.Errorf("page token does not match the queue, time bounds, and statuses: %w", ErrInvalidRequest)
		}
		query.ReceivedAtOrAfterMs = token.ReceivedAtOrAfterMs
		query.ReceivedBeforeMs = token.ReceivedBeforeMs
		query.Before = basestorage.RequestReceiptCursor{ReceivedAtMs: token.LastReceivedAtMs, RequestID: token.LastRequestID}
		return query, tokenStatuses, nil
	}
	if !req.HasReceivedAtOrAfterMs || !req.HasReceivedBeforeMs || req.ReceivedAtOrAfterMs >= req.ReceivedBeforeMs {
		return basestorage.RequestReceiptRange{}, nil, fmt.Errorf("first page requires received_at_or_after_ms < received_before_ms: %w", ErrInvalidRequest)
	}
	query.ReceivedAtOrAfterMs = req.ReceivedAtOrAfterMs
	query.ReceivedBeforeMs = req.ReceivedBeforeMs
	return query, statuses, nil
}

// canonicalListStatuses sorts and deduplicates a filter so equal filters compare equal; ok is false for an empty
// status or a filter above maxListStatuses.
func canonicalListStatuses(statuses []entity.RequestStatus) ([]entity.RequestStatus, bool) {
	if len(statuses) > maxListStatuses || slices.Contains(statuses, entity.RequestStatusUnknown) {
		return nil, false
	}
	canonical := slices.Clone(statuses)
	slices.Sort(canonical)
	return slices.Compact(canonical), true
}

func statusStrings(statuses []entity.RequestStatus) []string {
	if len(statuses) == 0 {
		return nil
	}
	values := make([]string, len(statuses))
	for i, status := range statuses {
		values[i] = string(status)
	}
	return values
}

func statusesFromStrings(values []string) []entity.RequestStatus {
	statuses := make([]entity.RequestStatus, len(values))
	for i, value := range values {
		statuses[i] = entity.RequestStatus(value)
	}
	return statuses
}

func encodeListPageToken(token listPageToken) (string, error) {
	contents, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(contents), nil
}

func decodeListPageToken(encoded string) (listPageToken, error) {
	contents, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return listPageToken{}, err
	}
	token, err := parseListPageTokenContents(contents)
	if err != nil {
		return listPageToken{}, err
	}
	if token.Queue == "" || token.LastRequestID == "" || token.LastReceivedAtMs <= 0 ||
		token.ReceivedAtOrAfterMs >= token.ReceivedBeforeMs ||
		token.LastReceivedAtMs < token.ReceivedAtOrAfterMs || token.LastReceivedAtMs >= token.ReceivedBeforeMs {
		return listPageToken{}, fmt.Errorf("invalid token fields")
	}
	return token, nil
}

func parseListPageTokenContents(contents []byte) (listPageToken, error) {
	// TODO(list-token): drop the unversioned query-string format once tokens minted before versioning have expired.
	if !bytes.HasPrefix(contents, []byte("{")) {
		return parseUnversionedListPageToken(contents)
	}
	var token listPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return listPageToken{}, err
	}
	if token.Version != listPageTokenVersion {
		return listPageToken{}, fmt.Errorf("unsupported token version %d", token.Version)
	}
	return token, nil
}

func parseUnversionedListPageToken(contents []byte) (listPageToken, error) {
	values, err := url.ParseQuery(string(contents))
	if err != nil {
		return listPageToken{}, err
	}
	receivedAtOrAfterMs, err := strconv.ParseInt(values.Get("received_at_or_after_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	receivedBeforeMs, err := strconv.ParseInt(values.Get("received_before_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	lastReceivedAtMs, err := strconv.ParseInt(values.Get("last_received_at_ms"), 10, 64)
	if err != nil {
		return listPageToken{}, err
	}
	return listPageToken{
		Queue:               values.Get("queue"),
		ReceivedAtOrAfterMs: receivedAtOrAfterMs,
		ReceivedBeforeMs:    receivedBeforeMs,
		LastReceivedAtMs:    lastReceivedAtMs,
		LastRequestID:       values.Get("last_request_id"),
	}, nil
}

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
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listPageTokenVersion = 1

type listPageToken struct {
	Version             int    `json:"version"`
	Queue               string `json:"queue"`
	AcceptedAtOrAfterMs int64  `json:"accepted_at_or_after_ms"`
	AcceptedBeforeMs    int64  `json:"accepted_before_ms"`
	LastAcceptedAtMs    int64  `json:"last_accepted_at_ms"`
	LastRequestID       string `json:"last_request_id"`
}

func (c *listController) resolveListRange(req entity.ListRequest) (storage.RequestAcceptanceRange, error) {
	if req.PageSize < 0 || req.PageSize > maxListPageSize {
		return storage.RequestAcceptanceRange{}, fmt.Errorf("List page_size must be between 0 and %d: %w", maxListPageSize, ErrInvalidRequest)
	}
	pageSize := int(req.PageSize)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	query := storage.RequestAcceptanceRange{Limit: pageSize + 1}
	if req.PageToken != "" {
		token, err := decodeListPageToken(req.PageToken)
		if err != nil {
			return storage.RequestAcceptanceRange{}, err
		}
		if token.Queue != req.Queue ||
			(req.HasAcceptedAtOrAfterMs && req.AcceptedAtOrAfterMs != token.AcceptedAtOrAfterMs) ||
			(req.HasAcceptedBeforeMs && req.AcceptedBeforeMs != token.AcceptedBeforeMs) {
			return storage.RequestAcceptanceRange{}, fmt.Errorf("List page_token does not match the queue and time bounds: %w", ErrInvalidRequest)
		}
		query.AcceptedAtOrAfterMs = token.AcceptedAtOrAfterMs
		query.AcceptedBeforeMs = token.AcceptedBeforeMs
		query.Before = storage.RequestAcceptanceCursor{AcceptedAtMs: token.LastAcceptedAtMs, RequestID: token.LastRequestID}
		return query, nil
	}
	if req.HasAcceptedAtOrAfterMs {
		query.AcceptedAtOrAfterMs = req.AcceptedAtOrAfterMs
	}
	if req.HasAcceptedBeforeMs {
		query.AcceptedBeforeMs = req.AcceptedBeforeMs
	} else {
		query.AcceptedBeforeMs = c.now().UnixMilli()
	}
	if query.AcceptedAtOrAfterMs < 0 || query.AcceptedBeforeMs <= query.AcceptedAtOrAfterMs {
		return storage.RequestAcceptanceRange{}, fmt.Errorf("List requires 0 <= accepted_at_or_after_ms < accepted_before_ms: %w", ErrInvalidRequest)
	}
	return query, nil
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
		return listPageToken{}, fmt.Errorf("List invalid page_token encoding: %w", ErrInvalidRequest)
	}
	var token listPageToken
	if err := json.Unmarshal(contents, &token); err != nil {
		return listPageToken{}, fmt.Errorf("List invalid page_token contents: %w", ErrInvalidRequest)
	}
	if token.Version != listPageTokenVersion || token.Queue == "" || token.LastRequestID == "" ||
		token.AcceptedAtOrAfterMs < 0 || token.AcceptedBeforeMs <= token.AcceptedAtOrAfterMs ||
		token.LastAcceptedAtMs <= 0 || token.LastAcceptedAtMs < token.AcceptedAtOrAfterMs || token.LastAcceptedAtMs >= token.AcceptedBeforeMs {
		return listPageToken{}, fmt.Errorf("List invalid page_token fields: %w", ErrInvalidRequest)
	}
	return token, nil
}

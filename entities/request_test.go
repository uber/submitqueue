package entities

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest_GetID(t *testing.T) {
	tests := []struct {
		name     string
		request  Request
		expected string
	}{
		{
			name:     "standard ID",
			request:  Request{Queue: "my-queue", Seq: 42},
			expected: "my-queue/42",
		},
		{
			name:     "seq 1",
			request:  Request{Queue: "q", Seq: 1},
			expected: "q/1",
		},
		{
			name:     "large seq",
			request:  Request{Queue: "prod", Seq: 9999999},
			expected: "prod/9999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.request.GetID())
		})
	}
}

func TestParseRequestID(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		wantQueue   string
		wantSeq     int64
		expectError bool
	}{
		{
			name:      "valid ID",
			id:        "my-queue/42",
			wantQueue: "my-queue",
			wantSeq:   42,
		},
		{
			name:      "seq 1",
			id:        "q/1",
			wantQueue: "q",
			wantSeq:   1,
		},
		{
			name:        "missing separator",
			id:          "no-separator",
			expectError: true,
		},
		{
			name:        "too many separators",
			id:          "a/b/c",
			expectError: true,
		},
		{
			name:        "empty string",
			id:          "",
			expectError: true,
		},
		{
			name:        "non-numeric seq",
			id:          "queue/abc",
			expectError: true,
		},
		{
			name:        "zero seq",
			id:          "queue/0",
			expectError: true,
		},
		{
			name:        "negative seq",
			id:          "queue/-1",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue, seq, err := ParseRequestID(tt.id)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantQueue, queue)
			assert.Equal(t, tt.wantSeq, seq)
		})
	}
}

func TestGetID_ParseRequestID_Roundtrip(t *testing.T) {
	req := &Request{Queue: "test-queue", Seq: 123}
	queue, seq, err := ParseRequestID(req.GetID())
	require.NoError(t, err)
	assert.Equal(t, req.Queue, queue)
	assert.Equal(t, req.Seq, seq)
}

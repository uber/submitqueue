package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequest_ToBytes(t *testing.T) {
	req := Request{
		ID:           "test-queue/123",
		Queue:        "test-queue",
		Change:       Change{Provider: "github", URIs: []string{"github.com/uber/submitqueue/456/abc123def", "github.com/uber/submitqueue/789/def456abc"}},
		LandStrategy: RequestLandStrategyRebase,
		State:        RequestStateNew,
		Version:      1,
	}

	data, err := req.ToBytes()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify JSON contains expected fields
	jsonStr := string(data)
	assert.Contains(t, jsonStr, "test-queue/123")
	assert.Contains(t, jsonStr, "github")
	assert.Contains(t, jsonStr, "github.com/uber/submitqueue/456/abc123def")
	assert.Contains(t, jsonStr, "rebase")
	assert.Contains(t, jsonStr, "new")
}

func TestRequestFromBytes(t *testing.T) {
	original := Request{
		ID:           "my-queue/999",
		Queue:        "my-queue",
		Change:       Change{Provider: "phabricator", URIs: []string{"phabricator.uber.com/D111/fedcba987"}},
		LandStrategy: RequestLandStrategyMerge,
		State:        RequestStateProcessing,
		Version:      3,
	}

	// Serialize
	data, err := original.ToBytes()
	require.NoError(t, err)

	// Deserialize
	deserialized, err := RequestFromBytes(data)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.ID, deserialized.ID)
	assert.Equal(t, original.Queue, deserialized.Queue)
	assert.Equal(t, original.Change.Provider, deserialized.Change.Provider)
	assert.Equal(t, original.Change.URIs, deserialized.Change.URIs)
	assert.Equal(t, original.LandStrategy, deserialized.LandStrategy)
	assert.Equal(t, original.State, deserialized.State)
	assert.Equal(t, original.Version, deserialized.Version)
}

func TestRequestFromBytes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": json"}`)

	_, err := RequestFromBytes(invalidJSON)
	assert.Error(t, err)
}

func TestRequestFromBytes_EmptyData(t *testing.T) {
	emptyJSON := []byte(`{}`)

	req, err := RequestFromBytes(emptyJSON)
	require.NoError(t, err)

	// Empty JSON should deserialize with zero values
	assert.Empty(t, req.ID)
	assert.Empty(t, req.Queue)
	assert.Equal(t, RequestStateUnknown, req.State)
	assert.Equal(t, RequestLandStrategyUnknown, req.LandStrategy)
	assert.Equal(t, int32(0), req.Version)
}

func TestRequest_SerializationRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "full request with multiple PRs",
			req: Request{
				ID:           "queue1/100",
				Queue:        "queue1",
				Change:       Change{Provider: "github", URIs: []string{"github.com/uber/repo-a/101/aaa111", "github.com/uber/repo-a/102/bbb222", "github.com/uber/repo-a/103/ccc333"}},
				LandStrategy: RequestLandStrategySquashRebase,
				State:        RequestStateLanded,
				Version:      5,
			},
		},
		{
			name: "phabricator revision",
			req: Request{
				ID:           "queue2/200",
				Queue:        "queue2",
				Change:       Change{Provider: "phabricator", URIs: []string{"phabricator.uber.com/D12345/abc123def456"}},
				LandStrategy: RequestLandStrategyRebase,
				State:        RequestStateNew,
				Version:      1,
			},
		},
		{
			name: "github enterprise request",
			req: Request{
				ID:           "queue3/300",
				Queue:        "queue3",
				Change:       Change{Provider: "github-enterprise", URIs: []string{"github.uber.com/internal/service/999/deadbeef12"}},
				LandStrategy: RequestLandStrategyMerge,
				State:        RequestStateError,
				Version:      10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			data, err := tt.req.ToBytes()
			require.NoError(t, err)

			// Deserialize
			deserialized, err := RequestFromBytes(data)
			require.NoError(t, err)

			// Verify complete equality
			assert.Equal(t, tt.req, deserialized)
		})
	}
}

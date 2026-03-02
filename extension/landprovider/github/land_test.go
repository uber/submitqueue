package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/entity"
	"github.com/uber/submitqueue/extension/landprovider"
	"go.uber.org/zap/zaptest"
)

func newTestLandProvider(t *testing.T, serverURL string) landprovider.LandProvider {
	logger := zaptest.NewLogger(t).Sugar()
	scope := tally.NoopScope

	return NewLandProvider(Params{
		HTTPClient:   &http.Client{},
		APIURL:       serverURL,
		Logger:       logger,
		MetricsScope: scope,
	})
}

func mergeHandler(t *testing.T, statusCode int, message string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t.Helper()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		resp := mergeResponse{Message: message}
		err := json.NewEncoder(w).Encode(resp)
		require.NoError(t, err)
	}
}

func TestLandProvider_Land(t *testing.T) {
	singleEntry := func(uri string) []entity.LandEntry {
		return []entity.LandEntry{{
			Strategy: entity.RequestLandStrategyRebase,
			Change:   entity.Change{URIs: []string{uri}},
		}}
	}

	tests := []struct {
		name     string
		handler  http.HandlerFunc
		entries  []entity.LandEntry
		wantErr  bool
		rejected bool
	}{
		{
			// Land → mergePR → 200 OK
			name:    "success",
			handler: mergeHandler(t, http.StatusOK, "Pull Request successfully merged"),
			entries: singleEntry("github://uber/repo/1/abc123"),
		},
		{
			// Land → ParseChangeID fails
			name: "invalid change ID",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Fail(t, "should not reach server")
			}),
			entries: singleEntry("invalid-id"),
			wantErr: true,
		},
		{
			// Land → mergePR → 405/409/422 → WrapLandRejected
			name:     "land rejected",
			handler:  mergeHandler(t, http.StatusConflict, "Head branch was modified"),
			entries:  singleEntry("github://uber/repo/1/abc123"),
			wantErr:  true,
			rejected: true,
		},
		{
			// Land → mergePR → unexpected status → plain error
			name: "server error",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("internal server error"))
			}),
			entries: singleEntry("github://uber/repo/1/abc123"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			lp := newTestLandProvider(t, server.URL)
			err := lp.Land(context.Background(), "test-queue", tt.entries)
			if tt.wantErr {
				require.Error(t, err)
				if tt.rejected {
					assert.True(t, landprovider.IsLandRejected(err))
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

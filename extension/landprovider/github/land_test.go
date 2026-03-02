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

// prMergedHandler returns a handler for GET /repos/{owner}/{repo}/pulls/{number}/merge.
// Returns 204 if merged, 404 if not (empty body, matching the GitHub API).
func prMergedHandler(merged bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if merged {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

// routingHandler routes PUT (merge) and GET (PR state) requests to separate handlers.
func routingHandler(putHandler, getHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			putHandler(w, r)
		case http.MethodGet:
			getHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
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
		name          string
		handler       http.HandlerFunc
		entries       []entity.LandEntry
		wantErr       bool
		rejected      bool
		alreadyLanded bool
	}{
		{
			// isPRMerged → not merged → mergePR → 200 OK
			name:    "success",
			handler: routingHandler(mergeHandler(t, http.StatusOK, "Pull Request successfully merged"), prMergedHandler(false)),
			entries: singleEntry("github://uber/repo/1/abc123"),
		},
		{
			// Land → ParseChangeID fails (no server call)
			name: "invalid change ID",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Fail(t, "should not reach server")
			}),
			entries: singleEntry("invalid-id"),
			wantErr: true,
		},
		{
			// isPRMerged → not merged → mergePR → 409 → WrapLandRejected
			name:     "land rejected",
			handler:  routingHandler(mergeHandler(t, http.StatusConflict, "Head branch was modified"), prMergedHandler(false)),
			entries:  singleEntry("github://uber/repo/1/abc123"),
			wantErr:  true,
			rejected: true,
		},
		{
			// isPRMerged → already merged → ErrAlreadyLanded (no merge attempt)
			name:          "already merged - idempotent retry",
			handler:       routingHandler(mergeHandler(t, http.StatusOK, "should not be called"), prMergedHandler(true)),
			entries:       singleEntry("github://uber/repo/1/abc123"),
			wantErr:       true,
			alreadyLanded: true,
		},
		{
			// isPRMerged → not merged → mergePR → 500 → plain error
			name: "server error",
			handler: routingHandler(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("internal server error"))
				}),
				prMergedHandler(false),
			),
			entries: singleEntry("github://uber/repo/1/abc123"),
			wantErr: true,
		},
		{
			// isPRMerged → unexpected status code (500) → error propagated
			name: "merge status check error",
			handler: routingHandler(
				mergeHandler(t, http.StatusOK, "should not be called"),
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}),
			),
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
				if tt.alreadyLanded {
					assert.True(t, landprovider.IsAlreadyLanded(err))
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

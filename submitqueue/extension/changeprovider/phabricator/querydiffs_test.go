package phabricator

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/platform/conduit"
	phttp "github.com/uber/submitqueue/platform/http"
)

func TestQueryDiffs(t *testing.T) {
	testCases := []struct {
		name          string
		diffIDs       []int
		reply         string
		want          map[int]*diffResult
		wantErr       bool
		wantErrorCode string
	}{
		{
			name:    "returns every requested diff",
			diffIDs: []int{100, 101},
			reply:   `{"result":{"100":{"authorName":"Alice","changes":[{"currentPath":"a.go"}]},"101":{"authorName":"Bob","changes":[{"currentPath":"b.go"}]}}}`,
			want: map[int]*diffResult{
				100: {AuthorName: "Alice", Changes: []fileChange{{CurrentPath: "a.go"}}},
				101: {AuthorName: "Bob", Changes: []fileChange{{CurrentPath: "b.go"}}},
			},
		},
		{
			name:    "diff missing from the result",
			diffIDs: []int{100},
			reply:   `{"result":{"999":{}}}`,
			wantErr: true,
		},
		{
			name:    "empty result",
			diffIDs: []int{100},
			reply:   `{"result":[]}`,
			wantErr: true,
		},
		{
			name:          "conduit error",
			diffIDs:       []int{100},
			reply:         `{"result":null,"error_code":"ERR-CONDUIT-CORE","error_info":"Invalid diff ID"}`,
			wantErr:       true,
			wantErrorCode: "ERR-CONDUIT-CORE",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			type request struct {
				path string
				ids  []string
			}
			requests := make(chan request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- request{path: r.URL.Path, ids: requestedDiffIDs(t, r)}
				_, _ = w.Write([]byte(tc.reply))
			}))
			t.Cleanup(server.Close)
			httpClient, err := phttp.NewClient(server.URL)
			require.NoError(t, err)

			got, err := queryDiffs(t.Context(), conduit.NewClient(httpClient, "test-token"), tc.diffIDs)

			received := <-requests
			assert.Equal(t, "/api/differential.querydiffs", received.path)
			wantIDs := make([]string, len(tc.diffIDs))
			for i, id := range tc.diffIDs {
				wantIDs[i] = strconv.Itoa(id)
			}
			assert.Equal(t, wantIDs, received.ids)
			if tc.wantErr {
				require.Error(t, err)
				var conduitErr *conduit.Error
				assert.Equal(t, tc.wantErrorCode != "", errors.As(err, &conduitErr))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

package phabricator

import (
	"context"
	"fmt"
	"strconv"

	"github.com/uber/submitqueue/platform/conduit"
)

const queryDiffsMethod = "differential.querydiffs"

// diffResult represents a single diff from the differential.querydiffs response.
type diffResult struct {
	Changes     []fileChange `json:"changes"`
	AuthorName  string       `json:"authorName"`
	AuthorEmail string       `json:"authorEmail"`
}

// fileChange represents a single file modification in a Phabricator diff.
type fileChange struct {
	CurrentPath string `json:"currentPath"`
	AddLines    string `json:"addLines"`
	DelLines    string `json:"delLines"`
}

// queryDiffs reads diffIDs with one differential.querydiffs call and fails if
// Conduit omits any of them.
func queryDiffs(ctx context.Context, client *conduit.Client, diffIDs []int) (map[int]*diffResult, error) {
	var byID map[string]*diffResult
	if err := client.Call(ctx, queryDiffsMethod, map[string][]int{"ids": diffIDs}, &byID); err != nil {
		return nil, err
	}

	results := make(map[int]*diffResult, len(diffIDs))
	for _, id := range diffIDs {
		diff, ok := byID[strconv.Itoa(id)]
		if !ok {
			return nil, fmt.Errorf("diff %d not found in querydiffs response", id)
		}
		results[id] = diff
	}
	return results, nil
}

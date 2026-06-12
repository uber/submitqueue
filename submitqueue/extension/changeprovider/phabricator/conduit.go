package phabricator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// conduitResponse stores a response from the Conduit API. Every Conduit endpoint wraps its
// payload in this shape; a non-nil ErrorCode signals a server-side failure.
type conduitResponse struct {
	Result    json.RawMessage `json:"result"`
	ErrorCode *string         `json:"error_code"`
	ErrorInfo *string         `json:"error_info"`
}

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

// buildQueryDiffsRequest builds query parameters for a differential.querydiffs call.
func buildQueryDiffsRequest(diffIDs []int, apiToken string) url.Values {
	form := url.Values{}
	for _, id := range diffIDs {
		form.Add("ids[]", strconv.Itoa(id))
	}
	if apiToken != "" {
		form.Set("api.token", apiToken)
	}
	return form
}

// doConduitRequest executes a Conduit HTTP request.
// The path is relative — transport layers on the client resolve it to the full URL.
// Authentication (e.g. Bearer token) is handled by the client's transport.
func doConduitRequest(ctx context.Context, httpClient *http.Client, form url.Values) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/differential.querydiffs", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.URL.RawQuery = form.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}

	return resp, nil
}

// parseConduitResponse reads the HTTP response, unwraps the Conduit response,
// and extracts the diffResults for the requested diff IDs.
func parseConduitResponse(resp *http.Response, diffIDs []int) (map[int]*diffResult, error) {
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Conduit API returned status %d: %s", resp.StatusCode, string(body))
	}

	var phabResponse conduitResponse
	if err := json.NewDecoder(resp.Body).Decode(&phabResponse); err != nil {
		return nil, fmt.Errorf("failed to decode Conduit response: %w", err)
	}

	if phabResponse.ErrorCode != nil {
		info := ""
		if phabResponse.ErrorInfo != nil {
			info = *phabResponse.ErrorInfo
		}
		return nil, fmt.Errorf("Conduit error %s: %s", *phabResponse.ErrorCode, info)
	}

	var rawMap map[string]*diffResult
	if err := json.Unmarshal(phabResponse.Result, &rawMap); err != nil {
		return nil, fmt.Errorf("failed to decode querydiffs result: %w", err)
	}

	results := make(map[int]*diffResult, len(diffIDs))
	for _, id := range diffIDs {
		diff, ok := rawMap[strconv.Itoa(id)]
		if !ok {
			return nil, fmt.Errorf("diff %d not found in querydiffs response", id)
		}
		results[id] = diff
	}

	return results, nil
}

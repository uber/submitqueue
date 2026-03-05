package github

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entity"
)

// mockRoundTripper is a mock implementation of http.RoundTripper for testing.
type mockRoundTripper struct {
	roundTripFunc func(*http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

// newMockClient creates an http.Client with a mock RoundTripper.
func newMockClient(roundTripFunc func(*http.Request) (*http.Response, error)) *http.Client {
	return &http.Client{
		Transport: &mockRoundTripper{roundTripFunc: roundTripFunc},
	}
}

func TestProvider_Get_Success(t *testing.T) {
	responseBody := `{
		"data": {
			"repository": {
				"pullRequest": {
					"number": 123,
					"headRefOid": "abc123def456",
					"author": {
						"login": "testuser",
						"name": "Test User",
						"email": "test@example.com"
					},
					"files": {
						"totalCount": 2,
						"pageInfo": {
							"endCursor": "",
							"hasNextPage": false
						},
						"nodes": [
							{
								"path": "main.go",
								"additions": 10,
								"deletions": 5,
								"changeType": "MODIFIED",
								"patch": "diff --git a/main.go b/main.go\n..."
							},
							{
								"path": "test.go",
								"additions": 20,
								"deletions": 0,
								"changeType": "ADDED",
								"patch": "diff --git a/test.go b/test.go\n..."
							}
						]
					}
				}
			}
		}
	}`

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "https://api.github.test/graphql", req.URL.String())
		assert.Equal(t, "application/json", req.Header.Get("Content-Type"))

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	changeInfo, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/123/abc123def456"},
	})

	require.NoError(t, err)
	require.Len(t, changeInfo, 1, "should return 1 ChangeInfo for 1 PR")

	info := changeInfo[0]
	assert.Equal(t, "github://uber/submitqueue/123/abc123def456", info.URI)
	assert.Equal(t, "Test User", info.User.Name)
	assert.Equal(t, "test@example.com", info.User.Email)
	assert.Len(t, info.ChangedFiles, 2)

	assert.Equal(t, "main.go", info.ChangedFiles[0].Path)
	assert.Equal(t, 10, info.ChangedFiles[0].LinesAdded)
	assert.Equal(t, 5, info.ChangedFiles[0].LinesDeleted)
	assert.Equal(t, 5, info.ChangedFiles[0].LinesModified)
	assert.Contains(t, info.ChangedFiles[0].Patch, "diff --git a/main.go")

	assert.Equal(t, "test.go", info.ChangedFiles[1].Path)
	assert.Equal(t, 20, info.ChangedFiles[1].LinesAdded)
	assert.Equal(t, 0, info.ChangedFiles[1].LinesDeleted)
	assert.Equal(t, 0, info.ChangedFiles[1].LinesModified)
}

func TestProvider_Get_Pagination(t *testing.T) {
	callCount := 0
	responses := []string{
		`{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 456,
						"headRefOid": "xyz789",
						"author": {
							"login": "user",
							"name": "User",
							"email": "user@example.com"
						},
						"files": {
							"totalCount": 150,
							"pageInfo": {
								"endCursor": "cursor1",
								"hasNextPage": true
							},
							"nodes": [
								{
									"path": "file1.go",
									"additions": 5,
									"deletions": 2,
									"changeType": "MODIFIED",
									"patch": "diff1"
								}
							]
						}
					}
				}
			}
		}`,
		`{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 456,
						"headRefOid": "xyz789",
						"author": {
							"login": "user",
							"name": "User",
							"email": "user@example.com"
						},
						"files": {
							"totalCount": 150,
							"pageInfo": {
								"endCursor": "",
								"hasNextPage": false
							},
							"nodes": [
								{
									"path": "file2.go",
									"additions": 3,
									"deletions": 1,
									"changeType": "MODIFIED",
									"patch": "diff2"
								}
							]
						}
					}
				}
			}
		}`,
	}

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		response := responses[callCount]
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(response)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	changeInfo, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/456/xyz789"},
	})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "should make 2 GraphQL requests for pagination")
	require.Len(t, changeInfo, 1, "should return 1 ChangeInfo for 1 PR")

	info := changeInfo[0]
	assert.Len(t, info.ChangedFiles, 2, "should combine files from both pages")
	assert.Equal(t, "file1.go", info.ChangedFiles[0].Path)
	assert.Equal(t, "file2.go", info.ChangedFiles[1].Path)
}

func TestProvider_Get_InvalidURI(t *testing.T) {
	client := NewClient(&http.Client{}, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"invalid://uri"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse GitHub change ID")
}

func TestProvider_Get_HTTPError(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return nil, assert.AnError
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/pull/123/abc"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP request failed")
}

func TestProvider_Get_APIError404(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBufferString(`{"message":"Not Found"}`)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/pull/999/abc"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GitHub API returned status 404")
}

func TestProvider_Get_GraphQLError(t *testing.T) {
	responseBody := `{
		"errors": [
			{
				"message": "Field 'pullRequest' doesn't exist on type 'Repository'",
				"type": "INVALID_FIELD"
			}
		]
	}`

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/pull/123/abc"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "GraphQL errors")
}

func TestProvider_Get_InvalidJSON(t *testing.T) {
	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(`{invalid json`)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/pull/123/abc"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode GraphQL response")
}

func TestNewProvider(t *testing.T) {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	client := NewClient(httpClient, "https://api.github.com/graphql")

	provider := NewProvider(client, zaptest.NewLogger(t).Sugar(), tally.NoopScope)

	assert.NotNil(t, provider)
}

func TestProvider_Get_MultiplePRs(t *testing.T) {
	callCount := 0
	responses := map[int]string{
		0: `{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 123,
						"headRefOid": "abc123",
						"author": {
							"login": "user1",
							"name": "User One",
							"email": "user1@example.com"
						},
						"files": {
							"totalCount": 1,
							"pageInfo": {
								"endCursor": "",
								"hasNextPage": false
							},
							"nodes": [
								{
									"path": "file1.go",
									"additions": 10,
									"deletions": 5,
									"changeType": "MODIFIED",
									"patch": "diff1"
								}
							]
						}
					}
				}
			}
		}`,
		1: `{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 456,
						"headRefOid": "def456",
						"author": {
							"login": "user1",
							"name": "User One",
							"email": "user1@example.com"
						},
						"files": {
							"totalCount": 1,
							"pageInfo": {
								"endCursor": "",
								"hasNextPage": false
							},
							"nodes": [
								{
									"path": "file2.go",
									"additions": 20,
									"deletions": 2,
									"changeType": "ADDED",
									"patch": "diff2"
								}
							]
						}
					}
				}
			}
		}`,
	}

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		response := responses[callCount]
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(response)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	changeInfo, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"github://uber/submitqueue/456/def456",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "should make 2 GraphQL requests for 2 PRs")
	require.Len(t, changeInfo, 2, "should return 2 ChangeInfo for 2 PRs")

	// First PR
	assert.Equal(t, "github://uber/submitqueue/123/abc123", changeInfo[0].URI)
	assert.Equal(t, "User One", changeInfo[0].User.Name)
	assert.Equal(t, "user1@example.com", changeInfo[0].User.Email)
	assert.Len(t, changeInfo[0].ChangedFiles, 1)
	assert.Equal(t, "file1.go", changeInfo[0].ChangedFiles[0].Path)

	// Second PR
	assert.Equal(t, "github://uber/submitqueue/456/def456", changeInfo[1].URI)
	assert.Equal(t, "User One", changeInfo[1].User.Name)
	assert.Equal(t, "user1@example.com", changeInfo[1].User.Email)
	assert.Len(t, changeInfo[1].ChangedFiles, 1)
	assert.Equal(t, "file2.go", changeInfo[1].ChangedFiles[0].Path)
}

func TestProvider_Get_CrossRepoStack(t *testing.T) {
	client := NewClient(&http.Client{}, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"github://uber/different-repo/456/def456", // Different repo!
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stacked changes must be from same repository")
	assert.Contains(t, err.Error(), "expected uber/submitqueue")
	assert.Contains(t, err.Error(), "got uber/different-repo")
}

func TestProvider_Get_MixedProviderStack(t *testing.T) {
	client := NewClient(&http.Client{}, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"ghe://uber/submitqueue/456/def456", // Different provider (GHE instead of github)!
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stacked changes must use same change provider")
	assert.Contains(t, err.Error(), "expected github")
	assert.Contains(t, err.Error(), "got ghe")
}

func TestProvider_Get_StalePR(t *testing.T) {
	responseBody := `{
		"data": {
			"repository": {
				"pullRequest": {
					"number": 123,
					"headRefOid": "newsha123",
					"author": {
						"login": "testuser",
						"name": "Test User",
						"email": "test@example.com"
					},
					"files": {
						"totalCount": 1,
						"pageInfo": {
							"endCursor": "",
							"hasNextPage": false
						},
						"nodes": [
							{
								"path": "main.go",
								"additions": 10,
								"deletions": 5,
								"changeType": "MODIFIED",
								"patch": "diff --git a/main.go..."
							}
						]
					}
				}
			}
		}
	}`

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(responseBody)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	_, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{"github://uber/submitqueue/123/oldsha456"}, // Different SHA!
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "PR #123 head SHA changed")
	assert.Contains(t, err.Error(), "expected oldsha456")
	assert.Contains(t, err.Error(), "got newsha123")
}

func TestProvider_Get_PartialSuccess(t *testing.T) {
	callCount := 0
	responses := map[int]string{
		0: `{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 123,
						"headRefOid": "abc123",
						"author": {
							"login": "user1",
							"name": "User One",
							"email": "user1@example.com"
						},
						"files": {
							"totalCount": 1,
							"pageInfo": {
								"endCursor": "",
								"hasNextPage": false
							},
							"nodes": [
								{
									"path": "file1.go",
									"additions": 10,
									"deletions": 5,
									"changeType": "MODIFIED",
									"patch": "diff1"
								}
							]
						}
					}
				}
			}
		}`,
		// Second PR will get an error response (404)
		2: `{
			"data": {
				"repository": {
					"pullRequest": {
						"number": 789,
						"headRefOid": "ghi789",
						"author": {
							"login": "user1",
							"name": "User One",
							"email": "user1@example.com"
						},
						"files": {
							"totalCount": 1,
							"pageInfo": {
								"endCursor": "",
								"hasNextPage": false
							},
							"nodes": [
								{
									"path": "file3.go",
									"additions": 15,
									"deletions": 3,
									"changeType": "MODIFIED",
									"patch": "diff3"
								}
							]
						}
					}
				}
			}
		}`,
	}

	mockClient := newMockClient(func(req *http.Request) (*http.Response, error) {
		if callCount == 1 {
			// Fail on second PR
			callCount++
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewBufferString(`{"message":"Not Found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		response := responses[callCount]
		callCount++
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBufferString(response)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(mockClient, "https://api.github.test/graphql")
	provider := NewProvider(
		client,
		zaptest.NewLogger(t).Sugar(),
		tally.NoopScope,
	)

	changeInfo, err := provider.Get(context.Background(), entity.Change{
		URIs: []string{
			"github://uber/submitqueue/123/abc123",
			"github://uber/submitqueue/456/def456", // This will fail
			"github://uber/submitqueue/789/ghi789",
		},
	})

	// Should return partial results with error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch 1 of 3 PRs")
	assert.Contains(t, err.Error(), "failed: [456]")

	// Should have 2 successful PRs
	require.Len(t, changeInfo, 2, "should return 2 successful ChangeInfo despite 1 failure")
	assert.Equal(t, "github://uber/submitqueue/123/abc123", changeInfo[0].URI)
	assert.Equal(t, "github://uber/submitqueue/789/ghi789", changeInfo[1].URI)
}

package github

import "github.com/uber/submitqueue/platform/githubapi"

// pullRequestQuery is the GraphQL query to fetch pull request information including files, author, and head SHA.
const pullRequestQuery = `
query($owner: String!, $repo: String!, $prNumber: Int!, $filesCursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      number
      headRefOid
      author {
        login
        ... on User {
          name
          email
        }
      }
      files(first: 100, after: $filesCursor) {
        totalCount
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          path
          additions
          deletions
          changeType
        }
      }
    }
  }
}
`

// pullRequestQueryData is the data of a pullRequestQuery response.
type pullRequestQueryData struct {
	Repository struct {
		PullRequest pullRequestData `json:"pullRequest"`
	} `json:"repository"`
}

// pullRequestData contains the pull request metadata.
type pullRequestData struct {
	Number     int        `json:"number"`
	HeadRefOid string     `json:"headRefOid"`
	Author     authorData `json:"author"`
	Files      filesData  `json:"files"`
}

// authorData contains the author information.
type authorData struct {
	Login string `json:"login"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// filesData contains the files changed in the pull request.
type filesData struct {
	TotalCount int        `json:"totalCount"`
	PageInfo   pageInfo   `json:"pageInfo"`
	Nodes      []fileNode `json:"nodes"`
}

// pageInfo contains pagination information.
type pageInfo struct {
	EndCursor   string `json:"endCursor"`
	HasNextPage bool   `json:"hasNextPage"`
}

// fileNode represents a single changed file.
type fileNode struct {
	Path       string `json:"path"`
	Additions  int    `json:"additions"`
	Deletions  int    `json:"deletions"`
	ChangeType string `json:"changeType"`
}

// buildGraphQLRequest builds a GraphQL request for fetching pull request data.
func buildGraphQLRequest(org, repo string, prNumber int, cursor string) githubapi.GraphQLRequest {
	return githubapi.GraphQLRequest{
		Query: pullRequestQuery,
		Variables: map[string]any{
			"owner":       org,
			"repo":        repo,
			"prNumber":    prNumber,
			"filesCursor": cursor,
		},
	}
}

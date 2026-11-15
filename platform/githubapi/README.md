# GitHub API conventions

The parts of a GitHub API request that every direct caller would otherwise repeat:

- `SetRESTHeaders` sets the JSON media type and pins the REST API version (`Version`), so every REST caller moves to a new version together. Pass it as `platform/http.SendRequest`'s `setHeaders`.
- `QueryGraphQL` posts a `GraphQLRequest` to `/graphql` and decodes the response's `data` into the caller's type. A non-200 reply is a `platform/http.StatusError`, so `platform/errs/http` classifies it like any other HTTP failure; a 200 reply that carries GraphQL errors fails even when it also carries data.

This package is not a client. Each caller keeps its own `*http.Client` (API root from `platform/http.BaseURLTransport`, auth from a transport) and its own typed requests and responses, and decides what a missing resource means for it.

Its Phabricator counterpart, `platform/conduit`, does hold a client, because Conduit reads the API token from the request body, where a transport cannot add it.

## Who uses it

- `platform/githubactions` and `runway/extension/merger/github` (REST)
- `submitqueue/extension/changeprovider/github` and `submitqueue/extension/validator/github` (GraphQL)
- `test/testutil/githubtestrepo` (REST)

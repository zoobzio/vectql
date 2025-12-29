package types

// QueryResult represents the output of rendering a VectorAST.
type QueryResult struct {
	// JSON holds the serialized JSON query for the provider API.
	JSON string

	// RequiredParams lists all parameter names required for the query.
	RequiredParams []string
}

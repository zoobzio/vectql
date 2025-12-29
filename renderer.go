package vectql

import "github.com/zoobzio/vectql/internal/types"

// Renderer defines the interface for provider-specific query rendering.
type Renderer interface {
	// Render converts a VectorAST to a provider-specific QueryResult.
	Render(ast *types.VectorAST) (*types.QueryResult, error)

	// SupportsOperation indicates if the provider supports an operation.
	SupportsOperation(op types.Operation) bool

	// SupportsFilter indicates if the provider supports a filter operator.
	SupportsFilter(op types.FilterOperator) bool

	// SupportsMetric indicates if the provider supports a distance metric.
	SupportsMetric(metric types.DistanceMetric) bool
}

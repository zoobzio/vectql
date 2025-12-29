// Package vectql provides a type-safe query builder for vector databases.
package vectql

import (
	"fmt"

	"github.com/zoobzio/vectql/internal/types"
)

// Builder provides a fluent API for constructing vector queries.
type Builder struct {
	ast *types.VectorAST
	err error
}

// Search creates a new similarity search query builder.
func Search(c types.Collection) *Builder {
	return &Builder{
		ast: &types.VectorAST{
			Operation:       types.OpSearch,
			Target:          c,
			IncludeMetadata: true,
		},
	}
}

// Upsert creates a new upsert (insert/update) query builder.
func Upsert(c types.Collection) *Builder {
	return &Builder{
		ast: &types.VectorAST{
			Operation: types.OpUpsert,
			Target:    c,
		},
	}
}

// Delete creates a new delete query builder.
func Delete(c types.Collection) *Builder {
	return &Builder{
		ast: &types.VectorAST{
			Operation: types.OpDelete,
			Target:    c,
		},
	}
}

// Fetch creates a new fetch-by-ID query builder.
func Fetch(c types.Collection) *Builder {
	return &Builder{
		ast: &types.VectorAST{
			Operation:       types.OpFetch,
			Target:          c,
			IncludeMetadata: true,
			IncludeVectors:  true,
		},
	}
}

// Update creates a new metadata update query builder.
func Update(c types.Collection) *Builder {
	return &Builder{
		ast: &types.VectorAST{
			Operation: types.OpUpdate,
			Target:    c,
			Updates:   make(map[types.MetadataField]types.Param),
		},
	}
}

// Vector sets the query vector for similarity search.
func (b *Builder) Vector(v types.VectorValue) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSearch {
		b.err = fmt.Errorf("Vector() can only be used with SEARCH")
		return b
	}
	b.ast.QueryVector = &v
	return b
}

// Embedding specifies which embedding field to search against.
func (b *Builder) Embedding(e types.EmbeddingField) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSearch {
		b.err = fmt.Errorf("Embedding() can only be used with SEARCH")
		return b
	}
	b.ast.QueryEmbedding = &e
	return b
}

// TopK sets the number of results to return.
func (b *Builder) TopK(k int) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSearch {
		b.err = fmt.Errorf("TopK() can only be used with SEARCH")
		return b
	}
	if k > types.MaxTopK {
		b.err = fmt.Errorf("topK exceeds maximum: %d > %d", k, types.MaxTopK)
		return b
	}
	if k <= 0 {
		b.err = fmt.Errorf("topK must be positive: %d", k)
		return b
	}
	b.ast.TopK = &types.PaginationValue{Static: &k}
	return b
}

// TopKParam sets topK from a parameter.
func (b *Builder) TopKParam(p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSearch {
		b.err = fmt.Errorf("TopKParam() can only be used with SEARCH")
		return b
	}
	b.ast.TopK = &types.PaginationValue{Param: &p}
	return b
}

// MinScore sets a minimum similarity threshold.
func (b *Builder) MinScore(p types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpSearch {
		b.err = fmt.Errorf("MinScore() can only be used with SEARCH")
		return b
	}
	b.ast.MinScore = &p
	return b
}

// IncludeVectors specifies whether to return vectors in results.
func (b *Builder) IncludeVectors(include bool) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.IncludeVectors = include
	return b
}

// IncludeMetadata specifies whether to return metadata in results.
func (b *Builder) IncludeMetadata(include bool) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.IncludeMetadata = include
	return b
}

// Filter sets or adds filter conditions.
func (b *Builder) Filter(f types.FilterItem) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.FilterClause == nil {
		b.ast.FilterClause = f
	} else {
		b.ast.FilterClause = types.FilterGroup{
			Logic:      types.AND,
			Conditions: []types.FilterItem{b.ast.FilterClause, f},
		}
	}
	return b
}

// Where is an alias for Filter.
func (b *Builder) Where(f types.FilterItem) *Builder {
	return b.Filter(f)
}

// SelectMetadata specifies which metadata fields to return.
func (b *Builder) SelectMetadata(fields ...types.MetadataField) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.MetadataFields = fields
	return b
}

// Namespace sets the namespace/partition for the query.
func (b *Builder) Namespace(ns types.Param) *Builder {
	if b.err != nil {
		return b
	}
	b.ast.Namespace = &ns
	return b
}

// AddVector adds a vector record for upsert.
func (b *Builder) AddVector(record types.VectorRecord) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpUpsert {
		b.err = fmt.Errorf("AddVector() can only be used with UPSERT")
		return b
	}
	if len(b.ast.Vectors) >= types.MaxBatchSize {
		b.err = fmt.Errorf("batch size exceeds maximum: %d", types.MaxBatchSize)
		return b
	}
	b.ast.Vectors = append(b.ast.Vectors, record)
	return b
}

// Vectors sets multiple vector records for batch upsert.
func (b *Builder) Vectors(records []types.VectorRecord) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpUpsert {
		b.err = fmt.Errorf("Vectors() can only be used with UPSERT")
		return b
	}
	if len(records) > types.MaxBatchSize {
		b.err = fmt.Errorf("batch size exceeds maximum: %d > %d", len(records), types.MaxBatchSize)
		return b
	}
	b.ast.Vectors = records
	return b
}

// Set adds a metadata field update.
func (b *Builder) Set(field types.MetadataField, value types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpUpdate {
		b.err = fmt.Errorf("Set() can only be used with UPDATE")
		return b
	}
	if b.ast.Updates == nil {
		b.ast.Updates = make(map[types.MetadataField]types.Param)
	}
	b.ast.Updates[field] = value
	return b
}

// IDs specifies vector IDs for fetch, delete, or update operations.
func (b *Builder) IDs(ids ...types.Param) *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpDelete && b.ast.Operation != types.OpFetch && b.ast.Operation != types.OpUpdate {
		b.err = fmt.Errorf("IDs() can only be used with DELETE, FETCH, or UPDATE")
		return b
	}
	if len(ids) > types.MaxIDsPerFetch {
		b.err = fmt.Errorf("too many IDs: %d > %d", len(ids), types.MaxIDsPerFetch)
		return b
	}
	b.ast.IDs = ids
	return b
}

// DeleteAll enables deletion of all vectors matching the filter.
func (b *Builder) DeleteAll() *Builder {
	if b.err != nil {
		return b
	}
	if b.ast.Operation != types.OpDelete {
		b.err = fmt.Errorf("DeleteAll() can only be used with DELETE")
		return b
	}
	b.ast.DeleteAll = true
	return b
}

// Build returns the constructed AST or an error.
func (b *Builder) Build() (*types.VectorAST, error) {
	if b.err != nil {
		return nil, b.err
	}
	if err := b.ast.Validate(); err != nil {
		return nil, err
	}
	return b.ast, nil
}

// MustBuild returns the AST or panics on error.
func (b *Builder) MustBuild() *types.VectorAST {
	ast, err := b.Build()
	if err != nil {
		panic(err)
	}
	return ast
}

// Render builds the AST and renders it using the provided renderer.
func (b *Builder) Render(renderer Renderer) (*types.QueryResult, error) {
	ast, err := b.Build()
	if err != nil {
		return nil, err
	}
	return renderer.Render(ast)
}

// MustRender renders the query or panics on error.
func (b *Builder) MustRender(renderer Renderer) *types.QueryResult {
	result, err := b.Render(renderer)
	if err != nil {
		panic(err)
	}
	return result
}

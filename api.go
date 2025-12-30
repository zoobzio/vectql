// Package vectql provides a type-safe query builder for vector databases.
//
// VECTQL supports multiple vector database backends through a common AST-based
// architecture, similar to how ASTQL handles SQL databases. It provides:
//
//   - Fluent builder API for constructing vector queries
//   - Provider-specific renderers for Pinecone, Qdrant, Milvus, and Weaviate
//   - Schema validation through VDML integration
//   - Parameterized queries for safe query construction
//
// Usage with VDML schema validation:
//
//	import (
//	    "github.com/zoobzio/vdml"
//	    "github.com/zoobzio/vectql"
//	    "github.com/zoobzio/vectql/pkg/pinecone"
//	)
//
//	schema := vdml.NewSchema("ecommerce").
//	    AddCollection(
//	        vdml.NewCollection("products").
//	            AddEmbedding(vdml.NewEmbedding("embedding", 1536).WithMetric(vdml.Cosine)).
//	            AddMetadata(vdml.NewMetadataField("category", vdml.TypeString)),
//	    )
//
//	v, err := vectql.NewFromVDML(schema)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	query := vectql.Search(v.C("products")).
//	    Vector(vectql.Vec(v.P("query_vec"))).
//	    Embedding(v.E("products", "embedding")).
//	    TopK(10).
//	    Filter(v.Eq(v.M("products", "category"), v.P("cat")))
//
//	result, err := query.Render(pinecone.New())
package vectql

import "github.com/zoobzio/vectql/internal/types"

// Re-export output types needed for Renderer interface and results.
// These are safe to export as they are produced by the library, not constructed by users.
type (
	// VectorAST represents the abstract syntax tree for vector queries.
	VectorAST = types.VectorAST

	// QueryResult represents the result of rendering a query.
	QueryResult = types.QueryResult
)

// Re-export interface types for type assertions and polymorphism.
// Interfaces are safe as they define behavior, not structure.
type (
	// FilterItem is the interface for filter conditions.
	FilterItem = types.FilterItem
)

// Re-export enum types - these are safe as they're just type-safe constants.
type (
	// Operation represents a vector database operation type.
	Operation = types.Operation

	// FilterOperator represents a filter operator.
	FilterOperator = types.FilterOperator

	// LogicOperator represents a logical operator (AND, OR, NOT).
	LogicOperator = types.LogicOperator

	// DistanceMetric represents a distance metric for similarity.
	DistanceMetric = types.DistanceMetric
)

// Internal types are intentionally NOT re-exported to prevent validation bypass:
// - Collection, EmbeddingField, MetadataField, Param: use instance methods (v.C(), v.E(), v.M(), v.P())
// - VectorValue, SparseVectorValue: use Vec(), SparseVec() constructors
// - VectorRecord: use Record() constructor
// - FilterCondition, FilterGroup, RangeFilter, GeoFilter: use filter constructors (Eq(), And(), Range(), etc.)

// Operation constants.
const (
	OpSearch = types.OpSearch
	OpUpsert = types.OpUpsert
	OpDelete = types.OpDelete
	OpFetch  = types.OpFetch
	OpUpdate = types.OpUpdate
)

// Filter operator constants.
const (
	OpEQ               = types.EQ
	OpNE               = types.NE
	OpGT               = types.GT
	OpGE               = types.GE
	OpLT               = types.LT
	OpLE               = types.LE
	OpIN               = types.IN
	OpNotIn            = types.NotIn
	OpContains         = types.Contains
	OpStartsWith       = types.StartsWith
	OpEndsWith         = types.EndsWith
	OpMatches          = types.Matches
	OpExists           = types.Exists
	OpNotExists        = types.NotExists
	OpArrayContains    = types.ArrayContains
	OpArrayContainsAny = types.ArrayContainsAny
	OpArrayContainsAll = types.ArrayContainsAll
)

// Logic operator constants.
const (
	LogicAND = types.AND
	LogicOR  = types.OR
	LogicNOT = types.NOT
)

// Distance metric constants.
const (
	MetricCosine     = types.Cosine
	MetricEuclidean  = types.Euclidean
	MetricDotProduct = types.DotProduct
	MetricManhattan  = types.Manhattan
)

// Complexity limit constants.
const (
	MaxFilterDepth    = types.MaxFilterDepth
	MaxBatchSize      = types.MaxBatchSize
	MaxTopK           = types.MaxTopK
	MaxMetadataFields = types.MaxMetadataFields
	MaxIDsPerFetch    = types.MaxIDsPerFetch
)

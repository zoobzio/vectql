package types

import "fmt"

// Operation represents the type of vector database operation.
type Operation string

// Vector database operations.
const (
	OpSearch Operation = "SEARCH"
	OpUpsert Operation = "UPSERT"
	OpDelete Operation = "DELETE"
	OpFetch  Operation = "FETCH"
	OpUpdate Operation = "UPDATE"
)

// Complexity limits.
const (
	MaxFilterDepth    = 5
	MaxBatchSize      = 100
	MaxTopK           = 10000
	MaxMetadataFields = 50
	MaxIDsPerFetch    = 1000
)

// VectorAST represents the abstract syntax tree for vector database queries.
type VectorAST struct {
	// Core operation
	Operation Operation
	Target    Collection

	// Search-specific fields
	QueryVector     *VectorValue
	QueryEmbedding  *EmbeddingField
	TopK            *PaginationValue
	MinScore        *Param
	IncludeVectors  bool
	IncludeMetadata bool

	// Filter clause
	FilterClause FilterItem

	// Metadata field selection
	MetadataFields []MetadataField

	// Upsert/Update specific
	Vectors []VectorRecord
	Updates map[MetadataField]Param

	// Delete/Fetch specific
	IDs       []Param
	DeleteAll bool

	// Namespace/partition
	Namespace *Param
}

// VectorValue can be a literal vector or a parameter reference.
type VectorValue struct {
	Literal []float32
	Param   *Param
}

// SparseVectorValue represents a sparse vector for hybrid search.
type SparseVectorValue struct {
	Indices []int
	Values  []float32
	Param   *Param
}

// VectorRecord represents a single vector for upsert operations.
type VectorRecord struct {
	ID           Param
	Vector       VectorValue
	Metadata     map[MetadataField]Param
	SparseVector *SparseVectorValue
}

// PaginationValue represents topK or limit values.
type PaginationValue struct {
	Static *int
	Param  *Param
}

// Validate validates the VectorAST.
func (ast *VectorAST) Validate() error {
	if ast.Target.Name == "" {
		return fmt.Errorf("target collection is required")
	}

	switch ast.Operation {
	case OpSearch:
		return ast.validateSearch()
	case OpUpsert:
		return ast.validateUpsert()
	case OpDelete:
		return ast.validateDelete()
	case OpFetch:
		return ast.validateFetch()
	case OpUpdate:
		return ast.validateUpdate()
	default:
		return fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (ast *VectorAST) validateSearch() error {
	if ast.QueryVector == nil {
		return fmt.Errorf("SEARCH requires a query vector")
	}

	if ast.TopK == nil {
		return fmt.Errorf("SEARCH requires TopK")
	}

	if ast.TopK.Static != nil && *ast.TopK.Static > MaxTopK {
		return fmt.Errorf("TopK exceeds maximum: %d > %d", *ast.TopK.Static, MaxTopK)
	}

	if ast.TopK.Static != nil && *ast.TopK.Static <= 0 {
		return fmt.Errorf("TopK must be positive: %d", *ast.TopK.Static)
	}

	if len(ast.MetadataFields) > MaxMetadataFields {
		return fmt.Errorf("metadata fields exceed maximum: %d > %d", len(ast.MetadataFields), MaxMetadataFields)
	}

	if ast.FilterClause != nil {
		if err := validateFilterDepth(ast.FilterClause, 0); err != nil {
			return err
		}
	}

	return nil
}

func (ast *VectorAST) validateUpsert() error {
	if len(ast.Vectors) == 0 {
		return fmt.Errorf("UPSERT requires at least one vector")
	}
	if len(ast.Vectors) > MaxBatchSize {
		return fmt.Errorf("batch size exceeds maximum: %d > %d", len(ast.Vectors), MaxBatchSize)
	}
	return nil
}

func (ast *VectorAST) validateDelete() error {
	if len(ast.IDs) == 0 && ast.FilterClause == nil {
		return fmt.Errorf("DELETE requires either IDs or a filter")
	}
	if ast.FilterClause != nil && !ast.DeleteAll {
		return fmt.Errorf("DELETE by filter requires DeleteAll() flag for safety")
	}
	if len(ast.IDs) > MaxIDsPerFetch {
		return fmt.Errorf("too many IDs: %d > %d", len(ast.IDs), MaxIDsPerFetch)
	}
	return nil
}

func (ast *VectorAST) validateFetch() error {
	if len(ast.IDs) == 0 {
		return fmt.Errorf("FETCH requires at least one ID")
	}
	if len(ast.IDs) > MaxIDsPerFetch {
		return fmt.Errorf("too many IDs: %d > %d", len(ast.IDs), MaxIDsPerFetch)
	}
	return nil
}

func (ast *VectorAST) validateUpdate() error {
	if len(ast.IDs) == 0 {
		return fmt.Errorf("UPDATE requires at least one ID")
	}
	if len(ast.Updates) == 0 {
		return fmt.Errorf("UPDATE requires at least one field to update")
	}
	if len(ast.IDs) > MaxIDsPerFetch {
		return fmt.Errorf("too many IDs: %d > %d", len(ast.IDs), MaxIDsPerFetch)
	}
	return nil
}

func validateFilterDepth(f FilterItem, depth int) error {
	if depth > MaxFilterDepth {
		return fmt.Errorf("filter nesting too deep: %d > %d", depth, MaxFilterDepth)
	}

	if group, ok := f.(FilterGroup); ok {
		for _, c := range group.Conditions {
			if err := validateFilterDepth(c, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

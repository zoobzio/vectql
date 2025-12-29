// Package pinecone provides a VECTQL renderer for Pinecone.
package pinecone

import (
	"encoding/json"
	"fmt"

	"github.com/zoobzio/vectql/internal/types"
)

// toResult serializes a query map to JSON and returns a QueryResult.
func toResult(query map[string]interface{}, params []string) (*types.QueryResult, error) {
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize query: %w", err)
	}
	return &types.QueryResult{
		JSON:           string(jsonBytes),
		RequiredParams: params,
	}, nil
}

// Renderer renders VectorAST to Pinecone query format.
type Renderer struct{}

// New creates a new Pinecone renderer.
func New() *Renderer {
	return &Renderer{}
}

// Render converts a VectorAST to Pinecone query format.
func (r *Renderer) Render(ast *types.VectorAST) (*types.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	var params []string

	switch ast.Operation {
	case types.OpSearch:
		return r.renderSearch(ast, &params)
	case types.OpUpsert:
		return r.renderUpsert(ast, &params)
	case types.OpDelete:
		return r.renderDelete(ast, &params)
	case types.OpFetch:
		return r.renderFetch(ast, &params)
	case types.OpUpdate:
		return r.renderUpdate(ast, &params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (r *Renderer) renderSearch(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})

	// TopK
	if ast.TopK != nil {
		if ast.TopK.Static != nil {
			query["topK"] = *ast.TopK.Static
		} else if ast.TopK.Param != nil {
			*params = append(*params, ast.TopK.Param.Name)
			query["topK"] = fmt.Sprintf(":%s", ast.TopK.Param.Name)
		}
	}

	// Include options
	query["includeValues"] = ast.IncludeVectors
	query["includeMetadata"] = ast.IncludeMetadata

	// Vector
	if ast.QueryVector != nil {
		if ast.QueryVector.Param != nil {
			*params = append(*params, ast.QueryVector.Param.Name)
			query["vector"] = fmt.Sprintf(":%s", ast.QueryVector.Param.Name)
		} else {
			query["vector"] = ast.QueryVector.Literal
		}
	}

	// Filter
	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	}

	// Namespace
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["namespace"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpsert(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	vectors := make([]map[string]interface{}, len(ast.Vectors))

	for i, record := range ast.Vectors {
		vec := make(map[string]interface{})

		// ID
		*params = append(*params, record.ID.Name)
		vec["id"] = fmt.Sprintf(":%s", record.ID.Name)

		// Vector
		if record.Vector.Param != nil {
			*params = append(*params, record.Vector.Param.Name)
			vec["values"] = fmt.Sprintf(":%s", record.Vector.Param.Name)
		} else {
			vec["values"] = record.Vector.Literal
		}

		// Metadata
		if len(record.Metadata) > 0 {
			metadata := make(map[string]interface{})
			for field, value := range record.Metadata {
				*params = append(*params, value.Name)
				metadata[field.Name] = fmt.Sprintf(":%s", value.Name)
			}
			vec["metadata"] = metadata
		}

		// Sparse vector
		if record.SparseVector != nil {
			if record.SparseVector.Param != nil {
				*params = append(*params, record.SparseVector.Param.Name)
				vec["sparseValues"] = fmt.Sprintf(":%s", record.SparseVector.Param.Name)
			} else {
				vec["sparseValues"] = map[string]interface{}{
					"indices": record.SparseVector.Indices,
					"values":  record.SparseVector.Values,
				}
			}
		}

		vectors[i] = vec
	}

	query := map[string]interface{}{
		"vectors": vectors,
	}

	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["namespace"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDelete(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})

	if len(ast.IDs) > 0 {
		ids := make([]string, len(ast.IDs))
		for i, id := range ast.IDs {
			*params = append(*params, id.Name)
			ids[i] = fmt.Sprintf(":%s", id.Name)
		}
		query["ids"] = ids
	} else if ast.FilterClause != nil && ast.DeleteAll {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
		query["deleteAll"] = false
	}

	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["namespace"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFetch(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	ids := make([]string, len(ast.IDs))
	for i, id := range ast.IDs {
		*params = append(*params, id.Name)
		ids[i] = fmt.Sprintf(":%s", id.Name)
	}

	query := map[string]interface{}{
		"ids": ids,
	}

	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["namespace"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	// Pinecone update is per-ID, so we use the first ID
	if len(ast.IDs) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one ID")
	}

	*params = append(*params, ast.IDs[0].Name)
	query := map[string]interface{}{
		"id": fmt.Sprintf(":%s", ast.IDs[0].Name),
	}

	if len(ast.Updates) > 0 {
		metadata := make(map[string]interface{})
		for field, value := range ast.Updates {
			*params = append(*params, value.Name)
			metadata[field.Name] = fmt.Sprintf(":%s", value.Name)
		}
		query["setMetadata"] = metadata
	}

	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["namespace"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFilter(f types.FilterItem, params *[]string) (interface{}, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		*params = append(*params, filter.Value.Name)
		return map[string]interface{}{
			filter.Field.Name: map[string]interface{}{
				r.mapOperator(filter.Operator): fmt.Sprintf(":%s", filter.Value.Name),
			},
		}, nil

	case types.FilterGroup:
		conditions := make([]interface{}, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			rendered, err := r.renderFilter(c, params)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rendered)
		}
		return map[string]interface{}{
			r.mapLogic(filter.Logic): conditions,
		}, nil

	case types.RangeFilter:
		rangeFilter := make(map[string]interface{})
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := "$gte"
			if filter.MinExclusive {
				op = "$gt"
			}
			rangeFilter[op] = fmt.Sprintf(":%s", filter.Min.Name)
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "$lte"
			if filter.MaxExclusive {
				op = "$lt"
			}
			rangeFilter[op] = fmt.Sprintf(":%s", filter.Max.Name)
		}
		return map[string]interface{}{
			filter.Field.Name: rangeFilter,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported filter type: %T", f)
	}
}

func (r *Renderer) mapOperator(op types.FilterOperator) string {
	switch op {
	case types.EQ:
		return "$eq"
	case types.NE:
		return "$ne"
	case types.GT:
		return "$gt"
	case types.GE:
		return "$gte"
	case types.LT:
		return "$lt"
	case types.LE:
		return "$lte"
	case types.IN:
		return "$in"
	case types.NotIn:
		return "$nin"
	default:
		return "$eq"
	}
}

func (r *Renderer) mapLogic(logic types.LogicOperator) string {
	switch logic {
	case types.AND:
		return "$and"
	case types.OR:
		return "$or"
	case types.NOT:
		return "$not"
	default:
		return "$and"
	}
}

// SupportsOperation indicates if Pinecone supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpSearch, types.OpUpsert, types.OpDelete, types.OpFetch, types.OpUpdate:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if Pinecone supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE, types.IN, types.NotIn:
		return true
	default:
		return false
	}
}

// SupportsMetric indicates if Pinecone supports a distance metric.
func (r *Renderer) SupportsMetric(metric types.DistanceMetric) bool {
	switch metric {
	case types.Cosine, types.Euclidean, types.DotProduct:
		return true
	default:
		return false
	}
}

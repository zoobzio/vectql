// Package qdrant provides a VECTQL renderer for Qdrant.
package qdrant

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

// Qdrant filter condition types.
const (
	condMust    = "must"
	condMustNot = "must_not"
	condShould  = "should"
)

// Renderer renders VectorAST to Qdrant query format.
type Renderer struct {
	// DefaultVectorName is the default vector name for named vectors.
	DefaultVectorName string
}

// New creates a new Qdrant renderer.
func New() *Renderer {
	return &Renderer{
		DefaultVectorName: "",
	}
}

// Render converts a VectorAST to Qdrant query format.
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

	// Vector
	vectorQuery := make(map[string]interface{})
	if ast.QueryVector != nil {
		if ast.QueryVector.Param != nil {
			*params = append(*params, ast.QueryVector.Param.Name)
			vectorQuery["vector"] = fmt.Sprintf(":%s", ast.QueryVector.Param.Name)
		} else {
			vectorQuery["vector"] = ast.QueryVector.Literal
		}
	}

	// Named vector support
	if ast.QueryEmbedding != nil && ast.QueryEmbedding.Name != "" {
		vectorQuery["name"] = ast.QueryEmbedding.Name
	} else if r.DefaultVectorName != "" {
		vectorQuery["name"] = r.DefaultVectorName
	}

	query["query"] = vectorQuery

	// TopK (limit in Qdrant)
	if ast.TopK != nil {
		if ast.TopK.Static != nil {
			query["limit"] = *ast.TopK.Static
		} else if ast.TopK.Param != nil {
			*params = append(*params, ast.TopK.Param.Name)
			query["limit"] = fmt.Sprintf(":%s", ast.TopK.Param.Name)
		}
	}

	// Score threshold
	if ast.MinScore != nil {
		*params = append(*params, ast.MinScore.Name)
		query["score_threshold"] = fmt.Sprintf(":%s", ast.MinScore.Name)
	}

	// With payload/vectors
	query["with_payload"] = ast.IncludeMetadata
	query["with_vector"] = ast.IncludeVectors

	// Filter
	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpsert(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	points := make([]map[string]interface{}, len(ast.Vectors))

	for i, record := range ast.Vectors {
		point := make(map[string]interface{})

		// ID
		*params = append(*params, record.ID.Name)
		point["id"] = fmt.Sprintf(":%s", record.ID.Name)

		// Vector
		if record.Vector.Param != nil {
			*params = append(*params, record.Vector.Param.Name)
			point["vector"] = fmt.Sprintf(":%s", record.Vector.Param.Name)
		} else {
			point["vector"] = record.Vector.Literal
		}

		// Payload (metadata)
		if len(record.Metadata) > 0 {
			payload := make(map[string]interface{})
			for field, value := range record.Metadata {
				*params = append(*params, value.Name)
				payload[field.Name] = fmt.Sprintf(":%s", value.Name)
			}
			point["payload"] = payload
		}

		points[i] = point
	}

	query := map[string]interface{}{
		"points": points,
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
		query["points"] = ids
	} else if ast.FilterClause != nil && ast.DeleteAll {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
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
		"ids":          ids,
		"with_payload": ast.IncludeMetadata,
		"with_vector":  ast.IncludeVectors,
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	ids := make([]string, len(ast.IDs))
	for i, id := range ast.IDs {
		*params = append(*params, id.Name)
		ids[i] = fmt.Sprintf(":%s", id.Name)
	}

	payload := make(map[string]interface{})
	for field, value := range ast.Updates {
		*params = append(*params, value.Name)
		payload[field.Name] = fmt.Sprintf(":%s", value.Name)
	}

	query := map[string]interface{}{
		"points":  ids,
		"payload": payload,
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFilter(f types.FilterItem, params *[]string) (interface{}, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		*params = append(*params, filter.Value.Name)
		return map[string]interface{}{
			r.mapConditionType(filter.Operator): []map[string]interface{}{
				{
					"key":   filter.Field.Name,
					"match": map[string]interface{}{"value": fmt.Sprintf(":%s", filter.Value.Name)},
				},
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
		rangeCondition := map[string]interface{}{
			"key": filter.Field.Name,
		}
		rangeValues := make(map[string]interface{})
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			if filter.MinExclusive {
				rangeValues["gt"] = fmt.Sprintf(":%s", filter.Min.Name)
			} else {
				rangeValues["gte"] = fmt.Sprintf(":%s", filter.Min.Name)
			}
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			if filter.MaxExclusive {
				rangeValues["lt"] = fmt.Sprintf(":%s", filter.Max.Name)
			} else {
				rangeValues["lte"] = fmt.Sprintf(":%s", filter.Max.Name)
			}
		}
		rangeCondition["range"] = rangeValues
		return map[string]interface{}{
			condMust: []interface{}{rangeCondition},
		}, nil

	case types.GeoFilter:
		*params = append(*params, filter.Center.Lat.Name)
		*params = append(*params, filter.Center.Lon.Name)
		*params = append(*params, filter.Radius.Name)
		return map[string]interface{}{
			condMust: []map[string]interface{}{
				{
					"key": filter.Field.Name,
					"geo_radius": map[string]interface{}{
						"center": map[string]interface{}{
							"lat": fmt.Sprintf(":%s", filter.Center.Lat.Name),
							"lon": fmt.Sprintf(":%s", filter.Center.Lon.Name),
						},
						"radius": fmt.Sprintf(":%s", filter.Radius.Name),
					},
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported filter type: %T", f)
	}
}

func (r *Renderer) mapConditionType(op types.FilterOperator) string {
	switch op {
	case types.NE:
		return condMustNot
	default:
		return condMust
	}
}

func (r *Renderer) mapLogic(logic types.LogicOperator) string {
	switch logic {
	case types.AND:
		return condMust
	case types.OR:
		return condShould
	case types.NOT:
		return condMustNot
	default:
		return condMust
	}
}

// SupportsOperation indicates if Qdrant supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpSearch, types.OpUpsert, types.OpDelete, types.OpFetch, types.OpUpdate:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if Qdrant supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE,
		types.IN, types.Contains, types.Exists, types.NotExists:
		return true
	default:
		return false
	}
}

// SupportsMetric indicates if Qdrant supports a distance metric.
func (r *Renderer) SupportsMetric(metric types.DistanceMetric) bool {
	switch metric {
	case types.Cosine, types.Euclidean, types.DotProduct, types.Manhattan:
		return true
	default:
		return false
	}
}

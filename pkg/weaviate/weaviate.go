// Package weaviate provides a VECTQL renderer for Weaviate.
package weaviate

import (
	"encoding/json"
	"fmt"
	"strings"

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

// Renderer renders VectorAST to Weaviate GraphQL format.
type Renderer struct{}

// New creates a new Weaviate renderer.
func New() *Renderer {
	return &Renderer{}
}

// Render converts a VectorAST to Weaviate query format.
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

	// Class name (collection)
	className := r.formatClassName(ast.Target.Name)
	query["class"] = className

	// Near vector
	nearVector := make(map[string]interface{})
	if ast.QueryVector != nil {
		if ast.QueryVector.Param != nil {
			*params = append(*params, ast.QueryVector.Param.Name)
			nearVector["vector"] = fmt.Sprintf(":%s", ast.QueryVector.Param.Name)
		} else {
			nearVector["vector"] = ast.QueryVector.Literal
		}
	}

	// Certainty threshold
	if ast.MinScore != nil {
		*params = append(*params, ast.MinScore.Name)
		nearVector["certainty"] = fmt.Sprintf(":%s", ast.MinScore.Name)
	}

	// Target vectors (named vectors)
	if ast.QueryEmbedding != nil && ast.QueryEmbedding.Name != "" {
		nearVector["targetVectors"] = []string{ast.QueryEmbedding.Name}
	}

	query["nearVector"] = nearVector

	// Limit
	if ast.TopK != nil {
		if ast.TopK.Static != nil {
			query["limit"] = *ast.TopK.Static
		} else if ast.TopK.Param != nil {
			*params = append(*params, ast.TopK.Param.Name)
			query["limit"] = fmt.Sprintf(":%s", ast.TopK.Param.Name)
		}
	}

	// Properties to return
	if ast.IncludeMetadata && len(ast.MetadataFields) > 0 {
		fields := make([]string, len(ast.MetadataFields))
		for i, f := range ast.MetadataFields {
			fields[i] = f.Name
		}
		query["properties"] = fields
	}

	// Filter (where clause)
	if ast.FilterClause != nil {
		where, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["where"] = where
	}

	// Tenant (namespace)
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["tenant"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	// Additional fields for vectors
	if ast.IncludeVectors {
		query["additional"] = []string{"vector", "distance", "certainty"}
	} else {
		query["additional"] = []string{"distance", "certainty"}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpsert(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	className := r.formatClassName(ast.Target.Name)

	objects := make([]map[string]interface{}, len(ast.Vectors))
	for i, record := range ast.Vectors {
		obj := map[string]interface{}{
			"class": className,
		}

		// ID
		*params = append(*params, record.ID.Name)
		obj["id"] = fmt.Sprintf(":%s", record.ID.Name)

		// Vector
		if record.Vector.Param != nil {
			*params = append(*params, record.Vector.Param.Name)
			obj["vector"] = fmt.Sprintf(":%s", record.Vector.Param.Name)
		} else {
			obj["vector"] = record.Vector.Literal
		}

		// Properties (metadata)
		if len(record.Metadata) > 0 {
			properties := make(map[string]interface{})
			for field, value := range record.Metadata {
				*params = append(*params, value.Name)
				properties[field.Name] = fmt.Sprintf(":%s", value.Name)
			}
			obj["properties"] = properties
		}

		objects[i] = obj
	}

	query := map[string]interface{}{
		"objects": objects,
	}

	// Tenant
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["tenant"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDelete(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	className := r.formatClassName(ast.Target.Name)

	query := map[string]interface{}{
		"class": className,
	}

	if len(ast.IDs) > 0 {
		ids := make([]string, len(ast.IDs))
		for i, id := range ast.IDs {
			*params = append(*params, id.Name)
			ids[i] = fmt.Sprintf(":%s", id.Name)
		}
		query["ids"] = ids
	} else if ast.FilterClause != nil && ast.DeleteAll {
		where, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["where"] = where
	}

	// Tenant
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["tenant"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFetch(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	className := r.formatClassName(ast.Target.Name)

	ids := make([]string, len(ast.IDs))
	for i, id := range ast.IDs {
		*params = append(*params, id.Name)
		ids[i] = fmt.Sprintf(":%s", id.Name)
	}

	query := map[string]interface{}{
		"class": className,
		"ids":   ids,
	}

	// Properties
	if ast.IncludeMetadata && len(ast.MetadataFields) > 0 {
		fields := make([]string, len(ast.MetadataFields))
		for i, f := range ast.MetadataFields {
			fields[i] = f.Name
		}
		query["properties"] = fields
	}

	// Additional
	additional := []string{}
	if ast.IncludeVectors {
		additional = append(additional, "vector")
	}
	if len(additional) > 0 {
		query["additional"] = additional
	}

	// Tenant
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["tenant"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	className := r.formatClassName(ast.Target.Name)

	// Weaviate updates one object at a time
	if len(ast.IDs) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one ID")
	}

	*params = append(*params, ast.IDs[0].Name)

	properties := make(map[string]interface{})
	for field, value := range ast.Updates {
		*params = append(*params, value.Name)
		properties[field.Name] = fmt.Sprintf(":%s", value.Name)
	}

	query := map[string]interface{}{
		"class":      className,
		"id":         fmt.Sprintf(":%s", ast.IDs[0].Name),
		"properties": properties,
	}

	// Tenant
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["tenant"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFilter(f types.FilterItem, params *[]string) (interface{}, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		*params = append(*params, filter.Value.Name)
		return map[string]interface{}{
			"path":        []string{filter.Field.Name},
			"operator":    r.mapOperator(filter.Operator),
			"valueString": fmt.Sprintf(":%s", filter.Value.Name),
		}, nil

	case types.FilterGroup:
		operands := make([]interface{}, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			rendered, err := r.renderFilter(c, params)
			if err != nil {
				return nil, err
			}
			operands = append(operands, rendered)
		}
		return map[string]interface{}{
			"operator": r.mapLogic(filter.Logic),
			"operands": operands,
		}, nil

	case types.RangeFilter:
		operands := []interface{}{}
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := "GreaterThanEqual"
			if filter.MinExclusive {
				op = "GreaterThan"
			}
			operands = append(operands, map[string]interface{}{
				"path":        []string{filter.Field.Name},
				"operator":    op,
				"valueNumber": fmt.Sprintf(":%s", filter.Min.Name),
			})
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "LessThanEqual"
			if filter.MaxExclusive {
				op = "LessThan"
			}
			operands = append(operands, map[string]interface{}{
				"path":        []string{filter.Field.Name},
				"operator":    op,
				"valueNumber": fmt.Sprintf(":%s", filter.Max.Name),
			})
		}
		if len(operands) == 1 {
			return operands[0], nil
		}
		return map[string]interface{}{
			"operator": "And",
			"operands": operands,
		}, nil

	case types.GeoFilter:
		*params = append(*params, filter.Center.Lat.Name)
		*params = append(*params, filter.Center.Lon.Name)
		*params = append(*params, filter.Radius.Name)
		return map[string]interface{}{
			"path":     []string{filter.Field.Name},
			"operator": "WithinGeoRange",
			"valueGeoRange": map[string]interface{}{
				"geoCoordinates": map[string]interface{}{
					"latitude":  fmt.Sprintf(":%s", filter.Center.Lat.Name),
					"longitude": fmt.Sprintf(":%s", filter.Center.Lon.Name),
				},
				"distance": map[string]interface{}{
					"max": fmt.Sprintf(":%s", filter.Radius.Name),
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported filter type: %T", f)
	}
}

func (r *Renderer) mapOperator(op types.FilterOperator) string {
	switch op {
	case types.EQ:
		return "Equal"
	case types.NE:
		return "NotEqual"
	case types.GT:
		return "GreaterThan"
	case types.GE:
		return "GreaterThanEqual"
	case types.LT:
		return "LessThan"
	case types.LE:
		return "LessThanEqual"
	case types.Contains:
		return "ContainsAny"
	case types.Exists:
		return "IsNull" // with false value
	default:
		return "Equal"
	}
}

func (r *Renderer) mapLogic(logic types.LogicOperator) string {
	switch logic {
	case types.AND:
		return "And"
	case types.OR:
		return "Or"
	case types.NOT:
		return "Not"
	default:
		return "And"
	}
}

func (r *Renderer) formatClassName(name string) string {
	// Weaviate class names must start with uppercase
	if len(name) == 0 {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

// SupportsOperation indicates if Weaviate supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpSearch, types.OpUpsert, types.OpDelete, types.OpFetch, types.OpUpdate:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if Weaviate supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE, types.Contains, types.Exists:
		return true
	default:
		return false
	}
}

// SupportsMetric indicates if Weaviate supports a distance metric.
func (r *Renderer) SupportsMetric(metric types.DistanceMetric) bool {
	switch metric {
	case types.Cosine, types.Euclidean, types.DotProduct, types.Manhattan:
		return true
	default:
		return false
	}
}

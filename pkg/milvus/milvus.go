// Package milvus provides a VECTQL renderer for Milvus.
package milvus

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

// Renderer renders VectorAST to Milvus query format.
type Renderer struct {
	// DefaultVectorField is the default vector field name.
	DefaultVectorField string
}

// New creates a new Milvus renderer.
func New() *Renderer {
	return &Renderer{
		DefaultVectorField: "embedding",
	}
}

// Render converts a VectorAST to Milvus query format.
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

	query["collection_name"] = ast.Target.Name

	// Vector field
	vectorField := r.DefaultVectorField
	if ast.QueryEmbedding != nil && ast.QueryEmbedding.Name != "" {
		vectorField = ast.QueryEmbedding.Name
	}
	query["anns_field"] = vectorField

	// Vector data
	if ast.QueryVector != nil {
		if ast.QueryVector.Param != nil {
			*params = append(*params, ast.QueryVector.Param.Name)
			query["data"] = fmt.Sprintf(":%s", ast.QueryVector.Param.Name)
		} else {
			query["data"] = [][]float32{ast.QueryVector.Literal}
		}
	}

	// TopK
	if ast.TopK != nil {
		if ast.TopK.Static != nil {
			query["limit"] = *ast.TopK.Static
		} else if ast.TopK.Param != nil {
			*params = append(*params, ast.TopK.Param.Name)
			query["limit"] = fmt.Sprintf(":%s", ast.TopK.Param.Name)
		}
	}

	// Output fields
	if ast.IncludeMetadata && len(ast.MetadataFields) > 0 {
		fields := make([]string, len(ast.MetadataFields))
		for i, f := range ast.MetadataFields {
			fields[i] = f.Name
		}
		query["output_fields"] = fields
	}

	// Filter expression
	if ast.FilterClause != nil {
		expr, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = expr
	}

	// Partition (namespace)
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["partition_names"] = []string{fmt.Sprintf(":%s", ast.Namespace.Name)}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpsert(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	query := map[string]interface{}{
		"collection_name": ast.Target.Name,
	}

	// Build data rows
	data := make([]map[string]interface{}, len(ast.Vectors))
	for i, record := range ast.Vectors {
		row := make(map[string]interface{})

		// ID
		*params = append(*params, record.ID.Name)
		row["id"] = fmt.Sprintf(":%s", record.ID.Name)

		// Vector
		vectorField := r.DefaultVectorField
		if record.Vector.Param != nil {
			*params = append(*params, record.Vector.Param.Name)
			row[vectorField] = fmt.Sprintf(":%s", record.Vector.Param.Name)
		} else {
			row[vectorField] = record.Vector.Literal
		}

		// Metadata
		for field, value := range record.Metadata {
			*params = append(*params, value.Name)
			row[field.Name] = fmt.Sprintf(":%s", value.Name)
		}

		data[i] = row
	}
	query["data"] = data

	// Partition
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["partition_name"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDelete(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	query := map[string]interface{}{
		"collection_name": ast.Target.Name,
	}

	if len(ast.IDs) > 0 {
		// Delete by IDs - build expression
		idExprs := make([]string, len(ast.IDs))
		for i, id := range ast.IDs {
			*params = append(*params, id.Name)
			idExprs[i] = fmt.Sprintf(":%s", id.Name)
		}
		query["filter"] = fmt.Sprintf("id in [%s]", strings.Join(idExprs, ", "))
	} else if ast.FilterClause != nil && ast.DeleteAll {
		expr, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = expr
	}

	// Partition
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["partition_name"] = fmt.Sprintf(":%s", ast.Namespace.Name)
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFetch(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	query := map[string]interface{}{
		"collection_name": ast.Target.Name,
	}

	// Build ID filter expression
	idExprs := make([]string, len(ast.IDs))
	for i, id := range ast.IDs {
		*params = append(*params, id.Name)
		idExprs[i] = fmt.Sprintf(":%s", id.Name)
	}
	query["filter"] = fmt.Sprintf("id in [%s]", strings.Join(idExprs, ", "))

	// Output fields
	if ast.IncludeMetadata && len(ast.MetadataFields) > 0 {
		fields := make([]string, len(ast.MetadataFields))
		for i, f := range ast.MetadataFields {
			fields[i] = f.Name
		}
		query["output_fields"] = fields
	} else if ast.IncludeMetadata {
		query["output_fields"] = []string{"*"}
	}

	// Partition
	if ast.Namespace != nil {
		*params = append(*params, ast.Namespace.Name)
		query["partition_names"] = []string{fmt.Sprintf(":%s", ast.Namespace.Name)}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.VectorAST, params *[]string) (*types.QueryResult, error) {
	// Milvus uses upsert for updates
	query := map[string]interface{}{
		"collection_name": ast.Target.Name,
	}

	// Build data rows with updates
	data := make([]map[string]interface{}, len(ast.IDs))
	for i, id := range ast.IDs {
		row := make(map[string]interface{})
		*params = append(*params, id.Name)
		row["id"] = fmt.Sprintf(":%s", id.Name)

		for field, value := range ast.Updates {
			*params = append(*params, value.Name)
			row[field.Name] = fmt.Sprintf(":%s", value.Name)
		}
		data[i] = row
	}
	query["data"] = data

	return toResult(query, *params)
}

func (r *Renderer) renderFilter(f types.FilterItem, params *[]string) (string, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		*params = append(*params, filter.Value.Name)
		return fmt.Sprintf("%s %s :%s", filter.Field.Name, r.mapOperator(filter.Operator), filter.Value.Name), nil

	case types.FilterGroup:
		if filter.Logic == types.NOT {
			if len(filter.Conditions) > 0 {
				inner, err := r.renderFilter(filter.Conditions[0], params)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("not (%s)", inner), nil
			}
			return "", nil
		}

		parts := make([]string, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			rendered, err := r.renderFilter(c, params)
			if err != nil {
				return "", err
			}
			parts = append(parts, rendered)
		}
		op := " and "
		if filter.Logic == types.OR {
			op = " or "
		}
		return "(" + strings.Join(parts, op) + ")", nil

	case types.RangeFilter:
		var parts []string
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := ">="
			if filter.MinExclusive {
				op = ">"
			}
			parts = append(parts, fmt.Sprintf("%s %s :%s", filter.Field.Name, op, filter.Min.Name))
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "<="
			if filter.MaxExclusive {
				op = "<"
			}
			parts = append(parts, fmt.Sprintf("%s %s :%s", filter.Field.Name, op, filter.Max.Name))
		}
		return "(" + strings.Join(parts, " and ") + ")", nil

	default:
		return "", fmt.Errorf("unsupported filter type: %T", f)
	}
}

func (r *Renderer) mapOperator(op types.FilterOperator) string {
	switch op {
	case types.EQ:
		return "=="
	case types.NE:
		return "!="
	case types.GT:
		return ">"
	case types.GE:
		return ">="
	case types.LT:
		return "<"
	case types.LE:
		return "<="
	case types.IN:
		return "in"
	case types.NotIn:
		return "not in"
	case types.Contains:
		return "like"
	default:
		return "=="
	}
}

// SupportsOperation indicates if Milvus supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpSearch, types.OpUpsert, types.OpDelete, types.OpFetch, types.OpUpdate:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if Milvus supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GE, types.LT, types.LE,
		types.IN, types.NotIn, types.Contains:
		return true
	default:
		return false
	}
}

// SupportsMetric indicates if Milvus supports a distance metric.
func (r *Renderer) SupportsMetric(metric types.DistanceMetric) bool {
	switch metric {
	case types.Cosine, types.Euclidean, types.DotProduct:
		return true
	default:
		return false
	}
}

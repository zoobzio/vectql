package weaviate

import (
	"strings"
	"testing"

	"github.com/zoobzio/vectql/internal/types"
)

func TestRenderSearch(t *testing.T) {
	renderer := New()

	topK := 10
	ast := &types.VectorAST{
		Operation: types.OpSearch,
		Target:    types.Collection{Name: "products"},
		QueryVector: &types.VectorValue{
			Param: &types.Param{Name: "query_vec"},
		},
		TopK: &types.PaginationValue{
			Static: &topK,
		},
		IncludeMetadata: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Class name should be capitalized
	if !strings.Contains(result.JSON, `"class":"Products"`) {
		t.Errorf("expected class:Products in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"limit":10`) {
		t.Errorf("expected limit:10 in JSON: %s", result.JSON)
	}

	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "query_vec" {
		t.Errorf("expected RequiredParams=[query_vec], got %v", result.RequiredParams)
	}
}

func TestRenderSearchWithFilter(t *testing.T) {
	renderer := New()

	topK := 10
	ast := &types.VectorAST{
		Operation: types.OpSearch,
		Target:    types.Collection{Name: "products"},
		QueryVector: &types.VectorValue{
			Param: &types.Param{Name: "query_vec"},
		},
		TopK: &types.PaginationValue{
			Static: &topK,
		},
		FilterClause: types.FilterCondition{
			Field:    types.MetadataField{Name: "category"},
			Operator: types.EQ,
			Value:    types.Param{Name: "cat"},
		},
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"where"`) {
		t.Errorf("expected where in JSON: %s", result.JSON)
	}
}

func TestRenderUpsert(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation: types.OpUpsert,
		Target:    types.Collection{Name: "products"},
		Vectors: []types.VectorRecord{
			{
				ID:     types.Param{Name: "id1"},
				Vector: types.VectorValue{Param: &types.Param{Name: "vec1"}},
				Metadata: map[types.MetadataField]types.Param{
					{Name: "category"}: {Name: "cat1"},
				},
			},
		},
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"objects"`) {
		t.Errorf("expected objects in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"class":"Products"`) {
		t.Errorf("expected class:Products in JSON: %s", result.JSON)
	}
}

func TestRenderDelete(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation: types.OpDelete,
		Target:    types.Collection{Name: "products"},
		IDs: []types.Param{
			{Name: "id1"},
			{Name: "id2"},
		},
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"class":"Products"`) {
		t.Errorf("expected class:Products in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"ids"`) {
		t.Errorf("expected ids in JSON: %s", result.JSON)
	}
}

func TestRenderDeleteWithFilter(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation: types.OpDelete,
		Target:    types.Collection{Name: "products"},
		FilterClause: types.FilterCondition{
			Field:    types.MetadataField{Name: "category"},
			Operator: types.EQ,
			Value:    types.Param{Name: "cat"},
		},
		DeleteAll: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"where"`) {
		t.Errorf("expected where in JSON: %s", result.JSON)
	}
}

func TestRenderFetch(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation:      types.OpFetch,
		Target:         types.Collection{Name: "products"},
		IDs:            []types.Param{{Name: "id1"}},
		IncludeVectors: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"class":"Products"`) {
		t.Errorf("expected class:Products in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"ids"`) {
		t.Errorf("expected ids in JSON: %s", result.JSON)
	}
}

func TestRenderUpdate(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation: types.OpUpdate,
		Target:    types.Collection{Name: "products"},
		IDs:       []types.Param{{Name: "id1"}},
		Updates: map[types.MetadataField]types.Param{
			{Name: "category"}: {Name: "new_cat"},
		},
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"class":"Products"`) {
		t.Errorf("expected class:Products in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"properties"`) {
		t.Errorf("expected properties in JSON: %s", result.JSON)
	}
}

func TestSupportsOperation(t *testing.T) {
	renderer := New()

	supportedOps := []types.Operation{
		types.OpSearch,
		types.OpUpsert,
		types.OpDelete,
		types.OpFetch,
		types.OpUpdate,
	}

	for _, op := range supportedOps {
		if !renderer.SupportsOperation(op) {
			t.Errorf("expected %s to be supported", op)
		}
	}
}

func TestSupportsFilter(t *testing.T) {
	renderer := New()

	supportedFilters := []types.FilterOperator{
		types.EQ,
		types.NE,
		types.GT,
		types.GE,
		types.LT,
		types.LE,
		types.Contains,
	}

	for _, op := range supportedFilters {
		if !renderer.SupportsFilter(op) {
			t.Errorf("expected %s to be supported", op)
		}
	}
}

func TestSupportsMetric(t *testing.T) {
	renderer := New()

	supportedMetrics := []types.DistanceMetric{
		types.Cosine,
		types.Euclidean,
		types.DotProduct,
	}

	for _, metric := range supportedMetrics {
		if !renderer.SupportsMetric(metric) {
			t.Errorf("expected %s to be supported", metric)
		}
	}
}

func TestOperatorMapping(t *testing.T) {
	renderer := New()

	tests := []struct {
		op       types.FilterOperator
		expected string
	}{
		{types.EQ, "Equal"},
		{types.NE, "NotEqual"},
		{types.GT, "GreaterThan"},
		{types.GE, "GreaterThanEqual"},
		{types.LT, "LessThan"},
		{types.LE, "LessThanEqual"},
		{types.Contains, "ContainsAny"},
	}

	for _, tt := range tests {
		t.Run(string(tt.op), func(t *testing.T) {
			result := renderer.mapOperator(tt.op)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestLogicMapping(t *testing.T) {
	renderer := New()

	tests := []struct {
		logic    types.LogicOperator
		expected string
	}{
		{types.AND, "And"},
		{types.OR, "Or"},
	}

	for _, tt := range tests {
		t.Run(string(tt.logic), func(t *testing.T) {
			result := renderer.mapLogic(tt.logic)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestFormatClassName(t *testing.T) {
	renderer := New()

	tests := []struct {
		input    string
		expected string
	}{
		{"products", "Products"},
		{"Products", "Products"},
		{"my_collection", "My_collection"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := renderer.formatClassName(tt.input)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

package milvus

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

	if !strings.Contains(result.JSON, `"collection_name":"products"`) {
		t.Errorf("expected collection_name in JSON: %s", result.JSON)
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

	if !strings.Contains(result.JSON, `"filter"`) {
		t.Errorf("expected filter in JSON: %s", result.JSON)
	}
}

func TestRenderSearchWithOutputFields(t *testing.T) {
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
		MetadataFields: []types.MetadataField{
			{Name: "category"},
			{Name: "price"},
		},
		IncludeMetadata: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"output_fields"`) {
		t.Errorf("expected output_fields in JSON: %s", result.JSON)
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

	if !strings.Contains(result.JSON, `"collection_name":"products"`) {
		t.Errorf("expected collection_name in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"data"`) {
		t.Errorf("expected data in JSON: %s", result.JSON)
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

	if !strings.Contains(result.JSON, `"collection_name":"products"`) {
		t.Errorf("expected collection_name in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"filter"`) {
		t.Errorf("expected filter in JSON: %s", result.JSON)
	}
}

func TestRenderFetch(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation:       types.OpFetch,
		Target:          types.Collection{Name: "products"},
		IDs:             []types.Param{{Name: "id1"}},
		IncludeMetadata: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(result.JSON, `"collection_name":"products"`) {
		t.Errorf("expected collection_name in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"filter"`) {
		t.Errorf("expected filter in JSON: %s", result.JSON)
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

	if !strings.Contains(result.JSON, `"collection_name":"products"`) {
		t.Errorf("expected collection_name in JSON: %s", result.JSON)
	}
	if !strings.Contains(result.JSON, `"data"`) {
		t.Errorf("expected data in JSON: %s", result.JSON)
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
		types.IN,
		types.NotIn,
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
		{types.EQ, "=="},
		{types.NE, "!="},
		{types.GT, ">"},
		{types.GE, ">="},
		{types.LT, "<"},
		{types.LE, "<="},
		{types.IN, "in"},
		{types.NotIn, "not in"},
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

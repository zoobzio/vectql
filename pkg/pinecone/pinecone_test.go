package pinecone

import (
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

	expected := `{"includeMetadata":true,"includeValues":false,"topK":10,"vector":":query_vec"}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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
		IncludeMetadata: true,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `{"filter":{"category":{"$eq":":cat"}},"includeMetadata":true,"includeValues":false,"topK":10,"vector":":query_vec"}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
	}
}

func TestRenderSearchWithNamespace(t *testing.T) {
	renderer := New()

	topK := 10
	ns := types.Param{Name: "ns"}
	ast := &types.VectorAST{
		Operation: types.OpSearch,
		Target:    types.Collection{Name: "products"},
		QueryVector: &types.VectorValue{
			Param: &types.Param{Name: "query_vec"},
		},
		TopK: &types.PaginationValue{
			Static: &topK,
		},
		Namespace: &ns,
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `{"includeMetadata":false,"includeValues":false,"namespace":":ns","topK":10,"vector":":query_vec"}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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

	expected := `{"vectors":[{"id":":id1","metadata":{"category":":cat1"},"values":":vec1"}]}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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

	expected := `{"ids":[":id1",":id2"]}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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

	expected := `{"deleteAll":false,"filter":{"category":{"$eq":":cat"}}}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
	}
}

func TestRenderFetch(t *testing.T) {
	renderer := New()

	ast := &types.VectorAST{
		Operation: types.OpFetch,
		Target:    types.Collection{Name: "products"},
		IDs:       []types.Param{{Name: "id1"}},
	}

	result, err := renderer.Render(ast)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `{"ids":[":id1"]}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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

	expected := `{"id":":id1","setMetadata":{"category":":new_cat"}}`
	if result.JSON != expected {
		t.Errorf("expected:\n%s\ngot:\n%s", expected, result.JSON)
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

func TestFilterOperatorMapping(t *testing.T) {
	renderer := New()

	tests := []struct {
		op       types.FilterOperator
		expected string
	}{
		{types.EQ, "$eq"},
		{types.NE, "$ne"},
		{types.GT, "$gt"},
		{types.GE, "$gte"},
		{types.LT, "$lt"},
		{types.LE, "$lte"},
		{types.IN, "$in"},
		{types.NotIn, "$nin"},
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

func TestLogicOperatorMapping(t *testing.T) {
	renderer := New()

	tests := []struct {
		logic    types.LogicOperator
		expected string
	}{
		{types.AND, "$and"},
		{types.OR, "$or"},
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

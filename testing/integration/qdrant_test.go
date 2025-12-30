package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/vectql"
	"github.com/zoobzio/vectql/pkg/qdrant"
)

func TestQdrant_SimpleSearch(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render search: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}

	if !strings.Contains(result.JSON, "products") {
		t.Error("Expected collection name in result")
	}
}

func TestQdrant_SearchWithFilter(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(instance.Eq(instance.M("products", "category"), instance.P("category"))).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render search with filter: %v", err)
	}

	if !strings.Contains(result.JSON, "filter") {
		t.Error("Expected filter in result")
	}

	// Check params include both query_vec and category
	if len(result.RequiredParams) < 2 {
		t.Errorf("Expected at least 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

func TestQdrant_SearchWithComplexFilter(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(instance.And(
			instance.Eq(instance.M("products", "active"), instance.P("active")),
			instance.Or(
				instance.Gt(instance.M("products", "price"), instance.P("min_price")),
				instance.Eq(instance.M("products", "category"), instance.P("category")),
			),
		)).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render search with complex filter: %v", err)
	}

	// Qdrant uses must/should for AND/OR
	if !strings.Contains(result.JSON, "must") {
		t.Error("Expected 'must' (AND) in result")
	}
	if !strings.Contains(result.JSON, "should") {
		t.Error("Expected 'should' (OR) in result")
	}
}

func TestQdrant_Upsert(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "name"), instance.P("name")).
		WithMetadata(instance.M("products", "category"), instance.P("category")).
		WithMetadata(instance.M("products", "price"), instance.P("price"))

	result, err := vectql.Upsert(instance.C("products")).
		AddVector(record.Build()).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render upsert: %v", err)
	}

	if !strings.Contains(result.JSON, "points") {
		t.Error("Expected 'points' in upsert result")
	}
}

func TestQdrant_Delete(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Delete(instance.C("products")).
		IDs(instance.P("ids")).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestQdrant_DeleteWithFilter(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Delete(instance.C("products")).
		Filter(instance.Eq(instance.M("products", "active"), instance.P("active"))).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render delete with filter: %v", err)
	}

	if !strings.Contains(result.JSON, "filter") {
		t.Error("Expected filter in delete result")
	}
}

func TestQdrant_Fetch(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	result, err := vectql.Fetch(instance.C("products")).
		IDs(instance.P("ids")).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render fetch: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestQdrant_RangeFilter(t *testing.T) {
	_ = setupQdrant(t)
	instance := createTestInstance(t)

	minPrice := instance.P("min_price")
	maxPrice := instance.P("max_price")

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(vectql.Range(instance.M("products", "price"), &minPrice, &maxPrice)).
		Render(qdrant.New())

	if err != nil {
		t.Fatalf("Failed to render search with range filter: %v", err)
	}

	// Qdrant uses range conditions
	if !strings.Contains(result.JSON, "range") {
		t.Error("Expected 'range' in filter result")
	}
}

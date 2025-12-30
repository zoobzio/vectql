package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/vectql"
	"github.com/zoobzio/vectql/pkg/weaviate"
)

func TestWeaviate_SimpleSearch(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render search: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}

	// Weaviate uses class and nearVector
	if !strings.Contains(result.JSON, "class") {
		t.Error("Expected 'class' in result")
	}
	if !strings.Contains(result.JSON, "nearVector") {
		t.Error("Expected 'nearVector' in result")
	}
}

func TestWeaviate_SearchWithFilter(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(instance.Eq(instance.M("products", "category"), instance.P("category"))).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render search with filter: %v", err)
	}

	// Weaviate uses where clause
	if !strings.Contains(result.JSON, "where") {
		t.Error("Expected 'where' in result")
	}

	if len(result.RequiredParams) < 2 {
		t.Errorf("Expected at least 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

func TestWeaviate_SearchWithComplexFilter(t *testing.T) {
	_ = setupWeaviate(t)
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
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render search with complex filter: %v", err)
	}

	// Weaviate uses And/Or operators
	if !strings.Contains(result.JSON, "And") && !strings.Contains(result.JSON, "Or") {
		t.Error("Expected 'And' or 'Or' operators in result")
	}
}

func TestWeaviate_Upsert(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "name"), instance.P("name")).
		WithMetadata(instance.M("products", "category"), instance.P("category")).
		WithMetadata(instance.M("products", "price"), instance.P("price"))

	result, err := vectql.Upsert(instance.C("products")).
		AddVector(record.Build()).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render upsert: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestWeaviate_Delete(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	result, err := vectql.Delete(instance.C("products")).
		IDs(instance.P("ids")).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestWeaviate_Fetch(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	result, err := vectql.Fetch(instance.C("products")).
		IDs(instance.P("ids")).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render fetch: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestWeaviate_RangeFilter(t *testing.T) {
	_ = setupWeaviate(t)
	instance := createTestInstance(t)

	minPrice := instance.P("min_price")
	maxPrice := instance.P("max_price")

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(vectql.Range(instance.M("products", "price"), &minPrice, &maxPrice)).
		Render(weaviate.New())

	if err != nil {
		t.Fatalf("Failed to render search with range filter: %v", err)
	}

	// Weaviate uses GreaterThan/LessThan operators
	if !strings.Contains(result.JSON, "GreaterThan") && !strings.Contains(result.JSON, "LessThan") {
		t.Error("Expected range operators in filter result")
	}
}

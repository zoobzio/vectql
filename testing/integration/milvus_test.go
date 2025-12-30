package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/vectql"
	"github.com/zoobzio/vectql/pkg/milvus"
)

func TestMilvus_SimpleSearch(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Render(milvus.New())

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

func TestMilvus_SearchWithFilter(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(instance.Eq(instance.M("products", "category"), instance.P("category"))).
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render search with filter: %v", err)
	}

	if !strings.Contains(result.JSON, "filter") {
		t.Error("Expected filter in result")
	}

	if len(result.RequiredParams) < 2 {
		t.Errorf("Expected at least 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

func TestMilvus_SearchWithComplexFilter(t *testing.T) {
	_ = setupMilvus(t)
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
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render search with complex filter: %v", err)
	}

	// Milvus uses boolean expressions with and/or
	if !strings.Contains(result.JSON, "and") && !strings.Contains(result.JSON, "or") {
		t.Error("Expected 'and' or 'or' operators in result")
	}
}

func TestMilvus_Upsert(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "name"), instance.P("name")).
		WithMetadata(instance.M("products", "category"), instance.P("category")).
		WithMetadata(instance.M("products", "price"), instance.P("price"))

	result, err := vectql.Upsert(instance.C("products")).
		AddVector(record.Build()).
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render upsert: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestMilvus_Delete(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	result, err := vectql.Delete(instance.C("products")).
		IDs(instance.P("ids")).
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestMilvus_Fetch(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	result, err := vectql.Fetch(instance.C("products")).
		IDs(instance.P("ids")).
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render fetch: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestMilvus_RangeFilter(t *testing.T) {
	_ = setupMilvus(t)
	instance := createTestInstance(t)

	minPrice := instance.P("min_price")
	maxPrice := instance.P("max_price")

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(vectql.Range(instance.M("products", "price"), &minPrice, &maxPrice)).
		Render(milvus.New())

	if err != nil {
		t.Fatalf("Failed to render search with range filter: %v", err)
	}

	// Milvus uses comparison operators for ranges in filter expression
	if !strings.Contains(result.JSON, "filter") {
		t.Error("Expected 'filter' in result")
	}
	if !strings.Contains(result.JSON, "price") {
		t.Error("Expected 'price' field in filter result")
	}
}

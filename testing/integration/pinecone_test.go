package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/vectql"
	"github.com/zoobzio/vectql/pkg/pinecone"
)

// Note: Pinecone is a cloud-only service with no local container option.
// These tests validate query rendering without requiring a live connection.
// For actual integration testing, set PINECONE_API_KEY and PINECONE_INDEX environment variables.

func TestPinecone_SimpleSearch(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render search: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}

	if !strings.Contains(result.JSON, "topK") {
		t.Error("Expected 'topK' in result")
	}
}

func TestPinecone_SearchWithFilter(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(instance.Eq(instance.M("products", "category"), instance.P("category"))).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render search with filter: %v", err)
	}

	if !strings.Contains(result.JSON, "filter") {
		t.Error("Expected 'filter' in result")
	}

	// Pinecone uses $eq operator
	if !strings.Contains(result.JSON, "$eq") {
		t.Error("Expected '$eq' operator in filter")
	}

	if len(result.RequiredParams) < 2 {
		t.Errorf("Expected at least 2 params, got %d: %v", len(result.RequiredParams), result.RequiredParams)
	}
}

func TestPinecone_SearchWithComplexFilter(t *testing.T) {
	skipIfNoPinecone(t)
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
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render search with complex filter: %v", err)
	}

	// Pinecone uses $and/$or operators
	if !strings.Contains(result.JSON, "$and") {
		t.Error("Expected '$and' operator in result")
	}
	if !strings.Contains(result.JSON, "$or") {
		t.Error("Expected '$or' operator in result")
	}
}

func TestPinecone_Upsert(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "name"), instance.P("name")).
		WithMetadata(instance.M("products", "category"), instance.P("category")).
		WithMetadata(instance.M("products", "price"), instance.P("price"))

	result, err := vectql.Upsert(instance.C("products")).
		AddVector(record.Build()).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render upsert: %v", err)
	}

	if !strings.Contains(result.JSON, "vectors") {
		t.Error("Expected 'vectors' in upsert result")
	}
}

func TestPinecone_Delete(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	result, err := vectql.Delete(instance.C("products")).
		IDs(instance.P("ids")).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	if !strings.Contains(result.JSON, "ids") {
		t.Error("Expected 'ids' in delete result")
	}
}

func TestPinecone_Fetch(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	result, err := vectql.Fetch(instance.C("products")).
		IDs(instance.P("ids")).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render fetch: %v", err)
	}

	if !strings.Contains(result.JSON, "ids") {
		t.Error("Expected 'ids' in fetch result")
	}
}

func TestPinecone_Namespace(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Namespace(instance.P("namespace")).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render search with namespace: %v", err)
	}

	if !strings.Contains(result.JSON, "namespace") {
		t.Error("Expected 'namespace' in result")
	}
}

func TestPinecone_RangeFilter(t *testing.T) {
	skipIfNoPinecone(t)
	instance := createTestInstance(t)

	minPrice := instance.P("min_price")
	maxPrice := instance.P("max_price")

	result, err := vectql.Search(instance.C("products")).
		Vector(vectql.Vec(instance.P("query_vec"))).
		Embedding(instance.E("products", "embedding")).
		TopK(10).
		Filter(vectql.Range(instance.M("products", "price"), &minPrice, &maxPrice)).
		Render(pinecone.New())

	if err != nil {
		t.Fatalf("Failed to render search with range filter: %v", err)
	}

	// Pinecone uses $gte/$lte for ranges
	if !strings.Contains(result.JSON, "$gte") && !strings.Contains(result.JSON, "$lte") {
		t.Error("Expected range operators in filter result")
	}
}

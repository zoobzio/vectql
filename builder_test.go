package vectql

import (
	"testing"

	"github.com/zoobzio/vectql/internal/types"
)

func TestSearch(t *testing.T) {
	coll := types.Collection{Name: "products"}
	builder := Search(coll)

	if builder.ast.Operation != types.OpSearch {
		t.Errorf("expected OpSearch, got %s", builder.ast.Operation)
	}
	if builder.ast.Target.Name != "products" {
		t.Errorf("expected products, got %s", builder.ast.Target.Name)
	}
}

func TestSearch_Vector(t *testing.T) {
	coll := types.Collection{Name: "products"}
	vec := Vec(types.Param{Name: "query_vec"})

	ast, err := Search(coll).
		Vector(vec).
		TopK(10).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.QueryVector == nil {
		t.Fatal("expected QueryVector to be set")
	}
	if ast.QueryVector.Param.Name != "query_vec" {
		t.Errorf("expected query_vec, got %s", ast.QueryVector.Param.Name)
	}
}

func TestSearch_TopK(t *testing.T) {
	coll := types.Collection{Name: "products"}

	ast, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(20).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.TopK == nil || ast.TopK.Static == nil {
		t.Fatal("expected TopK to be set")
	}
	if *ast.TopK.Static != 20 {
		t.Errorf("expected 20, got %d", *ast.TopK.Static)
	}
}

func TestSearch_TopKExceedsMax(t *testing.T) {
	coll := types.Collection{Name: "products"}

	_, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(types.MaxTopK + 1).
		Build()

	if err == nil {
		t.Fatal("expected error for TopK exceeding max")
	}
}

func TestSearch_Filter(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	ast, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(10).
		Filter(Eq(category, types.Param{Name: "cat"})).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.FilterClause == nil {
		t.Fatal("expected FilterClause to be set")
	}
}

func TestSearch_MultipleFilters(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}
	price := types.MetadataField{Name: "price"}

	ast, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(10).
		Filter(Eq(category, types.Param{Name: "cat"})).
		Filter(Lte(price, types.Param{Name: "max_price"})).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Multiple filters should be combined with AND
	group, ok := ast.FilterClause.(types.FilterGroup)
	if !ok {
		t.Fatal("expected FilterGroup for multiple filters")
	}
	if group.Logic != types.AND {
		t.Errorf("expected AND logic, got %s", group.Logic)
	}
}

func TestSearch_RequiresVector(t *testing.T) {
	coll := types.Collection{Name: "products"}

	_, err := Search(coll).
		TopK(10).
		Build()

	if err == nil {
		t.Fatal("expected error for missing vector")
	}
}

func TestSearch_RequiresTopK(t *testing.T) {
	coll := types.Collection{Name: "products"}

	_, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		Build()

	if err == nil {
		t.Fatal("expected error for missing TopK")
	}
}

func TestUpsert(t *testing.T) {
	coll := types.Collection{Name: "products"}
	builder := Upsert(coll)

	if builder.ast.Operation != types.OpUpsert {
		t.Errorf("expected OpUpsert, got %s", builder.ast.Operation)
	}
}

func TestUpsert_AddVector(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	record := NewRecord(types.Param{Name: "id1"}, Vec(types.Param{Name: "vec1"})).
		WithMetadata(category, types.Param{Name: "cat1"}).
		Build()

	ast, err := Upsert(coll).
		AddVector(record).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ast.Vectors) != 1 {
		t.Errorf("expected 1 vector, got %d", len(ast.Vectors))
	}
}

func TestUpsert_BatchSize(t *testing.T) {
	coll := types.Collection{Name: "products"}

	builder := Upsert(coll)
	for i := 0; i < types.MaxBatchSize+1; i++ {
		builder = builder.AddVector(NewRecord(types.Param{Name: "id"}, Vec(types.Param{Name: "vec"})).Build())
	}

	_, err := builder.Build()
	if err == nil {
		t.Fatal("expected error for exceeding batch size")
	}
}

func TestDelete_ByIDs(t *testing.T) {
	coll := types.Collection{Name: "products"}

	ast, err := Delete(coll).
		IDs(types.Param{Name: "id1"}, types.Param{Name: "id2"}).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ast.IDs) != 2 {
		t.Errorf("expected 2 IDs, got %d", len(ast.IDs))
	}
}

func TestDelete_ByFilter(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	ast, err := Delete(coll).
		Filter(Eq(category, types.Param{Name: "cat"})).
		DeleteAll().
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.FilterClause == nil {
		t.Fatal("expected FilterClause to be set")
	}
	if !ast.DeleteAll {
		t.Fatal("expected DeleteAll to be true")
	}
}

func TestDelete_FilterRequiresDeleteAll(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	_, err := Delete(coll).
		Filter(Eq(category, types.Param{Name: "cat"})).
		Build()

	if err == nil {
		t.Fatal("expected error for filter without DeleteAll")
	}
}

func TestFetch(t *testing.T) {
	coll := types.Collection{Name: "products"}

	ast, err := Fetch(coll).
		IDs(types.Param{Name: "id1"}, types.Param{Name: "id2"}).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ast.IDs) != 2 {
		t.Errorf("expected 2 IDs, got %d", len(ast.IDs))
	}
}

func TestFetch_RequiresIDs(t *testing.T) {
	coll := types.Collection{Name: "products"}

	_, err := Fetch(coll).Build()

	if err == nil {
		t.Fatal("expected error for missing IDs")
	}
}

func TestUpdate(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	ast, err := Update(coll).
		IDs(types.Param{Name: "id1"}).
		Set(category, types.Param{Name: "new_cat"}).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ast.IDs) != 1 {
		t.Errorf("expected 1 ID, got %d", len(ast.IDs))
	}
	if len(ast.Updates) != 1 {
		t.Errorf("expected 1 update, got %d", len(ast.Updates))
	}
}

func TestUpdate_RequiresIDs(t *testing.T) {
	coll := types.Collection{Name: "products"}
	category := types.MetadataField{Name: "category"}

	_, err := Update(coll).
		Set(category, types.Param{Name: "new_cat"}).
		Build()

	if err == nil {
		t.Fatal("expected error for missing IDs")
	}
}

func TestUpdate_RequiresUpdates(t *testing.T) {
	coll := types.Collection{Name: "products"}

	_, err := Update(coll).
		IDs(types.Param{Name: "id1"}).
		Build()

	if err == nil {
		t.Fatal("expected error for missing updates")
	}
}

func TestOperationMismatch(t *testing.T) {
	coll := types.Collection{Name: "products"}

	// Vector() on non-Search
	_, err := Upsert(coll).Vector(Vec(types.Param{Name: "v"})).Build()
	if err == nil {
		t.Error("expected error for Vector() on Upsert")
	}

	// TopK() on non-Search
	_, err = Delete(coll).TopK(10).Build()
	if err == nil {
		t.Error("expected error for TopK() on Delete")
	}

	// AddVector() on non-Upsert
	_, err = Search(coll).AddVector(NewRecord(types.Param{Name: "id"}, Vec(types.Param{Name: "v"})).Build()).Build()
	if err == nil {
		t.Error("expected error for AddVector() on Search")
	}

	// Set() on non-Update
	_, err = Search(coll).Set(types.MetadataField{Name: "cat"}, types.Param{Name: "v"}).Build()
	if err == nil {
		t.Error("expected error for Set() on Search")
	}
}

func TestNamespace(t *testing.T) {
	coll := types.Collection{Name: "products"}

	ast, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(10).
		Namespace(types.Param{Name: "ns"}).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Namespace == nil {
		t.Fatal("expected Namespace to be set")
	}
	if ast.Namespace.Name != "ns" {
		t.Errorf("expected ns, got %s", ast.Namespace.Name)
	}
}

func TestIncludeOptions(t *testing.T) {
	coll := types.Collection{Name: "products"}

	ast, err := Search(coll).
		Vector(Vec(types.Param{Name: "v"})).
		TopK(10).
		IncludeVectors(true).
		IncludeMetadata(false).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ast.IncludeVectors {
		t.Error("expected IncludeVectors to be true")
	}
	if ast.IncludeMetadata {
		t.Error("expected IncludeMetadata to be false")
	}
}

// Package benchmarks provides performance benchmarks for vectql.
package benchmarks

import (
	"testing"

	"github.com/zoobzio/vdml"
	"github.com/zoobzio/vectql"
	"github.com/zoobzio/vectql/pkg/milvus"
	"github.com/zoobzio/vectql/pkg/pinecone"
	"github.com/zoobzio/vectql/pkg/qdrant"
	"github.com/zoobzio/vectql/pkg/weaviate"
)

func createBenchmarkInstance(b *testing.B) *vectql.VECTQL {
	b.Helper()

	schema := vdml.NewSchema("bench")

	products := vdml.NewCollection("products")
	products.AddEmbedding(vdml.NewEmbedding("embedding", 1536).WithMetric(vdml.Cosine))
	products.AddMetadata(vdml.NewMetadataField("category", vdml.TypeString))
	products.AddMetadata(vdml.NewMetadataField("price", vdml.TypeFloat))
	products.AddMetadata(vdml.NewMetadataField("stock", vdml.TypeInt))
	products.AddMetadata(vdml.NewMetadataField("active", vdml.TypeBool))
	schema.AddCollection(products)

	documents := vdml.NewCollection("documents")
	documents.AddEmbedding(vdml.NewEmbedding("content", 768).WithMetric(vdml.DotProduct))
	documents.AddMetadata(vdml.NewMetadataField("source", vdml.TypeString))
	documents.AddMetadata(vdml.NewMetadataField("page", vdml.TypeInt))
	schema.AddCollection(documents)

	instance, err := vectql.NewFromVDML(schema)
	if err != nil {
		b.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

// Pinecone Benchmarks

func BenchmarkPinecone_SimpleSearch(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Render(pinecone.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPinecone_SearchWithFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))
	filter := instance.Eq(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Filter(filter).
			Render(pinecone.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPinecone_SearchWithComplexFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
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
			b.Fatal(err)
		}
	}
}

func BenchmarkPinecone_Upsert(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "category"), instance.P("category")).
		WithMetadata(instance.M("products", "price"), instance.P("price"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Upsert(collection).
			AddVector(record.Build()).
			Render(pinecone.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPinecone_Delete(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Delete(collection).
			IDs(instance.P("ids")).
			Render(pinecone.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPinecone_Fetch(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Fetch(collection).
			IDs(instance.P("ids")).
			Render(pinecone.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Qdrant Benchmarks

func BenchmarkQdrant_SimpleSearch(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Render(qdrant.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQdrant_SearchWithFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))
	filter := instance.Eq(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Filter(filter).
			Render(qdrant.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQdrant_Upsert(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Upsert(collection).
			AddVector(record.Build()).
			Render(qdrant.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Milvus Benchmarks

func BenchmarkMilvus_SimpleSearch(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Render(milvus.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMilvus_SearchWithFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))
	filter := instance.Eq(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Filter(filter).
			Render(milvus.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMilvus_Upsert(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Upsert(collection).
			AddVector(record.Build()).
			Render(milvus.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Weaviate Benchmarks

func BenchmarkWeaviate_SimpleSearch(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Render(weaviate.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWeaviate_SearchWithFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")
	embedding := instance.E("products", "embedding")
	vec := vectql.Vec(instance.P("query_vec"))
	filter := instance.Eq(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Search(collection).
			Vector(vec).
			Embedding(embedding).
			TopK(10).
			Filter(filter).
			Render(weaviate.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWeaviate_Upsert(b *testing.B) {
	instance := createBenchmarkInstance(b)
	collection := instance.C("products")

	record := vectql.NewRecord(instance.P("id"), vectql.Vec(instance.P("vec"))).
		WithMetadata(instance.M("products", "category"), instance.P("category"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := vectql.Upsert(collection).
			AddVector(record.Build()).
			Render(weaviate.New())
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Component Creation Benchmarks

func BenchmarkCreateCollection(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.C("products")
	}
}

func BenchmarkCreateEmbeddingField(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.E("products", "embedding")
	}
}

func BenchmarkCreateMetadataField(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.M("products", "category")
	}
}

func BenchmarkCreateParam(b *testing.B) {
	instance := createBenchmarkInstance(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.P("query_vec")
	}
}

func BenchmarkCreateFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	field := instance.M("products", "category")
	param := instance.P("category")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.Eq(field, param)
	}
}

func BenchmarkCreateComplexFilter(b *testing.B) {
	instance := createBenchmarkInstance(b)
	categoryField := instance.M("products", "category")
	priceField := instance.M("products", "price")
	activeField := instance.M("products", "active")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = instance.And(
			instance.Eq(categoryField, instance.P("category")),
			instance.Or(
				instance.Gt(priceField, instance.P("min_price")),
				instance.Eq(activeField, instance.P("active")),
			),
		)
	}
}

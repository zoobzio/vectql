// Package integration provides integration tests for vectql providers.
//
// These tests require running vector database instances.
// Use testcontainers for local testing where Docker images are available.
//
// Run with: go test -v ./testing/integration/...
// Skip with: go test -short ./...
package integration

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/milvus"
	"github.com/testcontainers/testcontainers-go/modules/qdrant"
	"github.com/testcontainers/testcontainers-go/modules/weaviate"
	"github.com/zoobzio/vdml"
	"github.com/zoobzio/vectql"
)

var (
	qdrantContainer   testcontainers.Container
	qdrantOnce        sync.Once
	qdrantEndpoint    string
	milvusContainer   testcontainers.Container
	milvusOnce        sync.Once
	milvusEndpoint    string
	weaviateContainer testcontainers.Container
	weaviateOnce      sync.Once
	weaviateEndpoint  string
)

// setupQdrant starts a Qdrant container for integration tests.
func setupQdrant(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	qdrantOnce.Do(func() {
		ctx := context.Background()
		container, err := qdrant.Run(ctx, "qdrant/qdrant:latest")
		if err != nil {
			t.Fatalf("Failed to start Qdrant container: %v", err)
		}

		endpoint, err := container.RESTEndpoint(ctx)
		if err != nil {
			t.Fatalf("Failed to get Qdrant endpoint: %v", err)
		}

		qdrantContainer = container
		qdrantEndpoint = endpoint
	})

	if qdrantEndpoint == "" {
		t.Fatal("Qdrant endpoint not available")
	}

	return qdrantEndpoint
}

// setupMilvus starts a Milvus container for integration tests.
func setupMilvus(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	milvusOnce.Do(func() {
		ctx := context.Background()
		container, err := milvus.Run(ctx, "milvusdb/milvus:latest")
		if err != nil {
			t.Fatalf("Failed to start Milvus container: %v", err)
		}

		endpoint, err := container.ConnectionString(ctx)
		if err != nil {
			t.Fatalf("Failed to get Milvus endpoint: %v", err)
		}

		milvusContainer = container
		milvusEndpoint = endpoint
	})

	if milvusEndpoint == "" {
		t.Fatal("Milvus endpoint not available")
	}

	return milvusEndpoint
}

// setupWeaviate starts a Weaviate container for integration tests.
func setupWeaviate(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	weaviateOnce.Do(func() {
		ctx := context.Background()
		container, err := weaviate.Run(ctx, "semitechnologies/weaviate:latest")
		if err != nil {
			t.Fatalf("Failed to start Weaviate container: %v", err)
		}

		host, port, err := container.HttpHostAddress(ctx)
		if err != nil {
			t.Fatalf("Failed to get Weaviate endpoint: %v", err)
		}

		weaviateContainer = container
		weaviateEndpoint = "http://" + host + ":" + port
	})

	if weaviateEndpoint == "" {
		t.Fatal("Weaviate endpoint not available")
	}

	return weaviateEndpoint
}

// skipIfNoPinecone skips the test if Pinecone credentials are not configured.
// Pinecone is a cloud-only service with no local container option.
func skipIfNoPinecone(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Pinecone requires API key - if not set, run as render-only test
	if os.Getenv("PINECONE_API_KEY") == "" {
		// Still run the test but without actual API calls
		// These tests validate query rendering
		return
	}
}

// createTestInstance creates a VECTQL instance for testing.
func createTestInstance(t *testing.T) *vectql.VECTQL {
	t.Helper()

	schema := vdml.NewSchema("test")

	// Products collection
	products := vdml.NewCollection("products")
	products.AddEmbedding(vdml.NewEmbedding("embedding", 4).WithMetric(vdml.Cosine))
	products.AddMetadata(vdml.NewMetadataField("name", vdml.TypeString))
	products.AddMetadata(vdml.NewMetadataField("category", vdml.TypeString))
	products.AddMetadata(vdml.NewMetadataField("price", vdml.TypeFloat))
	products.AddMetadata(vdml.NewMetadataField("active", vdml.TypeBool))
	schema.AddCollection(products)

	instance, err := vectql.NewFromVDML(schema)
	if err != nil {
		t.Fatalf("Failed to create test instance: %v", err)
	}
	return instance
}

// CleanupContainers terminates all test containers.
// Call this from TestMain if needed.
func CleanupContainers() {
	ctx := context.Background()
	if qdrantContainer != nil {
		_ = qdrantContainer.Terminate(ctx)
	}
	if milvusContainer != nil {
		_ = milvusContainer.Terminate(ctx)
	}
	if weaviateContainer != nil {
		_ = weaviateContainer.Terminate(ctx)
	}
}

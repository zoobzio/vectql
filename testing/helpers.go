// Package testing provides test utilities for vectql.
package testing

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/zoobzio/vdml"
	"github.com/zoobzio/vectql"
)

// TestInstance creates a fully-featured VECTQL instance for testing.
// Includes products and documents collections with embeddings and metadata.
func TestInstance(t *testing.T) *vectql.VECTQL {
	t.Helper()

	schema := vdml.NewSchema("test")

	// Products collection - e-commerce style
	products := vdml.NewCollection("products")
	products.AddEmbedding(vdml.NewEmbedding("embedding", 1536).WithMetric(vdml.Cosine))
	products.AddMetadata(vdml.NewMetadataField("name", vdml.TypeString))
	products.AddMetadata(vdml.NewMetadataField("category", vdml.TypeString))
	products.AddMetadata(vdml.NewMetadataField("price", vdml.TypeFloat))
	products.AddMetadata(vdml.NewMetadataField("stock", vdml.TypeInt))
	products.AddMetadata(vdml.NewMetadataField("active", vdml.TypeBool))
	products.AddMetadata(vdml.NewMetadataField("tags", vdml.TypeStringArray))
	schema.AddCollection(products)

	// Documents collection - RAG style
	documents := vdml.NewCollection("documents")
	documents.AddEmbedding(vdml.NewEmbedding("content_embedding", 768).WithMetric(vdml.DotProduct))
	documents.AddMetadata(vdml.NewMetadataField("title", vdml.TypeString))
	documents.AddMetadata(vdml.NewMetadataField("source", vdml.TypeString))
	documents.AddMetadata(vdml.NewMetadataField("page", vdml.TypeInt))
	documents.AddMetadata(vdml.NewMetadataField("created_at", vdml.TypeString))
	schema.AddCollection(documents)

	// Images collection - multimodal style
	images := vdml.NewCollection("images")
	images.AddEmbedding(vdml.NewEmbedding("clip_embedding", 512).WithMetric(vdml.Euclidean))
	images.AddMetadata(vdml.NewMetadataField("filename", vdml.TypeString))
	images.AddMetadata(vdml.NewMetadataField("width", vdml.TypeInt))
	images.AddMetadata(vdml.NewMetadataField("height", vdml.TypeInt))
	images.AddMetadata(vdml.NewMetadataField("format", vdml.TypeString))
	schema.AddCollection(images)

	instance, err := vectql.NewFromVDML(schema)
	if err != nil {
		t.Fatalf("Failed to create test instance: %v", err)
	}
	return instance
}

// AssertJSON compares expected and actual JSON strings.
func AssertJSON(t *testing.T, expected, actual string) {
	t.Helper()

	var expectedMap, actualMap map[string]interface{}

	if err := json.Unmarshal([]byte(expected), &expectedMap); err != nil {
		t.Fatalf("Failed to parse expected JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(actual), &actualMap); err != nil {
		t.Fatalf("Failed to parse actual JSON: %v\nActual: %s", err, actual)
	}

	expectedBytes, err := json.Marshal(expectedMap)
	if err != nil {
		t.Fatalf("Failed to marshal expected JSON: %v", err)
	}
	actualBytes, err := json.Marshal(actualMap)
	if err != nil {
		t.Fatalf("Failed to marshal actual JSON: %v", err)
	}

	if !bytes.Equal(expectedBytes, actualBytes) {
		t.Errorf("JSON mismatch:\nExpected: %s\nActual:   %s", expected, actual)
	}
}

// AssertJSONContains checks that actual JSON contains a key with expected value.
func AssertJSONContains(t *testing.T, actual string, key string, expectedValue interface{}) {
	t.Helper()

	var actualMap map[string]interface{}
	if err := json.Unmarshal([]byte(actual), &actualMap); err != nil {
		t.Fatalf("Failed to parse actual JSON: %v", err)
	}

	actualValue, ok := actualMap[key]
	if !ok {
		t.Errorf("Expected key %q not found in JSON: %s", key, actual)
		return
	}

	expectedJSON, err := json.Marshal(expectedValue)
	if err != nil {
		t.Fatalf("Failed to marshal expected value: %v", err)
	}
	actualJSON, err := json.Marshal(actualValue)
	if err != nil {
		t.Fatalf("Failed to marshal actual value: %v", err)
	}

	if !bytes.Equal(expectedJSON, actualJSON) {
		t.Errorf("Value mismatch for key %q:\nExpected: %s\nActual:   %s", key, expectedJSON, actualJSON)
	}
}

// AssertParams checks that the required params match expected values.
func AssertParams(t *testing.T, expected, actual []string) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("Param count mismatch: expected %d, got %d\nExpected: %v\nActual: %v",
			len(expected), len(actual), expected, actual)
		return
	}

	expectedMap := make(map[string]bool)
	for _, p := range expected {
		expectedMap[p] = true
	}

	for _, p := range actual {
		if !expectedMap[p] {
			t.Errorf("Unexpected param: %s\nExpected: %v\nActual: %v", p, expected, actual)
		}
	}
}

// AssertContainsParam checks that a specific param is in the list.
func AssertContainsParam(t *testing.T, params []string, param string) {
	t.Helper()
	for _, p := range params {
		if p == param {
			return
		}
	}
	t.Errorf("Expected param %q not found in %v", param, params)
}

// AssertNoError fails the test if err is not nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// AssertError fails the test if err is nil.
func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Expected error but got nil")
	}
}

// AssertErrorContains checks that error message contains substring.
func AssertErrorContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error containing %q but got nil", substr)
	}
	if !containsString(err.Error(), substr) {
		t.Errorf("Expected error containing %q, got: %v", substr, err)
	}
}

// AssertPanics verifies that a function panics.
func AssertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic but function completed normally")
		}
	}()
	fn()
}

// AssertPanicsWithMessage verifies that a function panics with a specific message.
func AssertPanicsWithMessage(t *testing.T, fn func(), substr string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Expected panic containing %q but function completed normally", substr)
			return
		}
		var msg string
		switch v := r.(type) {
		case error:
			msg = v.Error()
		case string:
			msg = v
		default:
			t.Errorf("Panic value is not string or error: %T", r)
			return
		}
		if !containsString(msg, substr) {
			t.Errorf("Expected panic containing %q, got: %s", substr, msg)
		}
	}()
	fn()
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || substr == "" ||
		(s != "" && substr != "" && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

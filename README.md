# vectql

[![CI](https://github.com/zoobzio/vectql/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobzio/vectql/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/zoobzio/vectql/branch/main/graph/badge.svg)](https://codecov.io/gh/zoobzio/vectql)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/vectql)](https://goreportcard.com/report/github.com/zoobzio/vectql)
[![CodeQL](https://github.com/zoobzio/vectql/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobzio/vectql/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/vectql.svg)](https://pkg.go.dev/github.com/zoobzio/vectql)
[![License](https://img.shields.io/github/license/zoobzio/vectql)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/vectql)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/vectql)](https://github.com/zoobzio/vectql/releases)

Type-safe query builder for vector databases with VDML schema validation.

Build queries as an AST, validate against your schema, render to provider-specific format.

## Build, Validate, Render

```go
// Build
query := vectql.Search(instance.C("products")).
    Vector(vectql.Vec(instance.P("query_vec"))).
    Embedding(instance.E("products", "embedding")).
    Filter(instance.Eq(instance.M("products", "category"), instance.P("category"))).
    TopK(10)

// Validate — C(), E(), M(), P() check against your VDML schema

// Render
result, _ := query.Render(pinecone.New())
// {"vector": ":query_vec", "topK": 10, "filter": {"category": {"$eq": ":category"}}}
```

Collections, embeddings, and metadata fields validated at construction. Values always parameterized. Use `TryC`, `TryE`, `TryM`, `TryP` for runtime validation with error returns.

## Install

```bash
go get github.com/zoobzio/vectql
go get github.com/zoobzio/vdml
```

Requires Go 1.24+.

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/zoobzio/vectql"
    "github.com/zoobzio/vectql/pkg/pinecone"
    "github.com/zoobzio/vdml"
)

func main() {
    // Define schema
    schema := vdml.NewSchema("ecommerce")
    products := vdml.NewCollection("products").
        AddEmbedding(vdml.NewEmbedding("embedding", 1536).WithMetric(vdml.Cosine)).
        AddMetadata(vdml.NewMetadata("name", vdml.String)).
        AddMetadata(vdml.NewMetadata("category", vdml.String)).
        AddMetadata(vdml.NewMetadata("price", vdml.Float))
    schema.AddCollection(products)

    // Create instance
    instance, err := vectql.NewFromVDML(schema)
    if err != nil {
        panic(err)
    }

    // Build and render
    result, err := vectql.Search(instance.C("products")).
        Vector(vectql.Vec(instance.P("query_vec"))).
        Embedding(instance.E("products", "embedding")).
        Filter(instance.Gte(instance.M("products", "price"), instance.P("min_price"))).
        TopK(10).
        IncludeMetadata(true).
        Render(pinecone.New())

    if err != nil {
        panic(err)
    }

    fmt.Println(result.JSON)
    fmt.Println(result.RequiredParams)
    // [query_vec min_price]
}
```

## Providers

Same AST, different vector databases:

```go
import (
    "github.com/zoobzio/vectql/pkg/pinecone"
    "github.com/zoobzio/vectql/pkg/qdrant"
    "github.com/zoobzio/vectql/pkg/milvus"
    "github.com/zoobzio/vectql/pkg/weaviate"
)

result, _ := query.Render(pinecone.New())   // Pinecone
result, _ := query.Render(qdrant.New())     // Qdrant
result, _ := query.Render(milvus.New())     // Milvus
result, _ := query.Render(weaviate.New())   // Weaviate
```

Each provider handles dialect differences — filter syntax, metadata format, distance metrics, sparse vector support.

## Why VECTQL?

- **Schema-validated** — `C("products")`, `E("products", "embedding")`, `M("products", "price")` checked against VDML at build time
- **Injection-resistant** — parameterized values, validated field names, no string concatenation
- **Multi-provider** — one query, four vector databases
- **Composable** — filters, namespaces, hybrid search, batch operations

## Documentation

- [Overview](docs/1.overview.md) — what vectql does and why

**Learn**
- [Quickstart](docs/2.learn/1.quickstart.md) — get started in minutes
- [Concepts](docs/2.learn/2.concepts.md) — collections, embeddings, metadata, parameters
- [Architecture](docs/2.learn/3.architecture.md) — AST structure, render pipeline, security layers

**Guides**
- [Schema Validation](docs/3.guides/1.schema-validation.md) — VDML integration and validation
- [Filters](docs/3.guides/2.filters.md) — metadata filtering patterns
- [Hybrid Search](docs/3.guides/3.hybrid-search.md) — dense + sparse vectors
- [Testing](docs/3.guides/4.testing.md) — testing patterns for query builders

**Cookbook**
- [Similarity Search](docs/4.cookbook/1.similarity-search.md) — semantic search patterns
- [Batch Operations](docs/4.cookbook/2.batch-operations.md) — bulk upsert and delete
- [Multi-Tenancy](docs/4.cookbook/3.multi-tenancy.md) — namespace isolation

**Reference**
- [API](docs/5.reference/1.api.md) — complete function documentation
- [Operators](docs/5.reference/2.operators.md) — filter operators and distance metrics

## Contributing

Contributions welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).

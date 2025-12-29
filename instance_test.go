package vectql

import (
	"testing"

	"github.com/zoobzio/vdml"
	"github.com/zoobzio/vectql/internal/types"
)

// testSchema creates a minimal VDML schema for testing.
func testSchema() *vdml.Schema {
	return &vdml.Schema{
		Collections: map[string]*vdml.Collection{
			"products": {
				Name: "products",
				Embeddings: []*vdml.Embedding{
					{Name: "description", Dimensions: 384, Metric: vdml.Cosine},
				},
				Metadata: []*vdml.MetadataField{
					{Name: "category", Type: vdml.TypeString},
					{Name: "price", Type: vdml.TypeFloat},
					{Name: "location", Type: vdml.TypeString},
				},
			},
		},
	}
}

func TestNewFromVDML(t *testing.T) {
	schema := testSchema()
	v, err := NewFromVDML(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil VECTQL instance")
	}
}

func TestNewFromVDML_NilSchema(t *testing.T) {
	_, err := NewFromVDML(nil)
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

// --- Injection Detection Tests ---

func TestIsValidIdentifier_ValidNames(t *testing.T) {
	validNames := []string{
		"query_vec",
		"id1",
		"Category",
		"_private",
		"a",
		"MyParam123",
	}
	for _, name := range validNames {
		if !isValidIdentifier(name) {
			t.Errorf("expected '%s' to be valid", name)
		}
	}
}

func TestIsValidIdentifier_InvalidNames(t *testing.T) {
	invalidNames := []string{
		"",
		"1startsWithNumber",
		"has space",
		"has-dash",
		"has.dot",
	}
	for _, name := range invalidNames {
		if isValidIdentifier(name) {
			t.Errorf("expected '%s' to be invalid", name)
		}
	}
}

func TestIsValidIdentifier_InjectionPatterns(t *testing.T) {
	injectionPatterns := []string{
		"name;drop",
		"name--comment",
		"name/*block*/",
		"name' OR '1",
		"name\" OR \"1",
		"name`backtick",
		"name\\escape",
	}
	for _, pattern := range injectionPatterns {
		if isValidIdentifier(pattern) {
			t.Errorf("expected injection pattern '%s' to be rejected", pattern)
		}
	}
}

func TestTryP_ValidatesIdentifier(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	// Valid
	p, err := v.TryP("valid_param")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if p.Name != "valid_param" {
		t.Errorf("expected name 'valid_param', got '%s'", p.Name)
	}

	// Invalid
	_, err = v.TryP("invalid param")
	if err == nil {
		t.Error("expected error for invalid param name")
	}
}

// --- Operator Accessor Tests ---

func TestOperatorAccessors(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	tests := []struct {
		name     string
		got      types.FilterOperator
		expected types.FilterOperator
	}{
		{"EQ", v.OpEQ(), types.EQ},
		{"NE", v.OpNE(), types.NE},
		{"GT", v.OpGT(), types.GT},
		{"GE", v.OpGE(), types.GE},
		{"LT", v.OpLT(), types.LT},
		{"LE", v.OpLE(), types.LE},
		{"IN", v.OpIN(), types.IN},
		{"NotIn", v.OpNotIn(), types.NotIn},
		{"Contains", v.OpContains(), types.Contains},
		{"StartsWith", v.OpStartsWith(), types.StartsWith},
		{"EndsWith", v.OpEndsWith(), types.EndsWith},
		{"Matches", v.OpMatches(), types.Matches},
		{"Exists", v.OpExists(), types.Exists},
		{"NotExists", v.OpNotExists(), types.NotExists},
		{"ArrayContains", v.OpArrayContains(), types.ArrayContains},
		{"ArrayContainsAny", v.OpArrayContainsAny(), types.ArrayContainsAny},
		{"ArrayContainsAll", v.OpArrayContainsAll(), types.ArrayContainsAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.got)
			}
		})
	}
}

func TestLogicOperatorAccessors(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	if v.LogicAND() != types.AND {
		t.Error("LogicAND() should return types.AND")
	}
	if v.LogicOR() != types.OR {
		t.Error("LogicOR() should return types.OR")
	}
	if v.LogicNOT() != types.NOT {
		t.Error("LogicNOT() should return types.NOT")
	}
}

func TestDistanceMetricAccessors(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	if v.MetricCosine() != types.Cosine {
		t.Error("MetricCosine() should return types.Cosine")
	}
	if v.MetricEuclidean() != types.Euclidean {
		t.Error("MetricEuclidean() should return types.Euclidean")
	}
	if v.MetricDotProduct() != types.DotProduct {
		t.Error("MetricDotProduct() should return types.DotProduct")
	}
}

func TestOperationAccessors(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	if v.OperationSearch() != types.OpSearch {
		t.Error("OperationSearch() should return types.OpSearch")
	}
	if v.OperationUpsert() != types.OpUpsert {
		t.Error("OperationUpsert() should return types.OpUpsert")
	}
	if v.OperationDelete() != types.OpDelete {
		t.Error("OperationDelete() should return types.OpDelete")
	}
	if v.OperationFetch() != types.OpFetch {
		t.Error("OperationFetch() should return types.OpFetch")
	}
	if v.OperationUpdate() != types.OpUpdate {
		t.Error("OperationUpdate() should return types.OpUpdate")
	}
}

// --- Filter Group Tests ---

func TestTryAnd_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	cond := types.FilterCondition{
		Field:    types.MetadataField{Name: "category"},
		Operator: types.EQ,
		Value:    types.Param{Name: "cat"},
	}

	group, err := v.TryAnd(cond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group.Logic != types.AND {
		t.Errorf("expected AND logic, got %s", group.Logic)
	}
	if len(group.Conditions) != 1 {
		t.Errorf("expected 1 condition, got %d", len(group.Conditions))
	}
}

func TestTryAnd_Empty(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	_, err := v.TryAnd()
	if err == nil {
		t.Error("expected error for empty AND")
	}
}

func TestTryOr_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	cond := types.FilterCondition{
		Field:    types.MetadataField{Name: "category"},
		Operator: types.EQ,
		Value:    types.Param{Name: "cat"},
	}

	group, err := v.TryOr(cond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group.Logic != types.OR {
		t.Errorf("expected OR logic, got %s", group.Logic)
	}
}

func TestTryOr_Empty(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	_, err := v.TryOr()
	if err == nil {
		t.Error("expected error for empty OR")
	}
}

func TestTryNot_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	cond := types.FilterCondition{
		Field:    types.MetadataField{Name: "category"},
		Operator: types.EQ,
		Value:    types.Param{Name: "cat"},
	}

	group, err := v.TryNot(cond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group.Logic != types.NOT {
		t.Errorf("expected NOT logic, got %s", group.Logic)
	}
}

func TestTryNot_Nil(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	_, err := v.TryNot(nil)
	if err == nil {
		t.Error("expected error for nil NOT")
	}
}

// --- Filter Condition Tests ---

func TestTryF_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "category")
	param := v.P("cat")

	cond, err := v.TryF(field, types.EQ, param)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cond.Field.Name != "category" {
		t.Errorf("expected field 'category', got '%s'", cond.Field.Name)
	}
	if cond.Operator != types.EQ {
		t.Errorf("expected operator EQ, got %s", cond.Operator)
	}
}

func TestTryF_InvalidCollection(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := types.MetadataField{Name: "category", Collection: "nonexistent"}
	param := v.P("cat")

	_, err := v.TryF(field, types.EQ, param)
	if err == nil {
		t.Error("expected error for nonexistent collection")
	}
}

func TestTryF_InvalidField(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := types.MetadataField{Name: "nonexistent", Collection: "products"}
	param := v.P("cat")

	_, err := v.TryF(field, types.EQ, param)
	if err == nil {
		t.Error("expected error for nonexistent field")
	}
}

func TestTryF_NoCollection(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := types.MetadataField{Name: "category"}
	param := v.P("cat")

	_, err := v.TryF(field, types.EQ, param)
	if err == nil {
		t.Error("expected error for field without collection context")
	}
}

func TestShorthandConditions(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "category")
	param := v.P("val")

	tests := []struct {
		name     string
		fn       func() (types.FilterCondition, error)
		expected types.FilterOperator
	}{
		{"TryEq", func() (types.FilterCondition, error) { return v.TryEq(field, param) }, types.EQ},
		{"TryNe", func() (types.FilterCondition, error) { return v.TryNe(field, param) }, types.NE},
		{"TryGt", func() (types.FilterCondition, error) { return v.TryGt(field, param) }, types.GT},
		{"TryGte", func() (types.FilterCondition, error) { return v.TryGte(field, param) }, types.GE},
		{"TryLt", func() (types.FilterCondition, error) { return v.TryLt(field, param) }, types.LT},
		{"TryLte", func() (types.FilterCondition, error) { return v.TryLte(field, param) }, types.LE},
		{"TryIn", func() (types.FilterCondition, error) { return v.TryIn(field, param) }, types.IN},
		{"TryNotIn", func() (types.FilterCondition, error) { return v.TryNotIn(field, param) }, types.NotIn},
		{"TryContains", func() (types.FilterCondition, error) { return v.TryContains(field, param) }, types.Contains},
		{"TryStartsWith", func() (types.FilterCondition, error) { return v.TryStartsWith(field, param) }, types.StartsWith},
		{"TryEndsWith", func() (types.FilterCondition, error) { return v.TryEndsWith(field, param) }, types.EndsWith},
		{"TryMatches", func() (types.FilterCondition, error) { return v.TryMatches(field, param) }, types.Matches},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, err := tt.fn()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cond.Operator != tt.expected {
				t.Errorf("expected operator %s, got %s", tt.expected, cond.Operator)
			}
		})
	}
}

func TestTryExists_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "category")
	cond, err := v.TryExists(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cond.Operator != types.Exists {
		t.Errorf("expected Exists operator, got %s", cond.Operator)
	}
}

func TestTryNotExists_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "category")
	cond, err := v.TryNotExists(field)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cond.Operator != types.NotExists {
		t.Errorf("expected NotExists operator, got %s", cond.Operator)
	}
}

// --- Range Filter Tests ---

func TestTryRange_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "price")
	min := v.P("min_price")
	max := v.P("max_price")

	r, err := v.TryRange(field, &min, &max)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Field.Name != "price" {
		t.Errorf("expected field 'price', got '%s'", r.Field.Name)
	}
	if r.Min == nil || r.Max == nil {
		t.Error("expected min and max to be set")
	}
}

func TestTryRange_MinOnly(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "price")
	min := v.P("min_price")

	r, err := v.TryRange(field, &min, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Min == nil {
		t.Error("expected min to be set")
	}
	if r.Max != nil {
		t.Error("expected max to be nil")
	}
}

func TestTryRange_NoBounds(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "price")

	_, err := v.TryRange(field, nil, nil)
	if err == nil {
		t.Error("expected error for range without bounds")
	}
}

func TestTryRangeExclusive(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "price")
	min := v.P("min")
	max := v.P("max")

	r, err := v.TryRangeExclusive(field, &min, &max)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.MinExclusive || !r.MaxExclusive {
		t.Error("expected exclusive bounds")
	}
}

// --- Geo Filter Tests ---

func TestTryGeo_Success(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := v.M("products", "location")
	lat := v.P("lat")
	lon := v.P("lon")
	radius := v.P("radius")

	g, err := v.TryGeo(field, lat, lon, radius)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g.Field.Name != "location" {
		t.Errorf("expected field 'location', got '%s'", g.Field.Name)
	}
	if g.Center.Lat.Name != "lat" {
		t.Errorf("expected lat param 'lat', got '%s'", g.Center.Lat.Name)
	}
}

func TestTryGeo_InvalidField(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	field := types.MetadataField{Name: "nonexistent", Collection: "products"}
	lat := v.P("lat")
	lon := v.P("lon")
	radius := v.P("radius")

	_, err := v.TryGeo(field, lat, lon, radius)
	if err == nil {
		t.Error("expected error for nonexistent field")
	}
}

// --- Programmatic Helper Tests ---

func TestProgrammaticHelpers(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	filters := v.FilterItems()
	if filters == nil || len(filters) != 0 {
		t.Error("FilterItems() should return empty slice")
	}

	params := v.Params()
	if params == nil || len(params) != 0 {
		t.Error("Params() should return empty slice")
	}

	records := v.VectorRecords()
	if records == nil || len(records) != 0 {
		t.Error("VectorRecords() should return empty slice")
	}

	metaMap := v.MetadataMap()
	if metaMap == nil || len(metaMap) != 0 {
		t.Error("MetadataMap() should return empty map")
	}
}

// --- Panic Variant Tests ---

func TestAnd_Panics(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty And()")
		}
	}()
	v.And()
}

func TestOr_Panics(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty Or()")
		}
	}()
	v.Or()
}

func TestNot_Panics(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil Not()")
		}
	}()
	v.Not(nil)
}

func TestF_Panics(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid field")
		}
	}()
	field := types.MetadataField{Name: "nonexistent", Collection: "products"}
	v.F(field, types.EQ, v.P("val"))
}

func TestRange_Panics(t *testing.T) {
	schema := testSchema()
	v, _ := NewFromVDML(schema)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for range without bounds")
		}
	}()
	field := v.M("products", "price")
	v.Range(field, nil, nil)
}

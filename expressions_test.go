package vectql

import (
	"testing"

	"github.com/zoobzio/vectql/internal/types"
)

func TestFilterHelpers(t *testing.T) {
	field := types.MetadataField{Name: "category"}
	param := types.Param{Name: "value"}

	tests := []struct {
		name     string
		filter   types.FilterCondition
		expected types.FilterOperator
	}{
		{"Eq", Eq(field, param), types.EQ},
		{"Ne", Ne(field, param), types.NE},
		{"Gt", Gt(field, param), types.GT},
		{"Gte", Gte(field, param), types.GE},
		{"Lt", Lt(field, param), types.LT},
		{"Lte", Lte(field, param), types.LE},
		{"In", In(field, param), types.IN},
		{"NotIn", NotIn(field, param), types.NotIn},
		{"Contains", Contains(field, param), types.Contains},
		{"StartsWith", StartsWith(field, param), types.StartsWith},
		{"EndsWith", EndsWith(field, param), types.EndsWith},
		{"Matches", Matches(field, param), types.Matches},
		{"ArrayContains", ArrayContains(field, param), types.ArrayContains},
		{"ArrayContainsAny", ArrayContainsAny(field, param), types.ArrayContainsAny},
		{"ArrayContainsAll", ArrayContainsAll(field, param), types.ArrayContainsAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.filter.Operator != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.filter.Operator)
			}
			if tt.filter.Field.Name != "category" {
				t.Errorf("expected category, got %s", tt.filter.Field.Name)
			}
			if tt.filter.Value.Name != "value" {
				t.Errorf("expected value, got %s", tt.filter.Value.Name)
			}
		})
	}
}

func TestExistsFilters(t *testing.T) {
	field := types.MetadataField{Name: "category"}

	exists := Exists(field)
	if exists.Operator != types.Exists {
		t.Errorf("expected Exists, got %s", exists.Operator)
	}

	notExists := NotExists(field)
	if notExists.Operator != types.NotExists {
		t.Errorf("expected NotExists, got %s", notExists.Operator)
	}
}

func TestLogicHelpers(t *testing.T) {
	field := types.MetadataField{Name: "category"}
	cond1 := Eq(field, types.Param{Name: "v1"})
	cond2 := Eq(field, types.Param{Name: "v2"})

	andGroup := And(cond1, cond2)
	if andGroup.Logic != types.AND {
		t.Errorf("expected AND, got %s", andGroup.Logic)
	}
	if len(andGroup.Conditions) != 2 {
		t.Errorf("expected 2 conditions, got %d", len(andGroup.Conditions))
	}

	orGroup := Or(cond1, cond2)
	if orGroup.Logic != types.OR {
		t.Errorf("expected OR, got %s", orGroup.Logic)
	}

	notGroup := Not(cond1)
	if notGroup.Logic != types.NOT {
		t.Errorf("expected NOT, got %s", notGroup.Logic)
	}
	if len(notGroup.Conditions) != 1 {
		t.Errorf("expected 1 condition, got %d", len(notGroup.Conditions))
	}
}

func TestRangeFilter(t *testing.T) {
	field := types.MetadataField{Name: "price"}
	minVal := types.Param{Name: "min_price"}
	maxVal := types.Param{Name: "max_price"}

	rangeFilter := Range(field, &minVal, &maxVal)
	if rangeFilter.Field.Name != "price" {
		t.Errorf("expected price, got %s", rangeFilter.Field.Name)
	}
	if rangeFilter.Min.Name != "min_price" {
		t.Errorf("expected min_price, got %s", rangeFilter.Min.Name)
	}
	if rangeFilter.Max.Name != "max_price" {
		t.Errorf("expected max_price, got %s", rangeFilter.Max.Name)
	}
	if rangeFilter.MinExclusive || rangeFilter.MaxExclusive {
		t.Error("expected exclusive to be false")
	}

	exclusiveRange := RangeExclusive(field, &minVal, &maxVal)
	if !exclusiveRange.MinExclusive || !exclusiveRange.MaxExclusive {
		t.Error("expected exclusive to be true")
	}
}

func TestGeoFilter(t *testing.T) {
	field := types.MetadataField{Name: "location"}
	lat := types.Param{Name: "lat"}
	lon := types.Param{Name: "lon"}
	radius := types.Param{Name: "radius"}

	geoFilter := Geo(field, lat, lon, radius)
	if geoFilter.Field.Name != "location" {
		t.Errorf("expected location, got %s", geoFilter.Field.Name)
	}
	if geoFilter.Center.Lat.Name != "lat" {
		t.Errorf("expected lat, got %s", geoFilter.Center.Lat.Name)
	}
	if geoFilter.Center.Lon.Name != "lon" {
		t.Errorf("expected lon, got %s", geoFilter.Center.Lon.Name)
	}
	if geoFilter.Radius.Name != "radius" {
		t.Errorf("expected radius, got %s", geoFilter.Radius.Name)
	}
}

func TestVectorHelpers(t *testing.T) {
	// Parameterized vector
	paramVec := Vec(types.Param{Name: "query_vec"})
	if paramVec.Param == nil {
		t.Fatal("expected Param to be set")
	}
	if paramVec.Param.Name != "query_vec" {
		t.Errorf("expected query_vec, got %s", paramVec.Param.Name)
	}

	// Literal vector
	literal := []float32{0.1, 0.2, 0.3}
	litVec := VecLiteral(literal)
	if litVec.Param != nil {
		t.Error("expected Param to be nil for literal")
	}
	if len(litVec.Literal) != 3 {
		t.Errorf("expected 3 values, got %d", len(litVec.Literal))
	}
}

func TestSparseVectorHelpers(t *testing.T) {
	// Parameterized sparse vector
	paramSparse := SparseVec(types.Param{Name: "sparse_vec"})
	if paramSparse.Param == nil {
		t.Fatal("expected Param to be set")
	}
	if paramSparse.Param.Name != "sparse_vec" {
		t.Errorf("expected sparse_vec, got %s", paramSparse.Param.Name)
	}

	// Literal sparse vector
	indices := []int{0, 5, 10}
	values := []float32{0.1, 0.5, 0.9}
	litSparse := SparseVecLiteral(indices, values)
	if litSparse.Param != nil {
		t.Error("expected Param to be nil for literal")
	}
	if len(litSparse.Indices) != 3 {
		t.Errorf("expected 3 indices, got %d", len(litSparse.Indices))
	}
	if len(litSparse.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(litSparse.Values))
	}
}

func TestVectorRecordBuilder(t *testing.T) {
	category := types.MetadataField{Name: "category"}
	price := types.MetadataField{Name: "price"}

	record := NewRecord(types.Param{Name: "id1"}, Vec(types.Param{Name: "vec1"})).
		WithMetadata(category, types.Param{Name: "cat"}).
		WithMetadata(price, types.Param{Name: "price_val"}).
		WithSparseVector(SparseVec(types.Param{Name: "sparse"})).
		Build()

	if record.ID.Name != "id1" {
		t.Errorf("expected id1, got %s", record.ID.Name)
	}
	if record.Vector.Param.Name != "vec1" {
		t.Errorf("expected vec1, got %s", record.Vector.Param.Name)
	}
	if len(record.Metadata) != 2 {
		t.Errorf("expected 2 metadata fields, got %d", len(record.Metadata))
	}
	if record.SparseVector == nil {
		t.Fatal("expected SparseVector to be set")
	}
	if record.SparseVector.Param.Name != "sparse" {
		t.Errorf("expected sparse, got %s", record.SparseVector.Param.Name)
	}
}

func TestGenericFilterHelper(t *testing.T) {
	field := types.MetadataField{Name: "category"}
	param := types.Param{Name: "value"}

	filter := F(field, types.EQ, param)
	if filter.Field.Name != "category" {
		t.Errorf("expected category, got %s", filter.Field.Name)
	}
	if filter.Operator != types.EQ {
		t.Errorf("expected EQ, got %s", filter.Operator)
	}
	if filter.Value.Name != "value" {
		t.Errorf("expected value, got %s", filter.Value.Name)
	}
}

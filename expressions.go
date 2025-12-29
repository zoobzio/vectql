package vectql

import "github.com/zoobzio/vectql/internal/types"

// F creates a filter condition.
func F(field types.MetadataField, op types.FilterOperator, value types.Param) types.FilterCondition {
	return types.FilterCondition{
		Field:    field,
		Operator: op,
		Value:    value,
	}
}

// And creates an AND filter group.
func And(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{
		Logic:      types.AND,
		Conditions: conditions,
	}
}

// Or creates an OR filter group.
func Or(conditions ...types.FilterItem) types.FilterGroup {
	return types.FilterGroup{
		Logic:      types.OR,
		Conditions: conditions,
	}
}

// Not negates a filter condition.
func Not(condition types.FilterItem) types.FilterGroup {
	return types.FilterGroup{
		Logic:      types.NOT,
		Conditions: []types.FilterItem{condition},
	}
}

// Range creates a numeric range filter.
func Range(field types.MetadataField, minVal, maxVal *types.Param) types.RangeFilter {
	return types.RangeFilter{
		Field: field,
		Min:   minVal,
		Max:   maxVal,
	}
}

// RangeExclusive creates a range with exclusive bounds.
func RangeExclusive(field types.MetadataField, minVal, maxVal *types.Param) types.RangeFilter {
	return types.RangeFilter{
		Field:        field,
		Min:          minVal,
		Max:          maxVal,
		MinExclusive: true,
		MaxExclusive: true,
	}
}

// Geo creates a geospatial filter.
func Geo(field types.MetadataField, lat, lon, radius types.Param) types.GeoFilter {
	return types.GeoFilter{
		Field:  field,
		Center: types.GeoPoint{Lat: lat, Lon: lon},
		Radius: radius,
	}
}

// Vec creates a VectorValue from a parameter.
func Vec(p types.Param) types.VectorValue {
	return types.VectorValue{Param: &p}
}

// VecLiteral creates a VectorValue from literal values.
func VecLiteral(values []float32) types.VectorValue {
	return types.VectorValue{Literal: values}
}

// SparseVec creates a SparseVectorValue from a parameter.
func SparseVec(p types.Param) types.SparseVectorValue {
	return types.SparseVectorValue{Param: &p}
}

// SparseVecLiteral creates a SparseVectorValue from literal values.
func SparseVecLiteral(indices []int, values []float32) types.SparseVectorValue {
	return types.SparseVectorValue{Indices: indices, Values: values}
}

// VectorRecordBuilder builds vector records for upsert.
type VectorRecordBuilder struct {
	record types.VectorRecord
}

// NewRecord creates a new vector record builder.
func NewRecord(id types.Param, vector types.VectorValue) *VectorRecordBuilder {
	return &VectorRecordBuilder{
		record: types.VectorRecord{
			ID:       id,
			Vector:   vector,
			Metadata: make(map[types.MetadataField]types.Param),
		},
	}
}

// WithMetadata adds metadata to the record.
func (rb *VectorRecordBuilder) WithMetadata(field types.MetadataField, value types.Param) *VectorRecordBuilder {
	rb.record.Metadata[field] = value
	return rb
}

// WithSparseVector adds a sparse vector for hybrid search.
func (rb *VectorRecordBuilder) WithSparseVector(sv types.SparseVectorValue) *VectorRecordBuilder {
	rb.record.SparseVector = &sv
	return rb
}

// Build returns the vector record.
func (rb *VectorRecordBuilder) Build() types.VectorRecord {
	return rb.record
}

// Eq creates an equality filter.
func Eq(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.EQ, value)
}

// Ne creates a not-equal filter.
func Ne(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.NE, value)
}

// Gt creates a greater-than filter.
func Gt(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.GT, value)
}

// Gte creates a greater-than-or-equal filter.
func Gte(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.GE, value)
}

// Lt creates a less-than filter.
func Lt(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.LT, value)
}

// Lte creates a less-than-or-equal filter.
func Lte(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.LE, value)
}

// In creates an IN filter.
func In(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.IN, value)
}

// NotIn creates a NOT IN filter.
func NotIn(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.NotIn, value)
}

// Contains creates a string contains filter.
func Contains(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.Contains, value)
}

// StartsWith creates a string starts-with filter.
func StartsWith(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.StartsWith, value)
}

// EndsWith creates a string ends-with filter.
func EndsWith(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.EndsWith, value)
}

// Matches creates a regex match filter.
func Matches(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.Matches, value)
}

// Exists creates an existence check filter.
func Exists(field types.MetadataField) types.FilterCondition {
	return types.FilterCondition{
		Field:    field,
		Operator: types.Exists,
	}
}

// NotExists creates a non-existence check filter.
func NotExists(field types.MetadataField) types.FilterCondition {
	return types.FilterCondition{
		Field:    field,
		Operator: types.NotExists,
	}
}

// ArrayContains creates an array contains filter.
func ArrayContains(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.ArrayContains, value)
}

// ArrayContainsAny creates an array contains-any filter.
func ArrayContainsAny(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.ArrayContainsAny, value)
}

// ArrayContainsAll creates an array contains-all filter.
func ArrayContainsAll(field types.MetadataField, value types.Param) types.FilterCondition {
	return F(field, types.ArrayContainsAll, value)
}

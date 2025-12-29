package vectql

import (
	"fmt"
	"strings"

	"github.com/zoobzio/vdml"
	"github.com/zoobzio/vectql/internal/types"
)

// VECTQL represents an instance with VDML schema validation.
type VECTQL struct {
	schema      *vdml.Schema
	collections map[string]*vdml.Collection
	embeddings  map[string]map[string]*vdml.Embedding
	metadata    map[string]map[string]*vdml.MetadataField
}

// NewFromVDML creates a new VECTQL instance from a VDML schema.
func NewFromVDML(schema *vdml.Schema) (*VECTQL, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	v := &VECTQL{
		schema:      schema,
		collections: make(map[string]*vdml.Collection),
		embeddings:  make(map[string]map[string]*vdml.Embedding),
		metadata:    make(map[string]map[string]*vdml.MetadataField),
	}

	// Build indexes
	for name, coll := range schema.Collections {
		v.collections[name] = coll
		v.embeddings[name] = make(map[string]*vdml.Embedding)
		v.metadata[name] = make(map[string]*vdml.MetadataField)

		for _, emb := range coll.Embeddings {
			v.embeddings[name][emb.Name] = emb
		}
		for _, meta := range coll.Metadata {
			v.metadata[name][meta.Name] = meta
		}
	}

	return v, nil
}

// C creates a validated collection reference.
func (v *VECTQL) C(name string) types.Collection {
	c, err := v.TryC(name)
	if err != nil {
		panic(err)
	}
	return c
}

// TryC creates a collection reference with error handling.
func (v *VECTQL) TryC(name string) (types.Collection, error) {
	if _, ok := v.collections[name]; !ok {
		return types.Collection{}, fmt.Errorf("collection '%s' not found in schema", name)
	}
	return types.Collection{Name: name}, nil
}

// E creates a validated embedding field reference.
func (v *VECTQL) E(collectionName, embeddingName string) types.EmbeddingField {
	e, err := v.TryE(collectionName, embeddingName)
	if err != nil {
		panic(err)
	}
	return e
}

// TryE creates an embedding reference with error handling.
func (v *VECTQL) TryE(collectionName, embeddingName string) (types.EmbeddingField, error) {
	collEmbs, ok := v.embeddings[collectionName]
	if !ok {
		return types.EmbeddingField{}, fmt.Errorf("collection '%s' not found", collectionName)
	}
	if _, ok := collEmbs[embeddingName]; !ok {
		return types.EmbeddingField{}, fmt.Errorf("embedding '%s' not found in collection '%s'", embeddingName, collectionName)
	}
	return types.EmbeddingField{Name: embeddingName, Collection: collectionName}, nil
}

// M creates a validated metadata field reference.
func (v *VECTQL) M(collectionName, fieldName string) types.MetadataField {
	m, err := v.TryM(collectionName, fieldName)
	if err != nil {
		panic(err)
	}
	return m
}

// TryM creates a metadata field reference with error handling.
func (v *VECTQL) TryM(collectionName, fieldName string) (types.MetadataField, error) {
	collMeta, ok := v.metadata[collectionName]
	if !ok {
		return types.MetadataField{}, fmt.Errorf("collection '%s' not found", collectionName)
	}
	if _, ok := collMeta[fieldName]; !ok {
		return types.MetadataField{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", fieldName, collectionName)
	}
	return types.MetadataField{Name: fieldName, Collection: collectionName}, nil
}

// P creates a validated parameter reference.
func (v *VECTQL) P(name string) types.Param {
	p, err := v.TryP(name)
	if err != nil {
		panic(err)
	}
	return p
}

// TryP creates a parameter with error handling.
func (v *VECTQL) TryP(name string) (types.Param, error) {
	if !isValidIdentifier(name) {
		return types.Param{}, fmt.Errorf("invalid parameter name: %s", name)
	}
	return types.Param{Name: name}, nil
}

// GetEmbeddingDimensions returns the dimensions for an embedding field.
func (v *VECTQL) GetEmbeddingDimensions(collectionName, embeddingName string) (int, error) {
	if collEmbs, ok := v.embeddings[collectionName]; ok {
		if emb, ok := collEmbs[embeddingName]; ok {
			return emb.Dimensions, nil
		}
	}
	return 0, fmt.Errorf("embedding '%s' not found in collection '%s'", embeddingName, collectionName)
}

// GetEmbeddingMetric returns the distance metric for an embedding field.
func (v *VECTQL) GetEmbeddingMetric(collectionName, embeddingName string) (vdml.DistanceMetric, error) {
	if collEmbs, ok := v.embeddings[collectionName]; ok {
		if emb, ok := collEmbs[embeddingName]; ok {
			return emb.Metric, nil
		}
	}
	return "", fmt.Errorf("embedding '%s' not found in collection '%s'", embeddingName, collectionName)
}

// Collections returns all collection names in the schema.
func (v *VECTQL) Collections() []string {
	names := make([]string, 0, len(v.collections))
	for name := range v.collections {
		names = append(names, name)
	}
	return names
}

// Embeddings returns all embedding names for a collection.
func (v *VECTQL) Embeddings(collectionName string) ([]string, error) {
	collEmbs, ok := v.embeddings[collectionName]
	if !ok {
		return nil, fmt.Errorf("collection '%s' not found", collectionName)
	}
	names := make([]string, 0, len(collEmbs))
	for name := range collEmbs {
		names = append(names, name)
	}
	return names, nil
}

// MetadataFields returns all metadata field names for a collection.
func (v *VECTQL) MetadataFields(collectionName string) ([]string, error) {
	collMeta, ok := v.metadata[collectionName]
	if !ok {
		return nil, fmt.Errorf("collection '%s' not found", collectionName)
	}
	names := make([]string, 0, len(collMeta))
	for name := range collMeta {
		names = append(names, name)
	}
	return names, nil
}

// suspiciousPatterns contains strings that indicate potential injection attempts.
var suspiciousPatterns = []string{
	";", "--", "/*", "*/", "'", "\"", "`", "\\",
	" or ", " and ", "drop ", "delete ", "insert ",
	"update ", "select ", "union ", "exec ", "execute ",
}

func isValidIdentifier(s string) bool {
	if s == "" {
		return false
	}

	// Check character validity
	for i, r := range s {
		if i == 0 {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != '_' {
				return false
			}
		} else {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
				return false
			}
		}
	}

	// Check for injection patterns
	lower := strings.ToLower(s)
	for _, pattern := range suspiciousPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	return true
}

// --- Filter Operator Accessors ---

// OpEQ returns the equality filter operator.
func (*VECTQL) OpEQ() types.FilterOperator { return types.EQ }

// OpNE returns the not-equal filter operator.
func (*VECTQL) OpNE() types.FilterOperator { return types.NE }

// OpGT returns the greater-than filter operator.
func (*VECTQL) OpGT() types.FilterOperator { return types.GT }

// OpGE returns the greater-than-or-equal filter operator.
func (*VECTQL) OpGE() types.FilterOperator { return types.GE }

// OpLT returns the less-than filter operator.
func (*VECTQL) OpLT() types.FilterOperator { return types.LT }

// OpLE returns the less-than-or-equal filter operator.
func (*VECTQL) OpLE() types.FilterOperator { return types.LE }

// OpIN returns the IN filter operator.
func (*VECTQL) OpIN() types.FilterOperator { return types.IN }

// OpNotIn returns the NOT IN filter operator.
func (*VECTQL) OpNotIn() types.FilterOperator { return types.NotIn }

// OpContains returns the string contains filter operator.
func (*VECTQL) OpContains() types.FilterOperator { return types.Contains }

// OpStartsWith returns the string starts-with filter operator.
func (*VECTQL) OpStartsWith() types.FilterOperator { return types.StartsWith }

// OpEndsWith returns the string ends-with filter operator.
func (*VECTQL) OpEndsWith() types.FilterOperator { return types.EndsWith }

// OpMatches returns the regex match filter operator.
func (*VECTQL) OpMatches() types.FilterOperator { return types.Matches }

// OpExists returns the field exists filter operator.
func (*VECTQL) OpExists() types.FilterOperator { return types.Exists }

// OpNotExists returns the field not-exists filter operator.
func (*VECTQL) OpNotExists() types.FilterOperator { return types.NotExists }

// OpArrayContains returns the array contains filter operator.
func (*VECTQL) OpArrayContains() types.FilterOperator { return types.ArrayContains }

// OpArrayContainsAny returns the array contains-any filter operator.
func (*VECTQL) OpArrayContainsAny() types.FilterOperator { return types.ArrayContainsAny }

// OpArrayContainsAll returns the array contains-all filter operator.
func (*VECTQL) OpArrayContainsAll() types.FilterOperator { return types.ArrayContainsAll }

// --- Logic Operator Accessors ---

// LogicAND returns the AND logic operator.
func (*VECTQL) LogicAND() types.LogicOperator { return types.AND }

// LogicOR returns the OR logic operator.
func (*VECTQL) LogicOR() types.LogicOperator { return types.OR }

// LogicNOT returns the NOT logic operator.
func (*VECTQL) LogicNOT() types.LogicOperator { return types.NOT }

// --- Distance Metric Accessors ---

// MetricCosine returns the cosine distance metric.
func (*VECTQL) MetricCosine() types.DistanceMetric { return types.Cosine }

// MetricEuclidean returns the Euclidean distance metric.
func (*VECTQL) MetricEuclidean() types.DistanceMetric { return types.Euclidean }

// MetricDotProduct returns the dot product distance metric.
func (*VECTQL) MetricDotProduct() types.DistanceMetric { return types.DotProduct }

// --- Operation Accessors ---

// OperationSearch returns the SEARCH operation constant.
func (*VECTQL) OperationSearch() types.Operation { return types.OpSearch }

// OperationUpsert returns the UPSERT operation constant.
func (*VECTQL) OperationUpsert() types.Operation { return types.OpUpsert }

// OperationDelete returns the DELETE operation constant.
func (*VECTQL) OperationDelete() types.Operation { return types.OpDelete }

// OperationFetch returns the FETCH operation constant.
func (*VECTQL) OperationFetch() types.Operation { return types.OpFetch }

// OperationUpdate returns the UPDATE operation constant.
func (*VECTQL) OperationUpdate() types.Operation { return types.OpUpdate }

// --- Filter Group Constructors ---

// TryAnd creates a validated AND filter group.
func (*VECTQL) TryAnd(conditions ...types.FilterItem) (types.FilterGroup, error) {
	if len(conditions) == 0 {
		return types.FilterGroup{}, fmt.Errorf("AND requires at least one condition")
	}
	return types.FilterGroup{
		Logic:      types.AND,
		Conditions: conditions,
	}, nil
}

// And creates an AND filter group (panics on error).
func (v *VECTQL) And(conditions ...types.FilterItem) types.FilterGroup {
	g, err := v.TryAnd(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}

// TryOr creates a validated OR filter group.
func (*VECTQL) TryOr(conditions ...types.FilterItem) (types.FilterGroup, error) {
	if len(conditions) == 0 {
		return types.FilterGroup{}, fmt.Errorf("OR requires at least one condition")
	}
	return types.FilterGroup{
		Logic:      types.OR,
		Conditions: conditions,
	}, nil
}

// Or creates an OR filter group (panics on error).
func (v *VECTQL) Or(conditions ...types.FilterItem) types.FilterGroup {
	g, err := v.TryOr(conditions...)
	if err != nil {
		panic(err)
	}
	return g
}

// TryNot creates a validated NOT filter group.
func (*VECTQL) TryNot(condition types.FilterItem) (types.FilterGroup, error) {
	if condition == nil {
		return types.FilterGroup{}, fmt.Errorf("NOT requires a condition")
	}
	return types.FilterGroup{
		Logic:      types.NOT,
		Conditions: []types.FilterItem{condition},
	}, nil
}

// Not creates a NOT filter group (panics on error).
func (v *VECTQL) Not(condition types.FilterItem) types.FilterGroup {
	g, err := v.TryNot(condition)
	if err != nil {
		panic(err)
	}
	return g
}

// --- Filter Condition Constructors ---

// TryF creates a validated filter condition.
func (v *VECTQL) TryF(field types.MetadataField, op types.FilterOperator, value types.Param) (types.FilterCondition, error) {
	if field.Collection == "" {
		return types.FilterCondition{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.FilterCondition{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.FilterCondition{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	return types.FilterCondition{
		Field:    field,
		Operator: op,
		Value:    value,
	}, nil
}

// F creates a filter condition (panics on error).
func (v *VECTQL) F(field types.MetadataField, op types.FilterOperator, value types.Param) types.FilterCondition {
	c, err := v.TryF(field, op, value)
	if err != nil {
		panic(err)
	}
	return c
}

// TryEq creates a validated equality filter condition.
func (v *VECTQL) TryEq(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.EQ, value)
}

// Eq creates an equality filter condition (panics on error).
func (v *VECTQL) Eq(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.EQ, value)
}

// TryNe creates a validated not-equal filter condition.
func (v *VECTQL) TryNe(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.NE, value)
}

// Ne creates a not-equal filter condition (panics on error).
func (v *VECTQL) Ne(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.NE, value)
}

// TryGt creates a validated greater-than filter condition.
func (v *VECTQL) TryGt(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.GT, value)
}

// Gt creates a greater-than filter condition (panics on error).
func (v *VECTQL) Gt(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.GT, value)
}

// TryGte creates a validated greater-than-or-equal filter condition.
func (v *VECTQL) TryGte(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.GE, value)
}

// Gte creates a greater-than-or-equal filter condition (panics on error).
func (v *VECTQL) Gte(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.GE, value)
}

// TryLt creates a validated less-than filter condition.
func (v *VECTQL) TryLt(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.LT, value)
}

// Lt creates a less-than filter condition (panics on error).
func (v *VECTQL) Lt(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.LT, value)
}

// TryLte creates a validated less-than-or-equal filter condition.
func (v *VECTQL) TryLte(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.LE, value)
}

// Lte creates a less-than-or-equal filter condition (panics on error).
func (v *VECTQL) Lte(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.LE, value)
}

// TryIn creates a validated IN filter condition.
func (v *VECTQL) TryIn(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.IN, value)
}

// In creates an IN filter condition (panics on error).
func (v *VECTQL) In(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.IN, value)
}

// TryNotIn creates a validated NOT IN filter condition.
func (v *VECTQL) TryNotIn(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.NotIn, value)
}

// NotIn creates a NOT IN filter condition (panics on error).
func (v *VECTQL) NotIn(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.NotIn, value)
}

// TryContains creates a validated string contains filter condition.
func (v *VECTQL) TryContains(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.Contains, value)
}

// Contains creates a string contains filter condition (panics on error).
func (v *VECTQL) Contains(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.Contains, value)
}

// TryStartsWith creates a validated string starts-with filter condition.
func (v *VECTQL) TryStartsWith(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.StartsWith, value)
}

// StartsWith creates a string starts-with filter condition (panics on error).
func (v *VECTQL) StartsWith(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.StartsWith, value)
}

// TryEndsWith creates a validated string ends-with filter condition.
func (v *VECTQL) TryEndsWith(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.EndsWith, value)
}

// EndsWith creates a string ends-with filter condition (panics on error).
func (v *VECTQL) EndsWith(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.EndsWith, value)
}

// TryMatches creates a validated regex match filter condition.
func (v *VECTQL) TryMatches(field types.MetadataField, value types.Param) (types.FilterCondition, error) {
	return v.TryF(field, types.Matches, value)
}

// Matches creates a regex match filter condition (panics on error).
func (v *VECTQL) Matches(field types.MetadataField, value types.Param) types.FilterCondition {
	return v.F(field, types.Matches, value)
}

// TryExists creates a validated field exists filter condition.
func (v *VECTQL) TryExists(field types.MetadataField) (types.FilterCondition, error) {
	if field.Collection == "" {
		return types.FilterCondition{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.FilterCondition{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.FilterCondition{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	return types.FilterCondition{
		Field:    field,
		Operator: types.Exists,
	}, nil
}

// Exists creates a field exists filter condition (panics on error).
func (v *VECTQL) Exists(field types.MetadataField) types.FilterCondition {
	c, err := v.TryExists(field)
	if err != nil {
		panic(err)
	}
	return c
}

// TryNotExists creates a validated field not-exists filter condition.
func (v *VECTQL) TryNotExists(field types.MetadataField) (types.FilterCondition, error) {
	if field.Collection == "" {
		return types.FilterCondition{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.FilterCondition{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.FilterCondition{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	return types.FilterCondition{
		Field:    field,
		Operator: types.NotExists,
	}, nil
}

// NotExists creates a field not-exists filter condition (panics on error).
func (v *VECTQL) NotExists(field types.MetadataField) types.FilterCondition {
	c, err := v.TryNotExists(field)
	if err != nil {
		panic(err)
	}
	return c
}

// --- Range Filter Constructors ---

// TryRange creates a validated range filter.
func (v *VECTQL) TryRange(field types.MetadataField, minVal, maxVal *types.Param) (types.RangeFilter, error) {
	if field.Collection == "" {
		return types.RangeFilter{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.RangeFilter{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.RangeFilter{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	if minVal == nil && maxVal == nil {
		return types.RangeFilter{}, fmt.Errorf("range requires at least min or max")
	}
	return types.RangeFilter{
		Field: field,
		Min:   minVal,
		Max:   maxVal,
	}, nil
}

// Range creates a range filter (panics on error).
func (v *VECTQL) Range(field types.MetadataField, minVal, maxVal *types.Param) types.RangeFilter {
	r, err := v.TryRange(field, minVal, maxVal)
	if err != nil {
		panic(err)
	}
	return r
}

// TryRangeExclusive creates a validated range filter with exclusive bounds.
func (v *VECTQL) TryRangeExclusive(field types.MetadataField, minVal, maxVal *types.Param) (types.RangeFilter, error) {
	if field.Collection == "" {
		return types.RangeFilter{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.RangeFilter{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.RangeFilter{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	if minVal == nil && maxVal == nil {
		return types.RangeFilter{}, fmt.Errorf("range requires at least min or max")
	}
	return types.RangeFilter{
		Field:        field,
		Min:          minVal,
		Max:          maxVal,
		MinExclusive: true,
		MaxExclusive: true,
	}, nil
}

// RangeExclusive creates a range filter with exclusive bounds (panics on error).
func (v *VECTQL) RangeExclusive(field types.MetadataField, minVal, maxVal *types.Param) types.RangeFilter {
	r, err := v.TryRangeExclusive(field, minVal, maxVal)
	if err != nil {
		panic(err)
	}
	return r
}

// --- Geo Filter Constructors ---

// TryGeo creates a validated geo filter.
func (v *VECTQL) TryGeo(field types.MetadataField, lat, lon, radius types.Param) (types.GeoFilter, error) {
	if field.Collection == "" {
		return types.GeoFilter{}, fmt.Errorf("metadata field has no collection context")
	}
	if _, ok := v.metadata[field.Collection]; !ok {
		return types.GeoFilter{}, fmt.Errorf("collection '%s' not found", field.Collection)
	}
	if _, ok := v.metadata[field.Collection][field.Name]; !ok {
		return types.GeoFilter{}, fmt.Errorf("metadata field '%s' not found in collection '%s'", field.Name, field.Collection)
	}
	return types.GeoFilter{
		Field:  field,
		Center: types.GeoPoint{Lat: lat, Lon: lon},
		Radius: radius,
	}, nil
}

// Geo creates a geo filter (panics on error).
func (v *VECTQL) Geo(field types.MetadataField, lat, lon, radius types.Param) types.GeoFilter {
	g, err := v.TryGeo(field, lat, lon, radius)
	if err != nil {
		panic(err)
	}
	return g
}

// --- Programmatic Helper Methods ---

// FilterItems returns an empty slice for programmatic filter building.
func (*VECTQL) FilterItems() []types.FilterItem {
	return []types.FilterItem{}
}

// Params returns an empty slice for programmatic parameter building.
func (*VECTQL) Params() []types.Param {
	return []types.Param{}
}

// VectorRecords returns an empty slice for programmatic record building.
func (*VECTQL) VectorRecords() []types.VectorRecord {
	return []types.VectorRecord{}
}

// MetadataMap returns an empty map for programmatic metadata building.
func (*VECTQL) MetadataMap() map[types.MetadataField]types.Param {
	return make(map[types.MetadataField]types.Param)
}

package types

// FilterItem represents either a single filter condition or a group.
type FilterItem interface {
	isFilterItem()
}

// FilterCondition represents a single metadata filter.
type FilterCondition struct {
	Field    MetadataField
	Operator FilterOperator
	Value    Param
}

func (FilterCondition) isFilterItem() {}

// FilterGroup represents grouped conditions with AND/OR/NOT logic.
type FilterGroup struct {
	Logic      LogicOperator
	Conditions []FilterItem
}

func (FilterGroup) isFilterItem() {}

// RangeFilter represents a numeric range query.
type RangeFilter struct {
	Field        MetadataField
	Min          *Param
	Max          *Param
	MinExclusive bool
	MaxExclusive bool
}

func (RangeFilter) isFilterItem() {}

// GeoFilter represents a geospatial query.
type GeoFilter struct {
	Field  MetadataField
	Center GeoPoint
	Radius Param
}

func (GeoFilter) isFilterItem() {}

// GeoPoint represents a geographic coordinate.
type GeoPoint struct {
	Lat Param
	Lon Param
}

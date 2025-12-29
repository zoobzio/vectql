// Package types provides internal type definitions for VECTQL.
package types

// FilterOperator represents metadata filter operators.
type FilterOperator string

// Equality operators.
const (
	EQ FilterOperator = "="
	NE FilterOperator = "!="
)

// Comparison operators.
const (
	GT FilterOperator = ">"
	GE FilterOperator = ">="
	LT FilterOperator = "<"
	LE FilterOperator = "<="
)

// Set operators.
const (
	IN    FilterOperator = "IN"
	NotIn FilterOperator = "NOT_IN"
)

// String operators.
const (
	Contains   FilterOperator = "CONTAINS"
	StartsWith FilterOperator = "STARTS_WITH"
	EndsWith   FilterOperator = "ENDS_WITH"
	Matches    FilterOperator = "MATCHES"
)

// Existence operators.
const (
	Exists    FilterOperator = "EXISTS"
	NotExists FilterOperator = "NOT_EXISTS"
)

// Array operators.
const (
	ArrayContains    FilterOperator = "ARRAY_CONTAINS"
	ArrayContainsAny FilterOperator = "ARRAY_CONTAINS_ANY"
	ArrayContainsAll FilterOperator = "ARRAY_CONTAINS_ALL"
)

// LogicOperator for combining filter conditions.
type LogicOperator string

// Logic operators.
const (
	AND LogicOperator = "AND"
	OR  LogicOperator = "OR"
	NOT LogicOperator = "NOT"
)

// DistanceMetric for similarity calculations.
type DistanceMetric string

// Distance metrics.
const (
	Cosine     DistanceMetric = "COSINE"
	Euclidean  DistanceMetric = "EUCLIDEAN"
	DotProduct DistanceMetric = "DOT_PRODUCT"
	Manhattan  DistanceMetric = "MANHATTAN"
)

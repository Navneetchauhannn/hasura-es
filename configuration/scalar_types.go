package configuration

// scalartypes := schema.SchemaResponseScalarTypes{
// 	"Int": schema.ScalarType{
// 		AggregateFunctions: schema.ScalarTypeAggregateFunctions{
// 			"max": schema.AggregateFunctionDefinition{
// 				ResultType: schema.NewNullableNamedType("Int").Encode(),
// 			},
// 			"min": schema.AggregateFunctionDefinition{
// 				ResultType: schema.NewNullableNamedType("Int").Encode(),
// 			},
// 		},
// 		ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 			"eq": schema.NewComparisonOperatorCustom(schema.NewNamedType("Int")).Encode(),
// 			"in": schema.NewComparisonOperatorCustom(schema.NewNamedType("Int")).Encode(),
// 			// "eq": schema.NewComparisonOperatorEqual().Encode(),
// 			// "in": schema.NewComparisonOperatorIn().Encode(),
// 		},
// 	},
// 	"Float": schema.ScalarType{
// 		AggregateFunctions: schema.ScalarTypeAggregateFunctions{
// 			"max": schema.AggregateFunctionDefinition{
// 				ResultType: schema.NewNullableNamedType("Float").Encode(),
// 			},
// 			"min": schema.AggregateFunctionDefinition{
// 				ResultType: schema.NewNullableNamedType("Float").Encode(),
// 			},
// 		},
// 		ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 			// "eq": schema.NewComparisonOperatorEqual().Encode(),
// 			// "in": schema.NewComparisonOperatorIn().Encode(),
// 			"eq": schema.NewComparisonOperatorCustom(schema.NewNamedType("Float")).Encode(),
// 			"in": schema.NewComparisonOperatorCustom(schema.NewNamedType("Float")).Encode(),
// 		},
// 	},
// 	"String": schema.ScalarType{
// 		AggregateFunctions: schema.ScalarTypeAggregateFunctions{},
// 		ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 			// "eq":   schema.NewComparisonOperatorEqual().Encode(),
// 			// "in":   schema.NewComparisonOperatorIn().Encode(),
// 			"eq":   schema.NewComparisonOperatorCustom(schema.NewNamedType("String")).Encode(),
// 			"in":   schema.NewComparisonOperatorCustom(schema.NewNamedType("String")).Encode(),
// 			"like": schema.NewComparisonOperatorCustom(schema.NewNamedType("String")).Encode(),
// 		},
// 	},
// }

import (
	"context"

	"github.com/hasura/ndc-sdk-go/schema"
)

type AggregateFunction struct {
	Name       string
	ResultType string
}

type ComparisonOperators struct {
	Name         string
	ArgumentType string
}

type ScalarType struct {
	AggregateFunction   []AggregateFunction
	ComparisonOperators []ComparisonOperators
}

var scalar_types = map[string]ScalarType{
	"Boolean": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "max",
				ResultType: "Boolean",
			},
			{
				Name:       "min",
				ResultType: "Boolean",
			},
			{
				Name:       "sum",
				ResultType: "Boolean",
			},
			{
				Name:       "avg",
				ResultType: "Boolean",
			},
			{
				Name:       "percentiles",
				ResultType: "Boolean",
			},
			{
				Name:       "stats",
				ResultType: "Boolean",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "Boolean",
			},
			{
				Name:         "neq",
				ArgumentType: "Boolean",
			},
			{
				Name:         "gt",
				ArgumentType: "Boolean",
			},
			{
				Name:         "gte",
				ArgumentType: "Boolean",
			},
			{
				Name:         "lt",
				ArgumentType: "Boolean",
			},
			{
				Name:         "lte",
				ArgumentType: "Boolean",
			},
			{
				Name:         "in",
				ArgumentType: "Boolean",
			},
		},
	},
	"Float": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "max",
				ResultType: "Float",
			},
			{
				Name:       "min",
				ResultType: "Float",
			},
			{
				Name:       "sum",
				ResultType: "Float",
			},
			{
				Name:       "avg",
				ResultType: "Float",
			},
			{
				Name:       "percentiles",
				ResultType: "Float",
			},
			{
				Name:       "stats",
				ResultType: "Float",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "Float",
			},
			{
				Name:         "neq",
				ArgumentType: "Float",
			},
			{
				Name:         "gt",
				ArgumentType: "Float",
			},
			{
				Name:         "gte",
				ArgumentType: "Float",
			},
			{
				Name:         "lt",
				ArgumentType: "Float",
			},
			{
				Name:         "lte",
				ArgumentType: "Float",
			},
			{
				Name:         "in",
				ArgumentType: "Float",
			},
		},
	},
	"Int": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "max",
				ResultType: "Int",
			},
			{
				Name:       "min",
				ResultType: "Int",
			},
			{
				Name:       "sum",
				ResultType: "Int",
			},
			{
				Name:       "avg",
				ResultType: "Int",
			},
			{
				Name:       "percentiles",
				ResultType: "Int",
			},
			{
				Name:       "stats",
				ResultType: "Int",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "Int",
			},
			{
				Name:         "neq",
				ArgumentType: "Int",
			},
			{
				Name:         "gt",
				ArgumentType: "Int",
			},
			{
				Name:         "gte",
				ArgumentType: "Int",
			},
			{
				Name:         "lt",
				ArgumentType: "Int",
			},
			{
				Name:         "lte",
				ArgumentType: "Int",
			},
			{
				Name:         "in",
				ArgumentType: "Int",
			},
		},
	},
	"keyword": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "string_stats",
				ResultType: "Int",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "keyword",
			},
			{
				Name:         "in",
				ArgumentType: "keyword",
			},
			{
				Name:         "like",
				ArgumentType: "keyword",
			},
			{
				Name:         "nlike",
				ArgumentType: "keyword",
			},
			{
				Name:         "regexp",
				ArgumentType: "keyword",
			},
		},
	},
	"date": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "max",
				ResultType: "date",
			},
			{
				Name:       "min",
				ResultType: "date",
			},
			{
				Name:       "sum",
				ResultType: "date",
			},
			{
				Name:       "avg",
				ResultType: "date",
			},
			{
				Name:       "cardinality",
				ResultType: "date",
			},
			{
				Name:       "percentiles",
				ResultType: "date",
			},
			{
				Name:       "stats",
				ResultType: "date",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "date",
			},
		},
	},
	"ip": {
		AggregateFunction: []AggregateFunction{
			{
				Name:       "ip_range",
				ResultType: "ip",
			},
		},
		ComparisonOperators: []ComparisonOperators{
			{
				Name:         "eq",
				ArgumentType: "ip",
			},
		},
	},
}

func getScalarTypes(scalartype map[string]bool, ctx context.Context) (schema.SchemaResponseScalarTypes, error) {
	scalartypes := make(schema.SchemaResponseScalarTypes)
	for datatype := range scalartype {
		var scalar schema.ScalarType
		scalar.AggregateFunctions = make(schema.ScalarTypeAggregateFunctions)
		for _, n := range scalar_types[datatype].AggregateFunction {
			scalar.AggregateFunctions[n.Name] = schema.AggregateFunctionDefinition{
				ResultType: schema.NewNullableNamedType(datatype).Encode(),
			}
		}

		scalar.ComparisonOperators = make(map[string]schema.ComparisonOperatorDefinition)
		for _, n := range scalar_types[datatype].ComparisonOperators {
			scalar.ComparisonOperators[n.Name] = schema.NewComparisonOperatorCustom(schema.NewNamedType(datatype)).Encode()
		}

		scalartypes[datatype] = scalar
	}
	return scalartypes, nil
}

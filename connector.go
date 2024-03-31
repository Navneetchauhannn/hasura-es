package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/hasura/ndc-sdk-go-reference/configuration"
	query_engine "github.com/hasura/ndc-sdk-go-reference/query"

	"github.com/hasura/ndc-sdk-go/connector"
	"github.com/hasura/ndc-sdk-go/schema"
	"github.com/hasura/ndc-sdk-go/utils"
)

type Configuration struct{}

type Article struct {
	ID       int    `json:"id"`
	Title    string `json:"title"`
	AuthorID int    `json:"author_id"`
}

type Author struct {
	ID        int    `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

type InstitutionLocation struct {
	City     string   `json:"city"`
	Country  string   `json:"country"`
	Campuses []string `json:"campuses"`
}

type InstitutionStaff struct {
	FirstName    string   `json:"first_name"`
	LastName     string   `json:"last_name"`
	Specialities []string `json:"specialities"`
}

type Institution struct {
	ID          int                 `json:"id"`
	Name        string              `json:"name"`
	Location    InstitutionLocation `json:"location"`
	Staff       []InstitutionStaff  `json:"staff"`
	Departments []string            `json:"departments"`
}

type Connector struct{}

func (mc *Connector) ParseConfiguration(ctx context.Context, rawConfiguration string) (*Configuration, error) {
	return &Configuration{}, nil
}
func (mc *Connector) TryInitState(ctx context.Context, configurations *Configuration, metrics *connector.TelemetryState) (*configuration.State, error) {
	// var Config configuration.Config

	// ymlFile, err := os.ReadFile("C:/Users/navnit.chauhan/Projects/Hasura-Elasticsearch_Connector/elasticsearch-queries/elastic/config.yml")
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// err = yaml.Unmarshal(ymlFile, &Config)
	// if err != nil {
	// 	fmt.Println("Error unmarshaling the data")
	// }
	// fmt.Println(Config.Elasticsearch.Username)
	// cert, _ := ioutil.ReadFile(Config.Elasticsearch.Cert)
	newClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"https://12f248d44c594b04b835c35a7b513e95.us-central1.gcp.cloud.es.io:443"},
		Username:  "enterprise_search",
		Password:  "changeme",
		// CACert:    cert,
	})
	if err != nil {
		panic(err)
	}
	var state configuration.State
	state.ElasticsearchClient = newClient
	fmt.Println("Connection Success!")
	return &state, nil
}

func (mc *Connector) HealthCheck(ctx context.Context, configuration *Configuration, state *configuration.State) error {
	return nil
}

func (mc *Connector) GetCapabilities(configuration *Configuration) schema.CapabilitiesResponseMarshaler {
	return &schema.CapabilitiesResponse{
		Version: "0.1.0",
		Capabilities: schema.Capabilities{
			Query: schema.QueryCapabilities{
				Aggregates: schema.LeafCapability{},
				Variables:  schema.LeafCapability{},
			},
			Relationships: schema.RelationshipCapabilities{
				OrderByAggregate:    schema.LeafCapability{},
				RelationComparisons: schema.LeafCapability{},
			},
		},
	}
}

func (mc *Connector) GetSchema(ctx context.Context, configuration *Configuration, state *configuration.State) (schema.SchemaResponseMarshaler, error) {
	// var schemaRes schema.SchemaResponse
	// schemaB, err := os.ReadFile("configuration.json")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// err = json.Unmarshal(schemaB, &schemaRes)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	return &schema.SchemaResponse{
		ScalarTypes: schema.SchemaResponseScalarTypes{
			"integer": schema.ScalarType{
				AggregateFunctions: schema.ScalarTypeAggregateFunctions{
					"max": schema.AggregateFunctionDefinition{
						ResultType: schema.NewNullableNamedType("integer").Encode(),
					},
					"min": schema.AggregateFunctionDefinition{
						ResultType: schema.NewNullableNamedType("integer").Encode(),
					},
				},
				ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
					"eq": schema.NewComparisonOperatorCustom(schema.NewNamedType("integer")).Encode(),
					"in": schema.NewComparisonOperatorCustom(schema.NewNamedType("integer")).Encode(),
				},
			},
			"Float": schema.ScalarType{
				AggregateFunctions: schema.ScalarTypeAggregateFunctions{
					"max": schema.AggregateFunctionDefinition{
						ResultType: schema.NewNullableNamedType("Float").Encode(),
					},
					"min": schema.AggregateFunctionDefinition{
						ResultType: schema.NewNullableNamedType("Float").Encode(),
					},
				},
				ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
					"eq": schema.NewComparisonOperatorCustom(schema.NewNamedType("Float")).Encode(),
					"in": schema.NewComparisonOperatorCustom(schema.NewNamedType("Float")).Encode(),
				},
			},
			"keyword": schema.ScalarType{
				AggregateFunctions: schema.ScalarTypeAggregateFunctions{},
				ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
					"eq":   schema.NewComparisonOperatorCustom(schema.NewNamedType("keyword")).Encode(),
					"in":   schema.NewComparisonOperatorCustom(schema.NewNamedType("keyword")).Encode(),
					"like": schema.NewComparisonOperatorCustom(schema.NewNamedType("keyword")).Encode(),
				},
			},
		},
		ObjectTypes: schema.SchemaResponseObjectTypes{
			"kibana_sample_data_ecommerce": schema.ObjectType{
				Description: nil,
				Fields: schema.ObjectTypeFields{
					"_id": schema.ObjectField{
						Type:        schema.NewNamedType("keyword").Encode(),
						Description: utils.ToPtr("An author"),
					},
					"category.keyword": schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"currency":         schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_birth_date": schema.ObjectField{
						Type:        schema.NewNullableNamedType("keyword").Encode(),
						Description: utils.ToPtr("handle date object"),
					},
					"customer_first_name.keyword": schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_full_name.keyword":  schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_gender":             schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_id":                 schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_last_name.keyword":  schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"customer_phone":              schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"day_of_week":                 schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"day_of_week_i":               schema.ObjectField{Type: schema.NewNullableNamedType("integer").Encode()},
					"event":                       schema.ObjectField{Type: schema.NewNullableType(schema.NewNullableNamedType("event")).Encode()},
					"geoip":                       schema.ObjectField{Type: schema.NewNullableType(schema.NewNullableNamedType("geoip")).Encode()},
					"manufacturer.keyword":        schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"order_date": schema.ObjectField{
						Type:        schema.NewNullableNamedType("keyword").Encode(),
						Description: utils.ToPtr("handle date object"),
					},
					"products":              schema.ObjectField{Type: schema.NewNullableType(schema.NewNullableNamedType("products")).Encode()},
					"sku":                   schema.ObjectField{Type: schema.NewArrayType(schema.NewNullableNamedType("keyword")).Encode()},
					"taxful_total_price":    schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"taxless_total_price":   schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"total_quantity":        schema.ObjectField{Type: schema.NewNullableNamedType("integer").Encode()},
					"total_unique_products": schema.ObjectField{Type: schema.NewNullableNamedType("integer").Encode()},
					"type":                  schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"user":                  schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
				},
			},
			"event": schema.ObjectType{
				Fields: schema.ObjectTypeFields{
					"dataset": schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
				},
			},
			"geoip": schema.ObjectType{
				Fields: schema.ObjectTypeFields{
					"city_name":        schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"continent_name":   schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"country_iso_code": schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"location":         schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"region_name":      schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
				},
			},
			"products": schema.ObjectType{
				Fields: schema.ObjectTypeFields{
					"_id":             schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"base_price":      schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"base_unit_price": schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"category":        schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"created_on": schema.ObjectField{
						Description: utils.ToPtr("handle date object"),
						Type:        schema.NewNullableNamedType("Float").Encode(),
					},
					"discount_amount":      schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"discount_percentage":  schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"manufacturer":         schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"min_price":            schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"price":                schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"product_name":         schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"quantity":             schema.ObjectField{Type: schema.NewNullableNamedType("integer").Encode()},
					"sku":                  schema.ObjectField{Type: schema.NewNullableNamedType("keyword").Encode()},
					"tax_amount":           schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"taxful_price":         schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"taxless_price":        schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"unit_discount_amount": schema.ObjectField{Type: schema.NewNullableNamedType("Float").Encode()},
					"product_id":           schema.ObjectField{Type: schema.NewNullableNamedType("integer").Encode()}},
			},
		},
		Collections: []schema.CollectionInfo{
			{
				Name:        "kibana_sample_data_ecommerces",
				Description: utils.ToPtr("A collection of ecommerce data"),
				Arguments:   schema.CollectionInfoArguments{},
				Type:        "kibana_sample_data_ecommerce",
				UniquenessConstraints: schema.CollectionInfoUniquenessConstraints{
					"EcommerceByID": schema.UniquenessConstraint{
						UniqueColumns: []string{"_id"},
					},
				},
				ForeignKeys: schema.CollectionInfoForeignKeys{},
			},
		},
		Functions:  []schema.FunctionInfo{},
		Procedures: []schema.ProcedureInfo{},
	}, nil
	// return schemaRes, nil
}

func (mc *Connector) QueryExplain(ctx context.Context, configuration *Configuration, state *configuration.State, request *schema.QueryRequest) (*schema.ExplainResponse, error) {
	return &schema.ExplainResponse{
		Details: schema.ExplainResponseDetails{},
	}, nil
}

func (mc *Connector) MutationExplain(ctx context.Context, configuration *Configuration, state *configuration.State, request *schema.MutationRequest) (*schema.ExplainResponse, error) {
	return &schema.ExplainResponse{
		Details: schema.ExplainResponseDetails{},
	}, nil
}

func (mc *Connector) Query(ctx context.Context, configuration *Configuration, state *configuration.State, request *schema.QueryRequest) (schema.QueryResponse, error) {

	// queryJosn, err := json.Marshal(request)
	// if err != nil {
	// 	fmt.Println("Error Marshling query")
	// }
	// fmt.Printf("QueryRequest: %v", string(queryJosn))

	variableSets := request.Variables
	if variableSets == nil {
		variableSets = []schema.QueryRequestVariablesElem{make(map[string]any)}
	}

	rowSets := make([]schema.RowSet, 0, len(variableSets))

	for _, variables := range variableSets {
		rowSet, err := executeQueryWithVariables(ctx, request.Collection, request.Arguments, request.CollectionRelationships, &request.Query, variables, state)
		if err != nil {
			return nil, err
		}

		rowset, err := json.Marshal(rowSet)
		if err != nil {
			fmt.Println("Error Marshling query")
		}
		fmt.Printf("RowSet: %v", string(rowset))

		rowSets = append(rowSets, *rowSet)
	}
	// fmt.Println("rowSet: ", rowSets)
	return rowSets, nil
}

func (mc *Connector) Mutation(ctx context.Context, configuration *Configuration, state *configuration.State, request *schema.MutationRequest) (*schema.MutationResponse, error) {

	operationResults := []schema.MutationOperationResults{}
	for _, operation := range request.Operations {
		results, err := executeMutationOperation(ctx, state, request.CollectionRelationships, &operation)
		if err != nil {
			return nil, err
		}
		operationResults = append(operationResults, results)
	}

	return &schema.MutationResponse{
		OperationResults: operationResults,
	}, nil
}

func executeMutationOperation(ctx context.Context, state *configuration.State, collectionRelationship schema.MutationRequestCollectionRelationships, operation *schema.MutationOperation) (schema.MutationOperationResults, error) {
	switch operation.Type {
	case schema.MutationOperationProcedure:
		return executeProcedure(ctx, state, collectionRelationship, operation)
	}

	return nil, schema.NotSupportedError(fmt.Sprintf("unsupported operation type: %s", operation.Type), nil)
}

type UpsertArticleArguments struct {
	Article Article `json:"article"`
}

func executeProcedure(_ context.Context, state *configuration.State, collectionRelationships schema.MutationRequestCollectionRelationships, operation *schema.MutationOperation) (schema.MutationOperationResults, error) {
	switch operation.Name {
	case "upsert_article":
		return executeUpsertArticle(state, operation.Arguments, operation.Fields, collectionRelationships)
	case "delete_articles":
		return executeDeleteArticles(state, operation.Arguments, operation.Fields, collectionRelationships)
	default:
		return nil, schema.UnprocessableContentError("unknown procedure", nil)
	}
}

func executeUpsertArticle(
	state *configuration.State,
	arguments json.RawMessage,
	fields schema.NestedField,
	collectionRelationships map[string]schema.Relationship,
) (schema.MutationOperationResults, error) {

	var args UpsertArticleArguments
	if err := json.Unmarshal(arguments, &args); err != nil {
		return nil, schema.UnprocessableContentError(err.Error(), nil)
	}

	var oldRow *Article
	latestArticle := state.GetLatestArticle()
	if args.Article.ID <= 0 {
		if latestArticle == nil {
			args.Article.ID = 1
		}
	} else {
		for i, article := range state.Articles {
			if article.ID == args.Article.ID {
				fmt.Println(i)
			}
		}
		if oldRow == nil {
			// state.Articles = append(state.Articles, args.Article)
		}
	}

	returning, err := evalNestedField(collectionRelationships, nil, state, oldRow, fields)
	if err != nil {
		return nil, err
	}

	return schema.NewProcedureResult(returning).Encode(), nil
}

func executeDeleteArticles(
	state *configuration.State,
	arguments json.RawMessage,
	fields schema.NestedField,
	collectionRelationships map[string]schema.Relationship,
) (schema.MutationOperationResults, error) {
	var argumentData struct {
		Where schema.Expression `json:"where"`
	}
	if err := json.Unmarshal(arguments, &argumentData); err != nil {
		return nil, schema.UnprocessableContentError(err.Error(), nil)
	}
	if len(argumentData.Where) == 0 {
		return nil, schema.UnprocessableContentError("Expected argument 'where'", nil)
	}

	var removed []map[string]any
	for _, article := range state.Articles {
		encodedArticle, err := utils.EncodeObject(article)
		if err != nil {
			return nil, schema.InternalServerError(err.Error(), nil)
		}

		ok, err := evalExpression(nil, nil, state, argumentData.Where, encodedArticle, encodedArticle)
		if err != nil {
			return nil, err
		}
		if ok {
			removed = append(removed, encodedArticle)
		}
	}

	returning, err := evalNestedField(collectionRelationships, nil, state, removed, fields)
	if err != nil {
		return nil, err
	}

	return schema.NewProcedureResult(returning).Encode(), nil
}

func executeQueryWithVariables(
	ctx context.Context,
	collection string,
	arguments map[string]schema.Argument,
	collectionRelationships map[string]schema.Relationship,
	query *schema.Query,
	variables map[string]any,
	state *configuration.State,
) (*schema.RowSet, error) {

	// fmt.Println("Query: ", query)

	argumentValues := make(map[string]schema.Argument)

	for argumentName, argument := range arguments {
		argumentValue, err := evalArgument(variables, &argument)
		if err != nil {
			return nil, err
		}
		argumentValues[argumentName] = schema.Argument{
			Type:  schema.ArgumentTypeLiteral,
			Value: argumentValue,
		}
	}

	return query_engine.ExecuteElasticQuery(ctx, variables, state, query, collection, false)
}

func evalAggregate(aggregate *schema.Aggregate, paginated []map[string]any) (any, error) {
	switch agg := aggregate.Interface().(type) {
	case *schema.AggregateStarCount:
		return len(paginated), nil
	case *schema.AggregateColumnCount:
		var values []string
		for _, value := range paginated {
			v, ok := value[agg.Column]
			if !ok {
				return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", agg.Column), nil)
			}
			if v == nil {
				continue
			}
			values = append(values, fmt.Sprint(v))
		}
		if !agg.Distinct {
			return len(values), nil
		}
		distinctValue := make(map[string]bool)
		for _, v := range values {
			distinctValue[v] = true
		}
		return len(distinctValue), nil
	case *schema.AggregateSingleColumn:
		var values []any
		for _, value := range paginated {
			v, ok := value[agg.Column]
			if !ok {
				return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", agg.Column), nil)
			}
			if v == nil {
				continue
			}
			values = append(values, v)
		}
		return evalAggregateFunction(agg.Function, values)
	default:
		return nil, schema.UnprocessableContentError("invalid aggregate field", map[string]any{
			"value": aggregate,
		})
	}
}

func evalAggregateFunction(function string, values []any) (*int, error) {
	if len(values) == 0 {
		return nil, nil
	}

	var intValues []int
	for _, value := range values {
		switch v := value.(type) {
		case int:
			intValues = append(intValues, v)
		case int16:
			intValues = append(intValues, int(v))
		case int32:
			intValues = append(intValues, int(v))
		case int64:
			intValues = append(intValues, int(v))
		default:
			return nil, schema.UnprocessableContentError(fmt.Sprintf("%s: column is not an integer, got %+v", function, reflect.ValueOf(v).Kind()), nil)
		}
	}

	sort.Ints(intValues)

	switch function {
	case "min":
		return &intValues[0], nil
	case "max":
		return &intValues[len(intValues)-1], nil
	default:
		return nil, schema.UnprocessableContentError(fmt.Sprintf("%s: invalid aggregation function", function), nil)
	}
}

func evalElaticAggregateFunction(function string, column string, aggKey string, query string) (string, error) {

	if function == "min" || function == "max" {
		query += `,
		"aggs": {
			"` + aggKey + `": {
			  "` + function + `": {
				"field": "` + column + `"
			  }
			}
		  }`
		return query, nil
	}

	return query, schema.UnprocessableContentError(fmt.Sprintf("%s: invalid aggregation function", function), nil)
}

func executeQuery(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	query *schema.Query,
	root map[string]any,
	collection []map[string]any,
	skipMappingFields bool,
) (*schema.RowSet, error) {
	sorted, err := sortCollection(collectionRelationships, variables, state, collection, query.OrderBy)
	if err != nil {
		return nil, err
	}

	filtered := sorted
	if len(query.Predicate) > 0 {
		filtered = []map[string]any{}
		for _, item := range sorted {
			rootItem := root
			if rootItem == nil {
				rootItem = item
			}
			ok, err := evalExpression(collectionRelationships, variables, state, query.Predicate, rootItem, item)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, item)
			}
		}
	}

	paginated := paginate(filtered, query.Limit, query.Offset)
	aggregates := make(map[string]any)

	for aggKey, aggregate := range query.Aggregates {
		aggValue, err := evalAggregate(&aggregate, paginated)
		if err != nil {
			return nil, err
		}
		aggregates[aggKey] = aggValue
	}

	rows := paginated
	if !skipMappingFields {
		rows = make([]map[string]any, 0)
		for _, item := range paginated {
			row, err := evalRow(query.Fields, collectionRelationships, variables, state, item)
			if err != nil {
				return nil, err
			}
			if row != nil {
				rows = append(rows, row)
			}
		}
	}

	return &schema.RowSet{
		Aggregates: aggregates,
		Rows:       rows,
	}, nil
}

func sortCollection(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	collection []map[string]any,
	orderBy *schema.OrderBy,
) ([]map[string]any, error) {
	if orderBy == nil || len(orderBy.Elements) == 0 {
		return collection, nil
	}

	var results []map[string]any
	for _, itemToInsert := range collection {
		if len(results) == 0 {
			results = append(results, itemToInsert)
			continue
		}
		inserted := false
		newResults := []map[string]any{}
		for _, other := range results {
			ordering, err := evalOrderBy(collectionRelationships, variables, state, orderBy, other, itemToInsert)
			if err != nil {
				return nil, err
			}
			if ordering > 0 {
				newResults = append(newResults, itemToInsert, other)
				inserted = true
			} else {
				newResults = append(newResults, other)
			}
		}

		if !inserted {
			newResults = append(newResults, itemToInsert)
		}

		results = newResults
	}
	return results, nil
}

func paginate[R any](collection []R, limit *int, offset *int) []R {
	var start int
	if offset != nil {
		start = *offset
	}
	if limit == nil {
		return collection[start:]
	}

	return collection[start : start+*limit]
}

func evalOrderBy(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	orderBy *schema.OrderBy,
	t1 map[string]any,
	t2 map[string]any,
) (int, error) {
	ordering := 0
	for _, orderElem := range orderBy.Elements {
		v1, err := evalOrderByElement(collectionRelationships, variables, state, &orderElem, t1)
		if err != nil {
			return 0, err
		}
		v2, err := evalOrderByElement(collectionRelationships, variables, state, &orderElem, t2)
		if err != nil {
			return 0, err
		}
		switch orderElem.OrderDirection {
		case schema.OrderDirectionAsc:
			// FIXME: compose ordering
			ordering, err = compare(v1, v2)
			if err != nil {
				return 0, err
			}
		case schema.OrderDirectionDesc:
			ordering, err = compare(v2, v1)
			if err != nil {
				return 0, err
			}
		}
		if ordering != 0 {
			return ordering, nil
		}
	}

	return ordering, nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func compare(v1 any, v2 any) (int, error) {
	if v1 == v2 || (v1 == nil && v2 == nil) {
		return 0, nil
	}
	if v1 == nil {
		return -1, nil
	}
	if v2 == nil {
		return 1, nil
	}

	value1 := reflect.ValueOf(v1)
	kindV1 := value1.Kind()
	value2 := reflect.ValueOf(v2)
	kindV2 := value2.Kind()

	if kindV1 != kindV2 {
		return 0, schema.InternalServerError(fmt.Sprintf("cannot compare values with different types: %s <> %s", kindV1, kindV2), nil)
	}

	if kindV1 == reflect.Pointer {
		return compare(value1.Elem().Interface(), value2.Elem().Interface())
	}

	switch value1 := v1.(type) {
	case bool:
		value2 := v2.(bool)
		return boolToInt(value1) - boolToInt(value2), nil
	case int:
		value2 := v2.(int)
		return value1 - value2, nil
	case int8:
		value2 := v2.(int8)
		return int(value1 - value2), nil
	case int16:
		value2 := v2.(int16)
		return int(value1 - value2), nil
	case int32:
		value2 := v2.(int32)
		return int(value1 - value2), nil
	case int64:
		value2 := v2.(int64)
		return int(value1 - value2), nil
	case string:
		value2 := v2.(string)
		return strings.Compare(value1, value2), nil
	default:
		rawV1, _ := json.Marshal(v1)
		return 0, schema.InternalServerError(fmt.Sprintf("cannot compare values with type: %s, value: %s", kindV1, string(rawV1)), nil)
	}
}

func evalOrderByElement(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	element *schema.OrderByElement,
	item map[string]any,
) (any, error) {
	switch target := element.Target.Interface().(type) {
	case *schema.OrderByColumn:
		return evalOrderByColumn(collectionRelationships, variables, state, item, target.Path, target.Name)
	case *schema.OrderBySingleColumnAggregate:
		return evalOrderBySingleColumnAggregate(collectionRelationships, variables, state, item, target.Path, target.Column, target.Function)
	case *schema.OrderByStarCountAggregate:
		return evalOrderByStarCountAggregate(collectionRelationships, variables, state, item, target.Path)
	default:
		return nil, schema.UnprocessableContentError("invalid order by field", map[string]any{
			"value": element.Target,
		})
	}
}

func evalOrderByStarCountAggregate(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	item map[string]any,
	path []schema.PathElement,
) (int, error) {
	rows, err := evalPath(collectionRelationships, variables, state, path, item)

	return len(rows), err
}

func evalOrderBySingleColumnAggregate(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	item map[string]any,
	path []schema.PathElement,
	column string,
	function string,
) (any, error) {
	rows, err := evalPath(collectionRelationships, variables, state, path, item)
	if err != nil {
		return nil, err
	}

	var values []any
	for _, row := range rows {
		value, ok := row[column]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", column), nil)
		}
		values = append(values, value)
	}
	return evalAggregateFunction(function, values)
}

func evalOrderByColumn(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	item map[string]any,
	path []schema.PathElement,
	name string,
) (any, error) {
	rows, err := evalPath(collectionRelationships, variables, state, path, item)
	if err != nil {
		return nil, err
	}
	if len(rows) > 1 {
		return nil, schema.UnprocessableContentError("expected one path value only", nil)
	}
	if len(rows) == 0 || rows[0] == nil {
		return nil, nil
	}
	value, ok := rows[0][name]
	if !ok {
		return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", name), nil)
	}
	return value, nil
}

func evalInCollection(
	collectionRelationships map[string]schema.Relationship,
	item map[string]any,
	variables map[string]any,
	state *configuration.State,
	inCollection schema.ExistsInCollection,
) ([]map[string]any, error) {
	switch inCol := inCollection.Interface().(type) {
	case *schema.ExistsInCollectionRelated:
		relationship, ok := collectionRelationships[inCol.Relationship]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid in collection relationship: %s", inCol.Relationship), nil)
		}
		source := []map[string]any{item}
		return evalPathElement(collectionRelationships, variables, state, &relationship, inCol.Arguments, source, nil)
	case *schema.ExistsInCollectionUnrelated:
		arguments := make(map[string]schema.Argument)
		for key, relArg := range inCol.Arguments {
			argValue, err := evalRelationshipArgument(variables, item, &relArg)
			if err != nil {
				return nil, err
			}
			arguments[key] = schema.Argument{
				Type:  schema.ArgumentTypeLiteral,
				Value: argValue,
			}
		}
		return getCollectionByName(inCol.Collection, arguments, state)
	default:
		return nil, schema.UnprocessableContentError("invalid in collection field", map[string]any{
			"value": inCollection,
		})
	}
}

func evalRow(fields map[string]schema.Field, collectionRelationships map[string]schema.Relationship, variables map[string]any, state *configuration.State, item map[string]any) (map[string]any, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	row := make(map[string]any)
	for fieldName, field := range fields {
		fieldValue, err := evalField(collectionRelationships, variables, state, field, item)
		if err != nil {
			return nil, err
		}
		row[fieldName] = fieldValue
	}

	return row, nil
}

func evalNestedField(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	value any,
	nestedField schema.NestedField,
) (any, error) {

	if utils.IsNil(value) {
		return value, nil
	}
	switch nf := nestedField.Interface().(type) {
	case *schema.NestedObject:
		fullRow, err := utils.EncodeObject(value)
		if err != nil {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("expected object, got %s", reflect.ValueOf(value).Kind()), nil)
		}

		return evalRow(nf.Fields, collectionRelationships, variables, state, fullRow)
	case *schema.NestedArray:
		array, err := utils.EncodeObjects(value)
		if err != nil {
			return nil, err
		}

		result := []any{}
		for _, item := range array {
			val, err := evalNestedField(collectionRelationships, variables, state, item, nf.Fields)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil
	default:
		return nil, schema.UnprocessableContentError("invalid nested field", map[string]any{
			"value": nestedField,
		})
	}
}

func evalField(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	field schema.Field,
	row map[string]any,
) (any, error) {
	switch f := field.Interface().(type) {
	case *schema.ColumnField:
		value, ok := row[f.Column]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", f.Column), nil)
		}
		if len(f.Fields) == 0 {
			return value, nil
		}
		return evalNestedField(collectionRelationships, variables, state, value, f.Fields)
	case *schema.RelationshipField:
		relationship, ok := collectionRelationships[f.Relationship]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid relationship name %s", f.Relationship), nil)
		}

		collection, err := evalPathElement(collectionRelationships, variables, state, &relationship, f.Arguments, []map[string]any{row}, nil)
		if err != nil {
			return nil, err
		}

		return executeQuery(collectionRelationships, variables, state, &f.Query, nil, collection, false)

	default:
		return nil, schema.UnprocessableContentError("invalid field", map[string]any{
			"value": field,
		})
	}
}

func evalPathElement(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	relationship *schema.Relationship,
	arguments map[string]schema.RelationshipArgument,
	source []map[string]any,
	predicate schema.Expression,
) ([]map[string]any, error) {
	allArguments := make(map[string]schema.Argument)
	var matchingRows []map[string]any

	// Note: Join strategy
	//
	// Rows can be related in two ways: 1) via a column mapping, and
	// 2) via collection arguments. Because collection arguments can be computed
	// using the columns on the source side of a relationship, in general
	// we need to compute the target collection once for each source row.
	// This join strategy can result in some target rows appearing in the
	// resulting row set more than once, if two source rows are both related
	// to the same target row.
	//
	// In practice, this is not an issue, either because a) the relationship
	// is computed in the course of evaluating a predicate, and all predicates are
	// implicitly or explicitly existentially quantified, or b) if the
	// relationship is computed in the course of evaluating an ordering, the path
	// should consist of all object relationships, and possibly terminated by a
	// single array relationship, so there should be no double counting.
	for _, srcRow := range source {

		for argName, arg := range relationship.Arguments {
			relValue, err := evalRelationshipArgument(variables, srcRow, &arg)
			if err != nil {
				return nil, err
			}
			allArguments[argName] = schema.Argument{
				Type:  schema.ArgumentTypeLiteral,
				Value: relValue,
			}
		}
		for argName, arg := range arguments {
			if _, ok := allArguments[argName]; ok {
				return nil, schema.UnprocessableContentError(fmt.Sprintf("duplicate argument name: %s", argName), nil)
			}
			relValue, err := evalRelationshipArgument(variables, srcRow, &arg)
			if err != nil {
				return nil, err
			}
			allArguments[argName] = schema.Argument{
				Type:  schema.ArgumentTypeLiteral,
				Value: relValue,
			}
		}

		targetRows, err := getCollectionByName(relationship.TargetCollection, allArguments, state)
		if err != nil {
			return nil, err
		}

		for _, targetRow := range targetRows {
			ok, err := evalColumnMapping(relationship, srcRow, targetRow)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}

			if predicate != nil {
				ok, err := evalExpression(collectionRelationships, variables, state, predicate, targetRow, targetRow)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			matchingRows = append(matchingRows, targetRow)
		}
	}

	return matchingRows, nil
}

func evalRelationshipArgument(variables map[string]any, row map[string]any, argument *schema.RelationshipArgument) (any, error) {
	switch argument.Type {
	case schema.RelationshipArgumentTypeColumn:
		value, ok := row[argument.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid column name: %s", argument.Name), nil)
		}
		return value, nil
	case schema.RelationshipArgumentTypeLiteral:
		return argument.Value, nil
	case schema.RelationshipArgumentTypeVariable:
		variable, ok := variables[argument.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", argument.Name), nil)
		}
		return variable, nil
	default:
		return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid argument type: %s", argument.Type), nil)
	}
}

func getCollectionByName(collectionName string, arguments schema.QueryRequestArguments, state *configuration.State) ([]map[string]any, error) {
	var rows []map[string]any
	switch collectionName {
	// function
	case "latest_article_id":
		latestArticle := state.GetLatestArticle()
		var latestID *int
		if latestArticle != nil {
			// latestID = &latestArticle.ID
		}
		return []map[string]any{
			{
				"__value": latestID,
			},
		}, nil
	case "latest_article":
		return []map[string]any{
			{
				"__value": state.GetLatestArticle(),
			},
		}, nil
		// collections
	case "articles":
		for _, item := range state.Articles {
			row, err := utils.EncodeObject(item)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	case "authors":
		for _, item := range state.Authors {
			row, err := utils.EncodeObject(item)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	case "institutions":
		for _, item := range state.Institutions {
			row, err := utils.EncodeObject(item)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	case "articles_by_author":
		authorIdArg, ok := arguments["author_id"]
		if !ok {
			return nil, schema.UnprocessableContentError("missing argument author_id", nil)
		}

		for _, row := range state.Articles {
			switch authorIdArg.Type {
			case schema.ArgumentTypeLiteral:
				if fmt.Sprint(row.AuthorID) == fmt.Sprint(authorIdArg.Value) {
					r, err := utils.EncodeObject(row)
					if err != nil {
						return nil, err
					}
					rows = append(rows, r)
				}
			}
		}
	default:
		return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid collection name %s", collectionName), nil)
	}

	return rows, nil
}

func evalComparisonValue(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	comparisonValue schema.ComparisonValue,
	root map[string]any,
	item map[string]any,
) ([]any, error) {
	switch compValue := comparisonValue.Interface().(type) {
	case *schema.ComparisonValueColumn:
		return evalComparisonTarget(collectionRelationships, variables, state, &compValue.Column, root, item)
	case *schema.ComparisonValueScalar:
		return []any{compValue.Value}, nil
	case *schema.ComparisonValueVariable:
		if len(variables) == 0 {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", compValue.Name), nil)
		}
		val, ok := variables[compValue.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", compValue.Name), nil)
		}
		return []any{val}, nil
	default:
		return nil, schema.UnprocessableContentError("invalid comparison value", map[string]any{
			"value": comparisonValue,
		})
	}
}

func evalComparisonTarget(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	target *schema.ComparisonTarget,
	root map[string]any,
	item map[string]any,
) ([]any, error) {
	switch target.Type {
	case schema.ComparisonTargetTypeColumn:
		rows, err := evalPath(collectionRelationships, variables, state, target.Path, item)
		if err != nil {
			return nil, err
		}
		var result []any
		for _, row := range rows {
			value, ok := row[target.Name]
			if !ok {
				return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid comparison target column name: %s", target.Name), nil)
			}
			result = append(result, value)
		}
		return result, nil
	case schema.ComparisonTargetTypeRootCollectionColumn:
		value, ok := root[target.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid comparison target column name: %s", target.Name), nil)
		}
		return []any{value}, nil
	default:
		return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid comparison target type: %s", target.Type), nil)
	}
}

func evalPath(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	path []schema.PathElement,
	item map[string]any,
) ([]map[string]any, error) {
	var err error
	result := []map[string]any{item}

	for _, pathElem := range path {
		relationshipName := pathElem.Relationship
		relationship, ok := collectionRelationships[relationshipName]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid relationship name in path: %s", relationshipName), nil)
		}
		result, err = evalPathElement(collectionRelationships, variables, state, &relationship, pathElem.Arguments, result, pathElem.Predicate)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func evalExpression(
	collectionRelationships map[string]schema.Relationship,
	variables map[string]any,
	state *configuration.State,
	expr schema.Expression,
	root map[string]any,
	item map[string]any,
) (bool, error) {
	switch expression := expr.Interface().(type) {
	case *schema.ExpressionAnd:
		for _, exp := range expression.Expressions {
			ok, err := evalExpression(collectionRelationships, variables, state, exp, root, item)
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	case *schema.ExpressionOr:
		for _, exp := range expression.Expressions {
			ok, err := evalExpression(collectionRelationships, variables, state, exp, root, item)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}
		return false, nil
	case *schema.ExpressionNot:
		ok, err := evalExpression(collectionRelationships, variables, state, expression.Expression, root, item)
		if err != nil {
			return false, err
		}
		return !ok, nil
	case *schema.ExpressionUnaryComparisonOperator:
		switch expression.Operator {
		case schema.UnaryComparisonOperatorIsNull:
			values, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			if err != nil {
				return false, err
			}
			for _, val := range values {
				if val == nil {
					return true, nil
				}
			}
			return false, nil
		default:
			return false, schema.UnprocessableContentError(fmt.Sprintf("invalid unary comparison operator: %s", expression.Operator), nil)
		}
	case *schema.ExpressionBinaryComparisonOperator:
		switch expression.Operator {
		case "eq":
			leftValues, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			if err != nil {
				return false, err
			}
			rightValues, err := evalComparisonValue(collectionRelationships, variables, state, expression.Value, root, item)
			if err != nil {
				return false, err
			}

			for _, leftVal := range leftValues {
				for _, rightVal := range rightValues {
					// TODO: coalesce equality
					if leftVal == rightVal || fmt.Sprint(leftVal) == fmt.Sprint(rightVal) {
						return true, nil
					}
				}
			}
			return false, nil
		case "like":
			columnValues, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			if err != nil {
				return false, err
			}
			regexValues, err := evalComparisonValue(collectionRelationships, variables, state, expression.Value, root, item)
			if err != nil {
				return false, err
			}

			for _, columnValue := range columnValues {

				columnStr, ok := columnValue.(string)
				if !ok {
					return false, schema.UnprocessableContentError(fmt.Sprintf("value of column %s is not a string, got %+v", expression.Column, columnValue), nil)
				}
				for _, rawRegex := range regexValues {
					regexStr, ok := rawRegex.(string)
					if !ok {
						return false, schema.UnprocessableContentError(fmt.Sprintf("invalid regular expression, got %+v", rawRegex), nil)
					}

					regex, err := regexp.Compile(regexStr)
					if err != nil {
						return false, schema.UnprocessableContentError(fmt.Sprintf("invalid regular expression: %s", err), nil)
					}

					if regex.Match([]byte(columnStr)) {
						return true, nil
					}
				}
			}

			return false, nil
		case "in":
			leftValues, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			if err != nil {
				return false, err
			}
			rightValueSets, err := evalComparisonValue(collectionRelationships, variables, state, expression.Value, root, item)
			if err != nil {
				return false, err
			}
			for _, rightValueSet := range rightValueSets {
				rightValues, ok := rightValueSet.([]any)
				if !ok {
					return false, schema.UnprocessableContentError(fmt.Sprintf("expected array, got %+v", rightValueSet), nil)
				}
				for _, leftVal := range leftValues {
					for _, rightVal := range rightValues {
						// TODO: coalesce equality
						if leftVal == rightVal || fmt.Sprint(leftVal) == fmt.Sprint(rightVal) {
							return true, nil
						}
					}
				}
			}
			return false, nil
		default:
			return false, schema.UnprocessableContentError(fmt.Sprintf("invalid comparison operator: %s", expression.Operator), nil)
		}
	case *schema.ExpressionExists:
		query := &schema.Query{
			Predicate: expression.Predicate,
		}
		collection, err := evalInCollection(collectionRelationships, item, variables, state, expression.InCollection)
		if err != nil {
			return false, err
		}

		rowSet, err := executeQuery(collectionRelationships, variables, state, query, root, collection, true)
		if err != nil {
			return false, err
		}

		return len(rowSet.Rows) > 0, nil
	default:
		return false, schema.UnprocessableContentError("invalid expression", map[string]any{
			"value": expr,
		})
	}
}

func evalArgument(variables map[string]any, argument *schema.Argument) (any, error) {
	switch argument.Type {
	case schema.ArgumentTypeVariable:
		value, ok := variables[argument.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", argument.Name), nil)
		}
		return value, nil
	case schema.ArgumentTypeLiteral:
		return argument.Value, nil
	default:
		return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid argument type: %s", argument.Type), nil)
	}
}

func evalColumnMapping(relationship *schema.Relationship, srcRow map[string]any, target map[string]any) (bool, error) {
	for srcColumn, targetColumn := range relationship.ColumnMapping {
		srcValue, ok := srcRow[srcColumn]
		if !ok {
			return false, schema.UnprocessableContentError(fmt.Sprintf("source column does not exist: %s", srcColumn), nil)
		}
		targetValue, ok := target[targetColumn]
		if !ok {
			return false, schema.UnprocessableContentError(fmt.Sprintf("target column does not exist: %s", targetColumn), nil)
		}
		if srcValue != targetValue {
			return false, nil
		}
	}
	return true, nil
}

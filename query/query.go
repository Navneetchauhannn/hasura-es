package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hasura/ndc-sdk-go-reference/configuration"

	"github.com/hasura/ndc-sdk-go/schema"
)

type QueryDSL struct {
	Source []string                 `json:"_source,omitempty"`
	Query  map[string]interface{}   `json:"query,omitempty"`
	From   int                      `json:"from,omitempty"`
	Size   int                      `json:"size,omitempty"`
	Pit    map[string]interface{}   `json:"pit,omitempty"`
	Sort   []map[string]interface{} `json:"sort,omitempty"`
	Aggs   map[string]interface{}   `json:"aggs,omitempty"`
}

func ExecuteElasticQuery(
	ctx context.Context,
	variables map[string]any,
	state *configuration.State,
	query *schema.Query,
	collection string,
	skipMappingFields bool,
) (*schema.RowSet, error) {
	var queryDSL QueryDSL

	for fieldName := range query.Fields {
		queryDSL.Source = append(queryDSL.Source, fieldName)
	}

	queryDSL.Query = make(map[string]interface{})
	if len(query.Predicate) > 0 {
		err := evalElasticExpression(variables, state, query.Predicate, &queryDSL)
		if err != nil {
			return nil, err
		}
	} else {
		queryDSL.Query["match_all"] = struct{}{}
	}

	queryDSL.Sort = make([]map[string]interface{}, 0)
	err := sortElasticCollection(variables, state, query.OrderBy, &queryDSL)
	if err != nil {
		fmt.Println("err: ", err)
		return nil, err
	}

	queryDSL.Aggs = make(map[string]interface{})
	for aggKey, aggregate := range query.Aggregates {
		err = evalElasticAggregate(&aggregate, aggKey, &queryDSL)
		if err != nil {
			return nil, err
		}
	}

	queryDSL.Pit = make(map[string]any)
	paginateElastic(state, query.Limit, query.Offset, &queryDSL)

	query_result, err := performQuery(state, collection, &queryDSL)
	if err != nil {
		fmt.Println("Error performing query:", err)
		return nil, err
	}

	rows := handleFieldResponse(&queryDSL, query_result)
	aggregate := handleAggregateResponse(query_result)

	return &schema.RowSet{
		Aggregates: aggregate,
		Rows:       rows,
	}, nil
}

func evalElasticExpression(
	variables map[string]any,
	state *configuration.State,
	expr schema.Expression,
	queryDSL *QueryDSL,
) error {
	switch expression := expr.Interface().(type) {
	case *schema.ExpressionAnd:
		for _, exp := range expression.Expressions {
			err := evalElasticExpression(variables, state, exp, queryDSL)
			if err != nil {
				return err
			}
		}
		return nil
	case *schema.ExpressionOr:
		for _, exp := range expression.Expressions {
			err := evalElasticExpression(variables, state, exp, queryDSL)
			if err != nil {
				return err
			}
		}
		return nil
	case *schema.ExpressionNot:
		err := evalElasticExpression(variables, state, expression.Expression, queryDSL)
		if err != nil {
			return err
		}
		return nil
	case *schema.ExpressionUnaryComparisonOperator:
		switch expression.Operator {
		case schema.UnaryComparisonOperatorIsNull:
			// values, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			// if err != nil {
			// 	return false, err
			// }
			// for _, val := range values {
			// 	if val == nil {
			// 		return true, nil
			// 	}
			// }
			return nil
		default:
			return schema.UnprocessableContentError(fmt.Sprintf("invalid unary comparison operator: %s", expression.Operator), nil)
		}
	case *schema.ExpressionBinaryComparisonOperator:
		switch expression.Operator {
		case "eq":
			rightValue, err := evalElasticComparisonValue(variables, state, expression.Value)
			if err != nil {
				return err
			}

			match := make(map[string]interface{})
			match[expression.Column.Name] = rightValue.(string)
			queryDSL.Query["term"] = match
			return nil
		case "like":
			rightValue, err := evalElasticComparisonValue(variables, state, expression.Value)
			if err != nil {
				return err
			}

			match := make(map[string]interface{})
			match[expression.Column.Name] = rightValue.(string)
			queryDSL.Query["match"] = match
			return nil
		case "in":
			// leftValues, err := evalComparisonTarget(collectionRelationships, variables, state, &expression.Column, root, item)
			// if err != nil {
			// 	return false, err
			// }
			// rightValueSets, err := evalComparisonValue(collectionRelationships, variables, state, expression.Value, root, item)
			// if err != nil {
			// 	return false, err
			// }
			// for _, rightValueSet := range rightValueSets {
			// 	rightValues, ok := rightValueSet.([]any)
			// 	if !ok {
			// 		return false, schema.UnprocessableContentError(fmt.Sprintf("expected array, got %+v", rightValueSet), nil)
			// 	}
			// 	for _, leftVal := range leftValues {
			// 		for _, rightVal := range rightValues {
			// 			// TODO: coalesce equality
			// 			if leftVal == rightVal || fmt.Sprint(leftVal) == fmt.Sprint(rightVal) {
			// 				return true, nil
			// 			}
			// 		}
			// 	}
			// }
			return nil
		default:
			return schema.UnprocessableContentError(fmt.Sprintf("invalid comparison operator: %s", expression.Operator), nil)
		}
	case *schema.ExpressionExists:
		// query := &schema.Query{
		// 	Predicate: expression.Predicate,
		// }
		// collection, err := evalInCollection(collectionRelationships, item, variables, state, expression.InCollection)
		// if err != nil {
		// 	return false, err
		// }

		// rowSet, err := executeQuery(collectionRelationships, variables, state, query, root, collection, true)
		// if err != nil {
		// 	return false, err
		// }

		// return len(rowSet.Rows) > 0, nil
		return nil
	default:
		return schema.UnprocessableContentError("invalid expression", map[string]any{
			"value": expr,
		})
	}
}

func paginateElastic(state *configuration.State, limit *int, offset *int, query *QueryDSL) {
	if offset != nil {
		query.From = *offset
	}

	if limit != nil {
		query.Size = *limit
	}

	if isPitExpired() {
		PitID, err := createPointInTime(state.ElasticsearchClient)
		if err != nil {
			fmt.Println("Error creating pit")
			return
		}
		fmt.Println("PIT EXPIRED, CREATING PIT: ", PitID)
	}
	if pitID != "" {
		query.Pit["id"] = pitID
		query.Pit["keep_alive"] = "1m"
	}
}

func sortElasticCollection(
	variables map[string]any,
	state *configuration.State,
	orderBy *schema.OrderBy,
	query *QueryDSL,
) error {
	if orderBy == nil || len(orderBy.Elements) == 0 {
		return nil
	}

	for _, orderElem := range orderBy.Elements {
		switch target := orderElem.Target.Interface().(type) {

		case *schema.OrderByColumn:
			if target.Name != "_id" {
				order := make(map[string]interface{})
				sort := make(map[string]interface{})
				order["order"] = string(orderElem.OrderDirection)
				sort[target.Name] = order
				query.Sort = append(query.Sort, sort)
			}

		// case *schema.OrderBySingleColumnAggregate:
		// 	return evalOrderBySingleColumnAggregate(collectionRelationships, variables, state, item, target.Path, target.Column, target.Function)
		// case *schema.OrderByStarCountAggregate:
		// 	return evalOrderByStarCountAggregate(collectionRelationships, variables, state, item, target.Path)
		default:
			return schema.UnprocessableContentError("invalid order by field", map[string]any{
				"value": orderElem.Target,
			})
		}
	}
	return nil
}

func evalElasticAggregate(aggregate *schema.Aggregate, aggKey string, query *QueryDSL) error {
	switch agg := aggregate.Interface().(type) {
	case *schema.AggregateStarCount:
		field := make(map[string]interface{})
		field["script"] = "1"
		function := make(map[string]any)
		function["value_count"] = field
		query.Aggs[aggKey] = function
		return nil
	case *schema.AggregateColumnCount:
		term := map[string]any{
			"script": map[string]any{
				"source": "doc.containsKey('" + agg.Column + "')",
			},
		}

		query.Aggs[aggKey] = map[string]any{
			"terms": term,
		}
		return nil
	case *schema.AggregateSingleColumn:
		// query, err := evalElaticAggregateFunction(agg.Function, agg.Column, aggKey, query)

		if agg.Function == "min" || agg.Function == "max" {
			field := make(map[string]interface{})
			function := make(map[string]any)
			if agg.Column == "_id" {
				field["script"] = "1"
				function["value_count"] = field
			} else {
				field["field"] = agg.Column
				function[agg.Function] = field
			}
			query.Aggs[aggKey] = function
			return nil
		}
		return schema.UnprocessableContentError(fmt.Sprintf("%s: invalid aggregation function", agg.Function), nil)
	default:
		return schema.UnprocessableContentError("invalid aggregate field", map[string]any{
			"value": aggregate,
		})
	}
}

func performQuery(state *configuration.State, index string, queryDSL *QueryDSL) (map[string]any, error) {
	// cert, _ := ioutil.ReadFile("C:/Users/navnit.chauhan/L&D/Go/elasticsearch/ca-cert.pem")
	// client, err := elasticsearch.NewClient(elasticsearch.Config{
	// 	Addresses: []string{"https://12f248d44c594b04b835c35a7b513e95.us-central1.gcp.cloud.es.io:443"},
	// 	Username:  "enterprise_search",
	// 	Password:  "changeme",
	// 	// CACert:    cert,
	// })
	// if err != nil {
	// 	panic(err)
	// }
	client := state.ElasticsearchClient
	// Perform the search request
	query, err := json.Marshal(queryDSL)
	if err != nil {
		fmt.Println("Error Marshaling Query")
		return nil, err
	}
	fmt.Printf("QueryDSL :%v", string(query))
	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		// client.Search.WithIndex("kibana_sample_data_ecommerce"),
		client.Search.WithBody(strings.NewReader(string(query))),
		client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	// Handle response
	query_result := make(map[string]any)

	if err := json.NewDecoder(res.Body).Decode(&query_result); err != nil {
		fmt.Println("Error parsing response body:", err)
		return nil, err
	}
	// Extract _id and category fields and put them into the empty map
	// total_hits := row["hits"].(map[string]any)["total"].(map[string]any)["value"]
	// fmt.Println(total_hits)
	return query_result, nil
}

func evalElasticComparisonValue(
	variables map[string]any,
	state *configuration.State,
	comparisonValue schema.ComparisonValue,
) (any, error) {
	switch compValue := comparisonValue.Interface().(type) {
	case *schema.ComparisonValueColumn:
		return "fn", nil
	case *schema.ComparisonValueScalar:
		return compValue.Value, nil
	case *schema.ComparisonValueVariable:
		if len(variables) == 0 {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", compValue.Name), nil)
		}
		val, ok := variables[compValue.Name]
		if !ok {
			return nil, schema.UnprocessableContentError(fmt.Sprintf("invalid variable name: %s", compValue.Name), nil)
		}
		return val, nil
	default:
		return nil, schema.UnprocessableContentError("invalid comparison value", map[string]any{
			"value": comparisonValue,
		})
	}
}

func handleFieldResponse(queryDSL *QueryDSL, query_result map[string]any) []map[string]any {
	rows := make([]map[string]any, 0)

	hits := query_result["hits"].(map[string]any)["hits"].([]any)
	for _, hit := range hits {

		hitData := hit.(map[string]any)
		if queryDSL.Source != nil {
			extractedData := make(map[string]any)
			for _, field := range queryDSL.Source {
				extractedData[field] = nil
			}
			source := hitData["_source"].(map[string]any)
			for key, value := range source {
				if value == "_id" {
					extractedData["_id"] = hitData["_id"]
				}
				extractedData[key] = value
			}
			rows = append(rows, extractedData)
		}
	}

	return rows
}

func handleAggregateResponse(row map[string]any) map[string]any {
	aggregate := make(map[string]any)

	row_aggs, ok := row["aggregations"].(map[string]any)
	if ok {
		for aggs_name, aggs := range row_aggs {
			aggs := aggs.(map[string]any)
			aggregate[aggs_name] = aggs["value"]
			buckets, ok := aggs["buckets"].([]any)
			if ok {
				for _, bucket := range buckets {
					bucket := bucket.(map[string]any)
					aggregate[aggs_name] = bucket["doc_count"]
				}
			}
		}
	}

	return aggregate
}

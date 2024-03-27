// Schema := schema.SchemaResponse{
// 	ScalarTypes: schema.SchemaResponseScalarTypes{
// 		"Int": schema.ScalarType{
// 			AggregateFunctions: schema.ScalarTypeAggregateFunctions{
// 				"max": schema.AggregateFunctionDefinition{
// 					ResultType: schema.NewNullableNamedType("Int").Encode(),
// 				},
// 				"min": schema.AggregateFunctionDefinition{
// 					ResultType: schema.NewNullableNamedType("Int").Encode(),
// 				},
// 			},
// 			ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 				"eq": schema.NewComparisonOperatorEqual().Encode(),
// 				"in": schema.NewComparisonOperatorIn().Encode(),
// 			},
// 		},
// 		"Float": schema.ScalarType{
// 			AggregateFunctions: schema.ScalarTypeAggregateFunctions{
// 				"max": schema.AggregateFunctionDefinition{
// 					ResultType: schema.NewNullableNamedType("Float").Encode(),
// 				},
// 				"min": schema.AggregateFunctionDefinition{
// 					ResultType: schema.NewNullableNamedType("Float").Encode(),
// 				},
// 			},
// 			ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 				"eq": schema.NewComparisonOperatorEqual().Encode(),
// 				"in": schema.NewComparisonOperatorIn().Encode(),
// 			},
// 		},
// 		"String": schema.ScalarType{
// 			AggregateFunctions: schema.ScalarTypeAggregateFunctions{},
// 			ComparisonOperators: map[string]schema.ComparisonOperatorDefinition{
// 				"eq":   schema.NewComparisonOperatorEqual().Encode(),
// 				"in":   schema.NewComparisonOperatorIn().Encode(),
// 				"like": schema.NewComparisonOperatorCustom(schema.NewNamedType("String")).Encode(),
// 			},
// 		},
// 	},
// 	ObjectTypes: schema.SchemaResponseObjectTypes{
// 		`"indexName"`: schema.ObjectType{
// 			Description: nil,
// 			Fields: schema.ObjectTypeFields{
// 				"_id": schema.ObjectField{
// 					Type:        schema.NewNamedType("String").Encode(),
// 					Description: utils.ToPtr("Document id"),
// 				},

// 				"category": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"currency": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_birth_date": schema.ObjectField{
// 					Type:        schema.NewNamedType("String").Encode(),
// 					Description: utils.ToPtr("handle date object"),
// 				},
// 				"customer_first_name": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_full_name":  schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_gender":     schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_id":         schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_last_name":  schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"customer_phone":      schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"day_of_week":         schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"day_of_week_i":       schema.ObjectField{Type: schema.NewNamedType("Int").Encode()},
// 				// "event":               schema.ObjectField{Type: schema.NewNestedObject("event").Encode()},
// 				// "geoip":               schema.ObjectField{Type: schema.NewNestedObject("geoip").Encode()},
// 				"manufacturer": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"order_date": schema.ObjectField{
// 					Type:        schema.NewNamedType("String").Encode(),
// 					Description: utils.ToPtr("handle date object"),
// 				},
// 				// "products":              schema.ObjectField{Type: schema.NewNamedType("products").Encode()},
// 				"sku":                   schema.ObjectField{Type: schema.NewArrayType(schema.NewNamedType("String")).Encode()},
// 				"taxful_total_price":    schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"taxless_total_price":   schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"total_quantity":        schema.ObjectField{Type: schema.NewNamedType("Int").Encode()},
// 				"total_unique_products": schema.ObjectField{Type: schema.NewNamedType("Int").Encode()},
// 				"type":                  schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"user":                  schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 			},
// 		},
// 		"event": schema.ObjectType{
// 			Fields: schema.ObjectTypeFields{
// 				"dataset": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 			},
// 		},
// 		"geoip": schema.ObjectType{
// 			Fields: schema.ObjectTypeFields{
// 				"city_name":        schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"continent_name":   schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"country_iso_code": schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"location":         schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"region_name":      schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 			},
// 		},
// 		"products": schema.ObjectType{
// 			Fields: schema.ObjectTypeFields{
// 				"_id":             schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"base_price":      schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"base_unit_price": schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"category":        schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"created_on": schema.ObjectField{
// 					Description: utils.ToPtr("handle date object"),
// 					Type:        schema.NewNamedType("Float").Encode(),
// 				},
// 				"discount_amount":      schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"discount_percentage":  schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"manufacturer":         schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"min_price":            schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"price":                schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"product_name":         schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"quantity":             schema.ObjectField{Type: schema.NewNamedType("Int").Encode()},
// 				"sku":                  schema.ObjectField{Type: schema.NewNamedType("String").Encode()},
// 				"tax_amount":           schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"taxful_price":         schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"taxless_price":        schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"unit_discount_amount": schema.ObjectField{Type: schema.NewNamedType("Float").Encode()},
// 				"product_id":           schema.ObjectField{Type: schema.NewNamedType("Int").Encode()}},
// 		},
// 	},
// 	Collections: []schema.CollectionInfo{
// 		{
// 			Name:        "kibana_sample_data_ecommerces",
// 			Description: utils.ToPtr("A collection of ecommerce data"),
// 			Arguments:   schema.CollectionInfoArguments{},
// 			Type:        "kibana_sample_data_ecommerce",
// 			UniquenessConstraints: schema.CollectionInfoUniquenessConstraints{
// 				"EcommerceByID": schema.UniquenessConstraint{
// 					UniqueColumns: []string{"_id"},
// 				},
// 			},
// 			ForeignKeys: schema.CollectionInfoForeignKeys{},
// 		},
// 	},
// 	Functions:  []schema.FunctionInfo{},
// 	Procedures: []schema.ProcedureInfo{},
// }

package configuration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/hasura/ndc-sdk-go/schema"
)

var Schema schema.SchemaResponse

func UpdatetConfigurations(ctx context.Context) error {
	err := introspect(ctx, &Schema)
	if err != nil {
		return err
	}

	jsonData, err := json.MarshalIndent(Schema, "", "    ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return err
	}

	if err := ioutil.WriteFile("configuration.json", jsonData, 0644); err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	return nil
}

func introspect(ctx context.Context, Schema *schema.SchemaResponse) error {
	// Fetching all the indices
	indicesData, err := getIndices(ctx)
	if err != nil {
		return err
	}

	schmaResponseObjecttypes := make(schema.SchemaResponseObjectTypes)
	scalar := make(map[string]bool)
	for _, index := range indicesData {
		// Fetching mappings for each user index
		result, err := getMappings(index, ctx)
		if err != nil {
			return err
		}

		properties, ok := result.(map[string]interface{})[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
		if !ok {
			fmt.Println("Invalid JSON structure")
			return nil
		}

		// generate schema.ObjectType object
		var objects schema.ObjectType
		fields := make(schema.ObjectTypeFields)
		getObjectTypes(fields, scalar, properties, "")
		fmt.Println(scalar)
		objects.Description = nil
		objects.Fields = fields
		schmaResponseObjecttypes[index] = objects
	}

	// genereate schema.ScalarTypes object
	ScalarTypes, err := getScalarTypes(scalar, ctx)
	if err != nil {
		return err
	}

	// generate schema.CollectionInfo object
	collectionInfo := getCollectionInfo(indicesData)

	Schema.ScalarTypes = ScalarTypes
	Schema.ObjectTypes = schmaResponseObjecttypes
	Schema.Collections = collectionInfo
	Schema.Functions = []schema.FunctionInfo{}
	Schema.Procedures = []schema.ProcedureInfo{}

	return nil
}

func getIndices(ctx context.Context) ([]string, error) {
	client := ctx.Value("elasticsearch").(*elasticsearch.Client)
	resp, err := client.Cat.Indices()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var indicesData []string

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			indexName := fields[2]
			if !strings.HasPrefix(indexName, ".") {
				indicesData = append(indicesData, indexName)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return indicesData, nil
}

func getMappings(indexName string, ctx context.Context) (interface{}, error) {
	client := ctx.Value("elasticsearch").(*elasticsearch.Client)
	resp, err := client.Indices.GetMapping(client.Indices.GetMapping.WithIndex(indexName))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// maps Elasticsearch field types to Hasura types
func MapElasticsearchTypeToHasura(esType string) string {
	switch esType {
	case "keyword":
		return "String"
	case "boolean":
		return "Boolean"
	case "byte", "short", "integer", "long", "unsigned_long":
		return "Int"
	case "double", "float", "half_float", "scaled_float":
		return "Float"
	default:
		return ""
	}
}

func getObjectTypes(fields schema.ObjectTypeFields, scalar map[string]bool, properties map[string]interface{}, parentField string) {
	for key, value := range properties {
		var field schema.ObjectField
		valueData := value.(map[string]interface{})

		if valueData["properties"] != nil {
			getObjectTypes(fields, scalar, valueData["properties"].(map[string]any), key)
		}
		if valueData["fields"] != nil {
			getObjectTypes(fields, scalar, valueData["fields"].(map[string]any), key)
		}
		if valueData["type"] != nil {
			if !scalar[valueData["type"].(string)] {
				scalar[valueData["type"].(string)] = true
			}
			hasuraType := MapElasticsearchTypeToHasura(valueData["type"].(string))
			if hasuraType == "" {
				hasuraType = valueData["type"].(string)
			}
			field.Type = schema.NewNamedType(hasuraType).Encode()
			if parentField != "" {
				fields[parentField+"."+key] = field
			} else {
				fields[key] = field
			}
		}
	}
}

func getCollectionInfo(indices []string) []schema.CollectionInfo {
	var collections []schema.CollectionInfo
	for _, index := range indices {
		var collection schema.CollectionInfo
		collection.Description = nil
		collection.Arguments = schema.CollectionInfoArguments{}
		collection.ForeignKeys = schema.CollectionInfoForeignKeys{}
		collection.Name = index + "s"
		collection.Type = index
		collection.UniquenessConstraints = schema.CollectionInfoUniquenessConstraints{
			"ID": schema.UniquenessConstraint{
				UniqueColumns: []string{"_id"},
			},
		}

		collections = append(collections, collection)
	}
	return collections
}

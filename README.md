# NDC example with CSV

The connector is ported from [NDC Reference Connector](https://github.com/hasura/ndc-sdk-go/tree/main/example/reference) that read CSV files into memory. It is intended to illustrate the concepts involved, and should be complete, in the sense that every specification feature is covered. It is not optimized and is not intended for production use, but might be useful for testing.

## Getting Started

```bash
go run . serve
```

## Using the reference connector

The reference connector runs on http://localhost:8080:

```sh
curl http://localhost:8080/schema | jq .
```

## Projet breackdown

```

hasura-elasticsearch/
├─ cli/
│  ├─ update_configurations
├─ query/
│  ├─ fields_select
│  ├─ filter/
│  │  ├─ boolean_expression
│  │  ├─ comparison_value
│  │  ├─ text
│  ├─ sorting
│  ├─ pagination
│  ├─ aggregate
├─ connector/
│  ├─ health
│  ├─ capabilities
│  ├─ schema
│  ├─ metrices

```

### Query

The query endpoint accepts a `QueryRequest`, containing expressions to be evaluated in the context the data source, and returns a `QueryResponse` consisting of relevant rows of data.

A request to query endpoint goes through list of stages to translate `QueryRequest` into the DSL query of Elasticsearch. finally, 
the call is made to `performQuery` function to execute query against Elasticsearch and gets back a `QueryResponse` to the caller.

List of stages query goes through and it's usage

#### fields_select
   
   Puts requested fields into the `"_source"` field in DSL query

#### filter
   
   If filter is of type boolean expression than make recursive call with the underlaying filters.
   Handle filter for comparison values such as `_eq`, `_gt`, `_lt`, etc..
   Handle filter for text such as `like`, `regexp`, etc..

#### sorting
   
   Puts requested sorting fields into `"sort"` field in DSL query

#### pagination
   
   Puts limit and offset fields into `size` and `from` fields in DSL query

#### aggregate
   
   Translates aggregate function into DSL query.


## Steps to deploy a connector

1. Clone reference connector from github [NDC Reference Connector](https://github.com/hasura/ndc-sdk-go/tree/main/example/reference)
2. Push connector's code to your own public github repository.
3. Install Hasura CLI
4. Install connector plugin, follow (https://hasura.io/docs/latest/hasura-cli/connector-plugin/)
5. Create connector

```bash
hasura3 connector create <CONNECTOR_NAME> --github-repo-url <CONNECTOR_GITHUB_URL>
```

6. List connector to get the url of deployed connector.

```bash
hasura3 connector list
```

step: Create Hasura project

7. Log into Hasura
  
```bash
hasura3 login
```

8. Create a new project

```bash
hasura3 init --dir <PROJECT_DIRECTORY>
cd <PROJECT_DIRECTORY>
```

#### Use connector in hasura project:
9. create folder <FOLDER_NAME> in subgraphs/default/dataconnectors 
10. Create file in that folder with <CONNECTOR>.hml format and add following configuration in it.

```bash
kind: DataConnector
version: v2
definition:
  name: elastic
  url:
    singleUrl:
      value: <CONNECTOR_URL>
```

11. Specify connector in build-profile.yaml

```bash
version: 2
spec:
  environment: default
  mode: replace
  supergraph:
    resources:
    - supergraph/*
  subgraphs:
  - name: default
    resources:
    - subgraphs/default/**/*.hml
    connectors:
      <CUSTOM_CONNECTOR_NAME>:
        path: subgraphs/default/dataconnectors/<FOLDER_NAME>
        connectorConfigFile: <CONNECTOR>.hml
```

12. Build hasura project

```bash
hasura3 watch --dir .
```

13. Bring up the command palette, type ``` hasura track all ```, and choose the option from the dropdown. Then, select your data source's name (e.g., pg_db) from the dropdown.

14. If getting err that versions not found than rename version to versions in capability response in <CONNECTOR>.hml file and than run fillowing command:

```bash
hasura3 build create -d "<BUILD_NAME>"
```
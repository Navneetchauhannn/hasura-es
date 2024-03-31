package configuration

import (
	"context"
	"fmt"
	"os"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/hasura/ndc-sdk-go/connector"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Elasticsearch struct {
		Node     string `yaml:"node"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Cert     string `yaml:"cert"`
	} `yaml:"elasticsearch"`
}

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

type State struct {
	Authors             []Author
	Articles            []Article
	Institutions        []Institution
	ElasticsearchClient *elasticsearch.Client
	Telemetry           *connector.TelemetryState
}

func (s *State) GetLatestArticle() []int {
	return nil
}

func ConnectWithElasticsearch(ctx context.Context) context.Context {
	var Config Config

	ymlFile, err := os.ReadFile("C:/Users/navnit.chauhan/Projects/Hasura-Elasticsearch_Connector/elasticsearch-queries/elastic/config.yml")
	if err != nil {
		fmt.Println(err)
	}

	err = yaml.Unmarshal(ymlFile, &Config)
	if err != nil {
		fmt.Println("Error unmarshaling the data")
	}
	// fmt.Println(Config.Elasticsearch.Username)
	// cert, _ := ioutil.ReadFile(Config.Elasticsearch.Cert)
	newClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{Config.Elasticsearch.Node},
		Username:  Config.Elasticsearch.Username,
		Password:  Config.Elasticsearch.Password,
		// CACert:    cert,
	})
	if err != nil {
		panic(err)
	}
	return context.WithValue(ctx, "elasticsearch", newClient)
}

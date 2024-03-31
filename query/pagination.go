package query

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	pitID       string
	pitExpiry   map[string]time.Time
	pitExpiryMu sync.Mutex
)

const (
	pitKeepAlive = 2 * time.Minute // PIT keep-alive duration
	cleanupFreq  = 5 * time.Minute // Cleanup frequency
)

func createPointInTime(client *elasticsearch.Client) (string, error) {
	res, err := client.OpenPointInTime(
		[]string{"kibana_sample_data_ecommerce"}, // Replace with your index name
		"2m",
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	var body struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return "", err
	}

	// Update PIT expiry
	pitExpiryMu.Lock()
	pitExpiry = make(map[string]time.Time)
	pitExpiry[body.ID] = time.Now().Add(pitKeepAlive) // Set expiry time to 5 minutes
	pitExpiryMu.Unlock()

	fmt.Printf("Set expiry time to %v\n", pitExpiry[body.ID])

	pitID = body.ID
	return body.ID, nil
}

func isPitExpired() bool {
	pitExpiryMu.Lock()
	defer pitExpiryMu.Unlock()

	expiryTime, exists := pitExpiry[pitID]
	if !exists {
		fmt.Printf("Not found Expiry Time %v\n", pitExpiry)
		return true
	}

	return time.Now().After(expiryTime)
}

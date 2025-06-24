package sniff

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/http"
)

const (
	TokenSnifferApiKeyLocation = "%s-market-map-updater-token-sniffer-apiKey"
	TokenSnifferApiUrl         = "https://tokensniffer.com/api/v2/tokens/%s/%s?apikey=%s&include_metrics=true"
)

var _ SniffClient = &sniffClient{}

// SniffClient is an interface for checking if tokens are scams using the TokenSniffer API.
//
//go:generate mockery --name SniffClient --filename mock_sniff_client.go
type SniffClient interface {
	IsTokenAScam(chain string, contractAddress string) (bool, error)
}

type sniffClient struct {
	ctx    context.Context
	apiKey string
	client *http.Client
}

// Chains supported by TokenSniffer
// https://tokensniffer.readme.io/reference/supported-networks
var ChainToIDMap = map[string]string{
	"Ethereum":                "1",
	"Solana":                  "101",
	"Base":                    "8453",
	"BNB Smart Chain (BEP20)": "56",
	"Polygon":                 "137",
	"Arbitrum":                "42161",
	"Optimism":                "10",
	"Avalanche C-Chain":       "43114",
	"Fantom":                  "250",
	"Gnosis Chain":            "100",
	"Harmony":                 "1666600000",
	"KCC":                     "321",
	"Cronos":                  "25",
	"Oasis Network":           "42262",
}

func NewSniffClient(ctx context.Context) SniffClient {
	env := os.Getenv("ENVIRONMENT")
	apiKey, err := aws.GetSecret(ctx, fmt.Sprintf(TokenSnifferApiKeyLocation, env))
	if err != nil {
		panic(err)
	}

	return &sniffClient{
		apiKey: apiKey,
		client: http.NewClient(),
		ctx:    ctx,
	}
}

func (c *sniffClient) IsTokenAScam(chain string, contractAddress string) (bool, error) {
	chainID := ChainToIDMap[chain]
	if chainID == "" {
		return false, fmt.Errorf("chain not supported")
	}

	url := fmt.Sprintf(TokenSnifferApiUrl, chainID, contractAddress, c.apiKey)
	resp, err := c.client.GetWithContextRetryOnce(c.ctx, url)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		return false, fmt.Errorf("failed to check if token is a scam: %s", resp.Status)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return false, err
	}

	is_flagged, ok := data["is_flagged"].(bool)
	if !ok {
		return false, fmt.Errorf("missing or invalid is_flagged field in response")
	}

	is_suspect, ok := data["is_suspect"].(bool)
	if !ok {
		return false, fmt.Errorf("missing or invalid is_suspect field in response")
	}

	score, ok := data["score"].(float64)
	if !ok {
		return false, fmt.Errorf("missing or invalid score field in response")
	}

	swap_simulation, ok := data["swap_simulation"].(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("missing or invalid swap_simulation field in response")
	}

	is_sellable_raw, exists := swap_simulation["is_sellable"]
	is_sellable := true // default to true (not a scam) if field is missing/nil
	if exists && is_sellable_raw != nil {
		if sellable, ok := is_sellable_raw.(bool); ok {
			is_sellable = sellable
		}
	}

	if is_flagged || is_suspect || score < 30 || !is_sellable {
		return true, nil
	}

	return false, nil
}

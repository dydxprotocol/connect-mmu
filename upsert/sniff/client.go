package sniff

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/http"
)

const (
	TokenSnifferKeyLocation = "%s-market-map-updater-token-sniffer-apiKey"                                  // #nosec G101
	TokenSnifferURL         = "https://tokensniffer.com/api/v2/tokens/%s/%s?apikey=%s&include_metrics=true" // #nosec G101
)

var _ Client = &client{}

// Client is an interface for checking if tokens are scams using the TokenSniffer API.
type Client interface {
	IsTokenAScam(chain string, contractAddress string) (bool, error)
	IsTokenInWhitelisted(contractAddress string) bool
}

type client struct {
	ctx                  context.Context
	apiKey               string
	client               *http.Client
	tokenSnifferWhitelist []string
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

func NewClient(ctx context.Context, tokenSnifferWhitelist []string) Client {
	env := os.Getenv("ENVIRONMENT")
	apiKey, err := aws.GetSecret(ctx, fmt.Sprintf(TokenSnifferKeyLocation, env))
	if err != nil {
		panic(err)
	}

	return &client{
		apiKey: apiKey,
		client: http.NewClient(),
		ctx:    ctx,
		tokenSnifferWhitelist: tokenSnifferWhitelist,
	}
}

func (c *client) IsTokenInWhitelisted(cmcID string) bool {
	return slices.Contains(c.tokenSnifferWhitelist, cmcID)
}

func (c *client) IsTokenAScam(chain string, contractAddress string) (bool, error) {
	chainID := ChainToIDMap[chain]
	if chainID == "" {
		return false, fmt.Errorf("chain not supported")
	}

	url := fmt.Sprintf(TokenSnifferURL, chainID, contractAddress, c.apiKey)
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

	isFlagged, ok := data["is_flagged"].(bool)
	if !ok {
		return false, fmt.Errorf("missing or invalid is_flagged field in response")
	}

	isSuspect, ok := data["is_suspect"].(bool)
	if !ok {
		return false, fmt.Errorf("missing or invalid is_suspect field in response")
	}

	score, ok := data["score"].(float64)
	if !ok {
		return false, fmt.Errorf("missing or invalid score field in response")
	}

	swapSimulation, ok := data["swap_simulation"].(map[string]interface{})
	if !ok {
		return false, fmt.Errorf("missing or invalid swap_simulation field in response")
	}

	isSellableRaw, exists := swapSimulation["is_sellable"]
	isSellable := true // default to true (not a scam) if field is missing/nil
	if exists && isSellableRaw != nil {
		if sellable, ok := isSellableRaw.(bool); ok {
			isSellable = sellable
		}
	}

	if isFlagged || isSuspect || score < 30 || !isSellable {
		return true, nil
	}

	return false, nil
}

package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/http"
)

const (
	TokenSnifferApiKeyLocation = "%s-market-map-updater-token-sniffer-api-key"
	TokenSnifferApiUrl         = "https://tokensniffer.com/api/v2/tokens/%s/%s?api_key=%s&include_metrics=true"
)

var _ SniffClient = &sniffClient{}

// SniffClient is an interface for checking if tokens are scams using the TokenSniffer API.
//
//go:generate mockery --name SniffClient --filename mock_sniff_client.go
type SniffClient interface {
	IsTokenAScam(ctx context.Context, chain string, contractAddress string) (bool, error)
}

type sniffClient struct {
	apiKey string
	client *http.Client
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
	}
}

func (c *sniffClient) IsTokenAScam(ctx context.Context, chain string, contractAddress string) (bool, error) {
	url := fmt.Sprintf(TokenSnifferApiUrl, chain, contractAddress, c.apiKey)
	resp, err := c.client.GetWithContext(ctx, url)
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

	is_sellable, ok := swap_simulation["is_sellable"].(bool)
	if !ok {
		return false, fmt.Errorf("missing or invalid is_sellable field in response")
	}

	if is_flagged || is_suspect || score < 30 || !is_sellable {
		return true, nil
	}

	return false, nil
}

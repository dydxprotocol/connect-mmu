package dydx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cosmos/cosmos-sdk/types/query"
)

const (
	// STATUS_ACTIVE represents an active clob pair.
	CLOB_PAIR_STATUS_ACTIVE = "STATUS_ACTIVE"
	// STATUS_INITIALIZING represents a newly-added clob pair.
	// Clob pairs in this state only accept orders which are
	// both short-term and post-only.
	CLOB_PAIR_STATUS_INITIALIZING = "STATUS_INITIALIZING"
	// STATUS_FINAL_SETTLEMENT represents a clob pair which is deactivated
	// and trading has ceased. All open positions will be closed by the
	// protocol. Open stateful orders will be cancelled. Open short-term
	// orders will be left to expire.
	CLOB_PAIR_STATUS_FINAL_SETTLEMENT = "STATUS_FINAL_SETTLEMENT"
)

// ClobPair represents a clob pair on the dYdX exchange.
type ClobPair struct {
	ID                        uint64                `json:"id"`
	PerpetualClobMetadata     PerpetualClobMetadata `json:"perpetual_clob_metadata"`
	StepBaseQuantums          string                `json:"step_base_quantums"`
	SubticksPerTick           uint64                `json:"subticks_per_tick"`
	QuantumConversionExponent int64                 `json:"quantum_conversion_exponent"`
	Status                    string                `json:"status"`
}

// PerpetualClobMetadata represents the parameters of an associated perpetual for a clob pair on the dYdX exchange.
type PerpetualClobMetadata struct {
	PerpetualID uint64 `json:"perpetual_id"`
}

// AllClobPairsResponse is the response type for the AllClobPairs RPC method.
type AllClobPairsResponse struct {
	ClobPairs  []ClobPair `json:"clob_pair"`
	Pagination struct {
		NextKey string `json:"next_key"`
		Total   string `json:"total"`
	} `json:"pagination"`
}

// GetPagination implements the saurongrpc ResponseWithPagination interface.
func (r *AllClobPairsResponse) GetPagination() *query.PageResponse {
	// unmarshal base64 next key
	nextKey, err := base64.StdEncoding.DecodeString(r.Pagination.NextKey)
	if err != nil {
		return nil
	}

	total, err := strconv.ParseUint(r.Pagination.Total, 10, 64)
	if err != nil {
		return nil
	}

	return &query.PageResponse{
		NextKey: nextKey,
		Total:   total,
	}
}

// AllClobPairs retrieves all clob pairs from the dYdX API.
func (c *HTTPClient) AllClobPairs(ctx context.Context) (*AllClobPairsResponse, error) {
	baseURL := fmt.Sprintf("%s/dydxprotocol/clob/clob_pair", c.BaseURL)
	resp, err := c.client.GetWithContext(ctx, baseURL)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var actualResult AllClobPairsResponse
	if err := json.NewDecoder(resp.Body).Decode(&actualResult); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	lastKey := ""
	nextKey := actualResult.Pagination.NextKey
	for nextKey != "" {
		paginatedResponse, err := c.client.GetWithContext(ctx, baseURL+"?pagination.key="+nextKey)
		if err != nil {
			return nil, err
		}

		if paginatedResponse.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code: %d", paginatedResponse.StatusCode)
		}

		var result AllClobPairsResponse
		if err := json.NewDecoder(paginatedResponse.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("error decoding response: %w", err)
		}

		actualResult.ClobPairs = append(actualResult.ClobPairs, result.ClobPairs...)

		lastKey = nextKey
		nextKey = result.Pagination.NextKey

		if lastKey == nextKey {
			return nil, fmt.Errorf("error saw repeat pagination key: %s", nextKey)
		}
	}

	return &actualResult, nil
}

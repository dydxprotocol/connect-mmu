package provider

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryStoreDepthPreservation(t *testing.T) {
	// Create test data with different positive and negative depths
	testDoc := Document{
		AssetInfos: []AssetInfo{
			{
				ID:     1,
				Symbol: "BTC",
				CMCID:  1,
			},
			{
				ID:     2,
				Symbol: "USD",
				CMCID:  2,
			},
		},
		ProviderMarkets: []ProviderMarket{
			{
				ID:               1,
				TargetBase:       "BTC",
				TargetQuote:      "USD",
				OffChainTicker:   "BTC-USD",
				ProviderName:     "test_provider",
				BaseAssetInfoID:  1,
				QuoteAssetInfoID: 2,
				MetadataJSON:     "test",
				ReferencePrice:   100000.0,
				NegativeDepthTwo: 100.0,
				PositiveDepthTwo: 200.0,
			},
		},
	}

	// Serialize to JSON
	jsonBz, err := json.Marshal(testDoc)
	require.NoError(t, err)

	// Create a temporary file with the test data
	tmpfile, err := os.CreateTemp("", "test-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write(jsonBz)
	require.NoError(t, err)
	err = tmpfile.Close()
	require.NoError(t, err)

	// Load the data into a new memory store
	store, err := NewMemoryStoreFromFile(tmpfile.Name())
	require.NoError(t, err)

	// Call GetProviderMarkets to get the filtered rows
	rows, err := store.GetProviderMarkets(context.Background(), GetFilteredProviderMarketsParams{
		ProviderNames: []string{"test_provider"},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)

	// Verify all fields are preserved correctly in the filtered rows
	row := rows[0]
	require.Equal(t, "BTC", row.TargetBase, "TargetBase should be preserved")
	require.Equal(t, "USD", row.TargetQuote, "TargetQuote should be preserved")
	require.Equal(t, "BTC-USD", row.OffChainTicker, "OffChainTicker should be preserved")
	require.Equal(t, "test_provider", row.ProviderName, "ProviderName should be preserved")
	require.Equal(t, int64(1), row.BaseCmcID, "BaseCmcID should be preserved")
	require.Equal(t, int64(2), row.QuoteCmcID, "QuoteCmcID should be preserved")
	require.Equal(t, "test", string(row.MetadataJSON), "MetadataJSON should be preserved")
	require.Equal(t, 100000.0, row.ReferencePrice, "ReferencePrice should be preserved")
	require.Equal(t, 100.0, row.NegativeDepthTwo, "NegativeDepthTwo should be preserved")
	require.Equal(t, 200.0, row.PositiveDepthTwo, "PositiveDepthTwo should be preserved")
}

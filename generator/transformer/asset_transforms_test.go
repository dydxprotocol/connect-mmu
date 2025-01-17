package transformer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
	mmutypes "github.com/skip-mev/connect-mmu/types"
)

func TestHasCMCTag(t *testing.T) {
	tests := []struct {
		name          string
		assetInfo     provider.AssetInfo
		tagsToFilter  []string
		expectedMatch bool
	}{
		{
			name: "match single tag",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{"wrapped-tokens"},
			},
			tagsToFilter:  []string{"wrapped-tokens"},
			expectedMatch: true,
		},
		{
			name: "match one of multiple tags",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{"wrapped-tokens", "defi"},
			},
			tagsToFilter:  []string{"algorithmic-stablecoin", "wrapped-tokens"},
			expectedMatch: true,
		},
		{
			name: "no match",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{"defi", "gaming"},
			},
			tagsToFilter:  []string{"wrapped-tokens", "algorithmic-stablecoin"},
			expectedMatch: false,
		},
		{
			name: "empty asset tags",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{},
			},
			tagsToFilter:  []string{"wrapped-tokens"},
			expectedMatch: false,
		},
		{
			name: "empty filter tags",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{"wrapped-tokens"},
			},
			tagsToFilter:  []string{},
			expectedMatch: false,
		},
		{
			name: "case sensitive match",
			assetInfo: provider.AssetInfo{
				CMCTags: []string{"Wrapped-Tokens"},
			},
			tagsToFilter:  []string{"wrapped-tokens"},
			expectedMatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := HasCMCTag(tc.assetInfo, tc.tagsToFilter)
			require.Equal(t, tc.expectedMatch, result)
		})
	}
}

func TestFilterOutCMCTags(t *testing.T) {
	logger := zap.NewNop()
	ctx := context.Background()

	tests := []struct {
		name              string
		feeds             types.Feeds
		cmcIDToAssetInfo  map[int64]provider.AssetInfo
		config            config.GenerateConfig
		expectedFeedCount int
	}{
		{
			name: "filter out wrapped tokens",
			feeds: types.Feeds{
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 1},
				},
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 2},
				},
			},
			cmcIDToAssetInfo: map[int64]provider.AssetInfo{
				1: {CMCTags: []string{"wrapped-tokens"}},
				2: {CMCTags: []string{"defi"}},
			},
			config: config.GenerateConfig{
				ExcludeCMCTags: []string{"wrapped-tokens"},
			},
			expectedFeedCount: 1,
		},
		{
			name: "filter out multiple tags",
			feeds: types.Feeds{
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 1},
				},
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 2},
				},
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 3},
				},
			},
			cmcIDToAssetInfo: map[int64]provider.AssetInfo{
				1: {CMCTags: []string{"wrapped-tokens"}},
				2: {CMCTags: []string{"algorithmic-stablecoin"}},
				3: {CMCTags: []string{"defi"}},
			},
			config: config.GenerateConfig{
				ExcludeCMCTags: []string{"wrapped-tokens", "algorithmic-stablecoin"},
			},
			expectedFeedCount: 1,
		},
		{
			name: "no tags to filter",
			feeds: types.Feeds{
				{
					CMCInfo: mmutypes.CoinMarketCapInfo{BaseID: 1},
				},
			},
			cmcIDToAssetInfo: map[int64]provider.AssetInfo{
				1: {CMCTags: []string{"defi"}},
			},
			config: config.GenerateConfig{
				ExcludeCMCTags: []string{},
			},
			expectedFeedCount: 1,
		},
		{
			name:  "no feeds",
			feeds: types.Feeds{},
			cmcIDToAssetInfo: map[int64]provider.AssetInfo{
				1: {CMCTags: []string{"wrapped-tokens"}},
			},
			config: config.GenerateConfig{
				ExcludeCMCTags: []string{"wrapped-tokens"},
			},
			expectedFeedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transformer := FilterOutCMCTags()
			result, exclusions, err := transformer(ctx, logger, tc.config, tc.feeds, tc.cmcIDToAssetInfo)

			require.NoError(t, err)
			require.NotNil(t, exclusions)
			require.Len(t, result, tc.expectedFeedCount)

			// Verify that none of the remaining feeds have excluded tags
			for _, feed := range result {
				assetInfo := tc.cmcIDToAssetInfo[feed.CMCInfo.BaseID]
				require.False(t, HasCMCTag(assetInfo, tc.config.ExcludeCMCTags))
			}
		})
	}
}

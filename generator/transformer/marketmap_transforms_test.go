package transformer_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dydxprotocol/slinky/pkg/types"
	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/transformer"
)

func TestOverrideMarkets(t *testing.T) {
	tests := []struct {
		name           string
		inputMarketMap mmtypes.MarketMap
		overrideConfig config.GenerateConfig
		expected       mmtypes.MarketMap
	}{
		{
			name: "Override existing market",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          true,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
						},
					},
				},
			},
			overrideConfig: config.GenerateConfig{
				MarketMapOverride: mmtypes.MarketMap{
					Markets: map[string]mmtypes.Market{
						"BTC/USD": {
							Ticker: mmtypes.Ticker{
								CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
								Decimals:         9,
								MinProviderCount: 3,
								Enabled:          true,
							},
							ProviderConfigs: []mmtypes.ProviderConfig{
								{Name: "provider1", OffChainTicker: "BTCUSD"},
								{Name: "provider2", OffChainTicker: "BTC-USD"},
							},
						},
					},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         9,
							MinProviderCount: 3,
							Enabled:          true,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTC-USD"},
						},
					},
				},
			},
		},
		{
			name: "Add new market",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          true,
						},
					},
				},
			},
			overrideConfig: config.GenerateConfig{
				MarketMapOverride: mmtypes.MarketMap{
					Markets: map[string]mmtypes.Market{
						"ETH/USD": {
							Ticker: mmtypes.Ticker{
								CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
								Decimals:         18,
								MinProviderCount: 2,
								Enabled:          true,
							},
							ProviderConfigs: []mmtypes.ProviderConfig{
								{Name: "provider1", OffChainTicker: "ETHUSD"},
							},
						},
					},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          true,
						},
					},
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							Decimals:         18,
							MinProviderCount: 2,
							Enabled:          true,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
		},
		{
			name: "No overrides",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          true,
						},
					},
				},
			},
			overrideConfig: config.GenerateConfig{
				MarketMapOverride: mmtypes.MarketMap{
					Markets: map[string]mmtypes.Market{},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          true,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			ctx := context.Background()

			transform := transformer.OverrideMarkets()
			result, _, err := transform(ctx, logger, tt.overrideConfig, tt.inputMarketMap)

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPruneInsufficientlyProvidedMarkets(t *testing.T) {
	tests := []struct {
		name           string
		inputMarketMap mmtypes.MarketMap
		config         config.GenerateConfig
		expected       mmtypes.MarketMap
		dropped        []string
	}{
		{
			name: "Prune markets with insufficient providers",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
						},
					},
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 1,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			config: config.GenerateConfig{
				Providers: map[string]config.ProviderConfig{
					"provider1": {},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 1,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			dropped: []string{"BTC/USD"},
		},
		{
			name: "Prune market with insufficient providers-min_provider_override",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
						},
					},
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
							{Name: "provider2", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			config: config.GenerateConfig{
				Providers: map[string]config.ProviderConfig{
					"provider1": {},
					"provider2": {},
				},
				MinCexProviderCount: 2,
				MinDexProviderCount: 1,
				Quotes: map[string]config.QuoteConfig{
					"USD": {},
				},
				MinProviderCountOverride: 1,
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 1,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
							{Name: "provider2", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			dropped: []string{"BTC/USD"},
		},
		{
			name: "Override min providers on multiple markets--prune 0 markets",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTCUSD"},
						},
					},
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
							{Name: "provider2", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			config: config.GenerateConfig{
				Providers: map[string]config.ProviderConfig{
					"provider1": {},
					"provider2": {},
				},
				Quotes: map[string]config.QuoteConfig{
					"USD": {},
				},
				MinCexProviderCount:      2,
				MinDexProviderCount:      1,
				MinProviderCountOverride: 1,
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 1,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTCUSD"},
						},
					},
					"ETH/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "ETH", Quote: "USD"},
							MinProviderCount: 1,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "ETHUSD"},
							{Name: "provider2", OffChainTicker: "ETHUSD"},
						},
					},
				},
			},
			dropped: []string{},
		},
		{
			name: "Keep market with sufficient providers",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTCUSD"},
						},
					},
				},
			},
			config: config.GenerateConfig{
				Providers: map[string]config.ProviderConfig{
					"provider1": {},
					"provider2": {},
				},
				MinCexProviderCount: 2,
				MinDexProviderCount: 1,
				Quotes: map[string]config.QuoteConfig{
					"USD": {
						NormalizeByPair: "",
					},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTCUSD"},
						},
					},
				},
			},
		},
		{
			name: "Ignore supplemental providers",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair:     types.CurrencyPair{Base: "BTC", Quote: "USD"},
							MinProviderCount: 2,
							Decimals:         8,
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{Name: "provider1", OffChainTicker: "BTCUSD"},
							{Name: "provider2", OffChainTicker: "BTCUSD"},
							{Name: "provider3", OffChainTicker: "BTCUSD"},
						},
					},
				},
			},
			config: config.GenerateConfig{
				Providers: map[string]config.ProviderConfig{
					"provider1": {},
					"provider2": {
						IsSupplemental: true,
					},
					"provider3": {},
				},
				MinCexProviderCount: 2,
				MinDexProviderCount: 1,
				Quotes: map[string]config.QuoteConfig{
					"USD": {
						NormalizeByPair: "",
					},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair: types.CurrencyPair{
								Base:  "BTC",
								Quote: "USD",
							},
							Decimals:         8,
							MinProviderCount: 2,
							Enabled:          false,
							Metadata_JSON:    "",
						},
						ProviderConfigs: []mmtypes.ProviderConfig{
							{
								Name:            "provider1",
								OffChainTicker:  "BTCUSD",
								NormalizeByPair: nil,
								Invert:          false,
								Metadata_JSON:   "",
							},
							{
								Name:            "provider2",
								OffChainTicker:  "BTCUSD",
								NormalizeByPair: nil,
								Invert:          false,
								Metadata_JSON:   "",
							},
							{
								Name:            "provider3",
								OffChainTicker:  "BTCUSD",
								NormalizeByPair: nil,
								Invert:          false,
								Metadata_JSON:   "",
							},
						},
					},
				},
			},
			dropped: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			ctx := context.Background()

			transform1 := transformer.PruneInsufficientlyProvidedMarkets()
			result, dropped, err1 := transform1(ctx, logger, tt.config, tt.inputMarketMap)
			transform2 := transformer.OverrideMinProviderCount()
			result, _, err2 := transform2(ctx, logger, tt.config, result)

			require.NoError(t, err1)
			require.NoError(t, err2)
			require.Equal(t, tt.expected, result)
			for _, d := range tt.dropped {
				_, ok := dropped[d]
				require.True(t, ok)
			}
		})
	}
}

func TestExcludeDisabledProviders(t *testing.T) {
	tests := []struct {
		name     string
		markets  map[string][]string
		disable  map[string][]string
		expected map[string][]string
	}{
		{
			name: "exclude disabled providers",
			markets: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
			},
			disable: map[string][]string{
				"ETH/USD": {"foo"},
			},
			expected: map[string][]string{
				"ETH/USD": {"bar", "baz"},
			},
		},
		{
			name: "no disabled providers",
			markets: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
			},
			disable: nil,
			expected: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
			},
		},
		{
			name: "completely disabled providers",
			markets: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
			},
			disable: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
			},
			expected: map[string][]string{
				"ETH/USD": {},
			},
		},
		{
			name: "multiple markets with disabled providers",
			markets: map[string][]string{
				"ETH/USD": {"foo", "bar", "baz"},
				"BTC/MOG": {"foo", "bar", "baz"},
			},
			disable: map[string][]string{
				"ETH/USD": {"foo", "baz"},
				"BTC/MOG": {"foo", "bar"},
			},
			expected: map[string][]string{
				"ETH/USD": {"bar"},
				"BTC/MOG": {"baz"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := zap.NewNop()
			ctx := context.Background()
			cfg := config.GenerateConfig{
				DisableProviders: tc.disable,
			}

			// setup the marketmap
			mm := mmtypes.MarketMap{Markets: make(map[string]mmtypes.Market)}
			for ticker, providers := range tc.markets {
				splitTicker := strings.Split(ticker, "/")
				market := mmtypes.Market{Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: splitTicker[0], Quote: splitTicker[1]}}}
				providerCfgs := make([]mmtypes.ProviderConfig, 0, len(providers))
				for _, provider := range providers {
					providerCfgs = append(providerCfgs, mmtypes.ProviderConfig{Name: provider})
				}
				market.ProviderConfigs = providerCfgs
				mm.Markets[ticker] = market
			}

			transform := transformer.ExcludeDisabledProviders()
			result, _, err := transform(ctx, logger, cfg, mm)
			require.NoError(t, err)

			for expectedMarket, expectedProviders := range tc.expected {
				resultMarket, ok := result.Markets[expectedMarket]
				require.True(t, ok, "expected market %q did not exist in result", expectedMarket)

				for _, expectedProvider := range expectedProviders {
					exists := slices.ContainsFunc(resultMarket.ProviderConfigs, func(providerConfig mmtypes.ProviderConfig) bool {
						return providerConfig.Name == expectedProvider
					})
					require.True(t, exists, "expected provider %q to exist in result", expectedProvider)
				}
			}
		})
	}
}

func TestSetEnabled(t *testing.T) {
	tests := []struct {
		name           string
		inputMarketMap mmtypes.MarketMap
		config         config.GenerateConfig
		expected       mmtypes.MarketMap
	}{
		{
			name: "No-op if nothing is set",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			config: config.GenerateConfig{
				EnableAll: false,
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
		},
		{
			name: "enable all single",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Enabled:      false,
						},
					},
				},
			},
			config: config.GenerateConfig{
				EnableAll: true,
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {
						Ticker: mmtypes.Ticker{
							CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"},
							Enabled:      true,
						},
					},
				},
			},
		},
		{
			name: "enable all multi",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}, Enabled: false}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}, Enabled: false}},
					"XRP/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "XRP", Quote: "USD"}, Enabled: false}},
				},
			},
			config: config.GenerateConfig{
				EnableAll: true,
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}, Enabled: true}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}, Enabled: true}},
					"XRP/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "XRP", Quote: "USD"}, Enabled: true}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			ctx := context.Background()

			transform := transformer.EnableMarkets()
			result, _, err := transform(ctx, logger, tt.config, tt.inputMarketMap)

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestPruneMarkets(t *testing.T) {
	tests := []struct {
		name           string
		inputMarketMap mmtypes.MarketMap
		config         config.GenerateConfig
		expected       mmtypes.MarketMap
		dropped        []string
	}{
		{
			name: "Prune excluded currency pairs",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			config: config.GenerateConfig{
				ExcludeCurrencyPairs: map[string]struct{}{
					"BTC/USD": {},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			dropped: []string{"BTC/USD"},
		},
		{
			name: "Keep all when no exclusions or allowlist",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			config: config.GenerateConfig{},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
		},
		{
			name: "Keep only allowed currency pairs",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
					"XRP/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "XRP", Quote: "USD"}}},
				},
			},
			config: config.GenerateConfig{
				AllowedCurrencyPairs: map[string]struct{}{
					"BTC/USD": {},
					"ETH/USD": {},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			dropped: []string{"XRP/USD"},
		},
		{
			name: "Exclusions take precedence over allowlist",
			inputMarketMap: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"BTC/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "BTC", Quote: "USD"}}},
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
					"XRP/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "XRP", Quote: "USD"}}},
				},
			},
			config: config.GenerateConfig{
				ExcludeCurrencyPairs: map[string]struct{}{
					"BTC/USD": {},
				},
				AllowedCurrencyPairs: map[string]struct{}{
					"BTC/USD": {},
					"ETH/USD": {},
				},
			},
			expected: mmtypes.MarketMap{
				Markets: map[string]mmtypes.Market{
					"ETH/USD": {Ticker: mmtypes.Ticker{CurrencyPair: types.CurrencyPair{Base: "ETH", Quote: "USD"}}},
				},
			},
			dropped: []string{"BTC/USD", "XRP/USD"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			ctx := context.Background()

			transform := transformer.PruneMarkets()
			result, dropped, err := transform(ctx, logger, tt.config, tt.inputMarketMap)

			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
			var droppedKeys []string
			for k := range dropped {
				droppedKeys = append(droppedKeys, k)
			}
			slices.Sort(droppedKeys)
			slices.Sort(tt.dropped)
			require.Equal(t, tt.dropped, droppedKeys)
		})
	}
}

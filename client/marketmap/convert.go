package marketmap

import (
	connecttypes "github.com/dydxprotocol/slinky/pkg/types"
	slinkytypes "github.com/dydxprotocol/slinky/pkg/types"
	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
)

func SlinkyToConnectMarket(market mmtypes.Market) mmtypes.Market {
	convertedProviderConfigs := make([]mmtypes.ProviderConfig, 0)

	for _, providerConfig := range market.ProviderConfigs {
		convertedProviderConfig := mmtypes.ProviderConfig{
			Name:           providerConfig.Name,
			OffChainTicker: providerConfig.OffChainTicker,
			Invert:         providerConfig.Invert,
			Metadata_JSON:  providerConfig.Metadata_JSON,
		}

		if providerConfig.NormalizeByPair != nil {
			convertedProviderConfig.NormalizeByPair = &connecttypes.CurrencyPair{
				Base:  providerConfig.NormalizeByPair.Base,
				Quote: providerConfig.NormalizeByPair.Quote,
			}
		}

		convertedProviderConfigs = append(convertedProviderConfigs, convertedProviderConfig)
	}

	newMarket := mmtypes.Market{
		Ticker: mmtypes.Ticker{
			CurrencyPair: connecttypes.CurrencyPair{
				Base:  market.Ticker.CurrencyPair.Base,
				Quote: market.Ticker.CurrencyPair.Quote,
			},
			Decimals:         market.Ticker.Decimals,
			MinProviderCount: market.Ticker.MinProviderCount,
			Enabled:          market.Ticker.Enabled,
			Metadata_JSON:    market.Ticker.Metadata_JSON,
		},
		ProviderConfigs: convertedProviderConfigs,
	}

	return newMarket
}

func SlinkyToConnectMarkets(markets []mmtypes.Market) []mmtypes.Market {
	convertedMarkets := make([]mmtypes.Market, len(markets))
	for i, market := range markets {
		convertedMarkets[i] = SlinkyToConnectMarket(market)
	}

	return convertedMarkets
}

func SlinkyToConnectMarketMap(marketMap mmtypes.MarketMap) mmtypes.MarketMap {
	mm := mmtypes.MarketMap{
		Markets: make(map[string]mmtypes.Market),
	}

	for _, market := range marketMap.Markets {
		newMarket := SlinkyToConnectMarket(market)
		mm.Markets[newMarket.Ticker.String()] = newMarket
	}

	return mm
}

func ConnectToSlinkyMarket(market mmtypes.Market) mmtypes.Market {
	convertedProviderConfigs := make([]mmtypes.ProviderConfig, 0)

	for _, providerConfig := range market.ProviderConfigs {
		convertedProviderConfig := mmtypes.ProviderConfig{
			Name:           providerConfig.Name,
			OffChainTicker: providerConfig.OffChainTicker,
			Invert:         providerConfig.Invert,
			Metadata_JSON:  providerConfig.Metadata_JSON,
		}

		if providerConfig.NormalizeByPair != nil {
			convertedProviderConfig.NormalizeByPair = &slinkytypes.CurrencyPair{
				Base:  providerConfig.NormalizeByPair.Base,
				Quote: providerConfig.NormalizeByPair.Quote,
			}
		}

		convertedProviderConfigs = append(convertedProviderConfigs, convertedProviderConfig)
	}

	newMarket := mmtypes.Market{
		Ticker: mmtypes.Ticker{
			CurrencyPair: slinkytypes.CurrencyPair{
				Base:  market.Ticker.CurrencyPair.Base,
				Quote: market.Ticker.CurrencyPair.Quote,
			},
			Decimals:         market.Ticker.Decimals,
			MinProviderCount: market.Ticker.MinProviderCount,
			Enabled:          market.Ticker.Enabled,
			Metadata_JSON:    market.Ticker.Metadata_JSON,
		},
		ProviderConfigs: convertedProviderConfigs,
	}

	return newMarket
}

func ConnectToSlinkyMarkets(markets []mmtypes.Market) []mmtypes.Market {
	convertedMarkets := make([]mmtypes.Market, len(markets))
	for i, market := range markets {
		convertedMarkets[i] = ConnectToSlinkyMarket(market)
	}

	return convertedMarkets
}

func ConnectToSlinkyMarketMap(marketMap mmtypes.MarketMap) mmtypes.MarketMap {
	mm := mmtypes.MarketMap{
		Markets: make(map[string]mmtypes.Market),
	}

	for _, market := range marketMap.Markets {
		newMarket := ConnectToSlinkyMarket(market)

		mm.Markets[newMarket.Ticker.String()] = newMarket
	}

	return mm
}

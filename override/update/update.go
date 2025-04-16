package update

import (
	"fmt"
	"strings"

	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	"github.com/dydxprotocol/slinky/x/marketmap/types/tickermetadata"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/client/dydx"
)

type Options struct {
	UpdateEnabled            bool
	OverwriteProviders       bool
	ExistingOnly             bool
	DisableDeFiMarketMerging bool
	CrossLaunchIDs           []string
}

// CombineMarketMaps adds the given generated markets to the actual market.
// If the market in generated does not exist in actual, append the whole market.
// If the market in actual does not exist in generated, append the whole market.
// If the market exists in actual AND generated, only append to the provider configs.
func CombineMarketMaps(
	logger *zap.Logger,
	actual, generated mmtypes.MarketMap,
	options Options,
	perps []dydx.Perpetual,
) (mmtypes.MarketMap, []string, error) {
	// allow for the input of fully empty market maps.  It is a valid case if the on-chain or generated market map is empty.
	if actual.Markets == nil {
		actual.Markets = make(map[string]mmtypes.Market)
	}

	if generated.Markets == nil {
		generated.Markets = make(map[string]mmtypes.Market)
	}

	combined := mmtypes.MarketMap{
		Markets: make(map[string]mmtypes.Market),
	}

	tickerToPerpetual := getTickerToPerpetual(perps)

	// update the enabled field of each market in the generated market-map
	for ticker, market := range generated.Markets {
		// generated market exists in the actual on chain map
		actualMarket, found := actual.Markets[ticker]
		if !found && options.ExistingOnly {
			// do not use markets that are not on chain if we only want to modify existing markets
			logger.Debug("not adding market because it is not in the actual market map",
				zap.String("ticker", ticker),
				zap.Bool("existing-only", options.ExistingOnly),
			)
			continue
		}

		if found {
			// Skip if generated market's CMC ID does not match the actual market's and actual market is enabled
			perp := tickerToPerpetual[ticker]
			skip, err := newMarketHasDifferentCMCID(logger, ticker, actualMarket, market, perp)
			if err != nil {
				return mmtypes.MarketMap{}, []string{}, err
			}
			if skip {
				combined.Markets[ticker] = actualMarket
				continue
			}

			if actualMarket.Ticker.Enabled && !options.UpdateEnabled {
				// if the market is enabled, but we are NOT updating enabled, keep it the set to actual
				logger.Debug("not updating market because it is already in the actual market map",
					zap.String("ticker", ticker),
					zap.Bool("update-enabled", options.UpdateEnabled),
				)
				market = actualMarket
			} else {
				logger.Debug("updating market that is is already in the actual market map",
					zap.String("ticker", ticker),
					zap.Bool("update-enabled", options.UpdateEnabled),
				)

				market.Ticker.Enabled = actualMarket.Ticker.Enabled
				market.Ticker.MinProviderCount = actualMarket.Ticker.MinProviderCount
				market.Ticker.Decimals = actualMarket.Ticker.Decimals

				updatedProviderConfigs := market.ProviderConfigs
				if !options.OverwriteProviders {
					updatedProviderConfigs = appendToProviders(actualMarket, market)
				}
				market.ProviderConfigs = updatedProviderConfigs
			}
		} else {
			logger.Debug("adding generated market that is not in the actual market map",
				zap.String("ticker", ticker),
			)

			// if not found in the on chain marketmap, add, but disable
			market.Ticker.Enabled = false
		}
		combined.Markets[ticker] = market
	}

	// append remove markets that are in actual, but NOT generated, unless the market is enabled
	removals := make([]string, 0)
	for ticker, market := range actual.Markets {
		if _, found := generated.Markets[ticker]; !found {
			if market.Ticker.Enabled {
				logger.Warn("Adding actual market that is not in the generated market map because it is enabled",
					zap.String("ticker", ticker),
				)
				combined.Markets[ticker] = market
			} else {
				removals = append(removals, ticker)
				logger.Debug("removing actual market that is not in the generated market map",
					zap.String("ticker", ticker),
				)
			}
		}
	}

	return combined, removals, nil
}

func getTickerToPerpetual(perps []dydx.Perpetual) map[string]dydx.Perpetual {
	tickerToPerpetual := make(map[string]dydx.Perpetual)
	for _, p := range perps {
		tickerParts := strings.Split(p.Params.Ticker, "-")
		mmTicker := strings.Join(tickerParts, "/")
		tickerToPerpetual[mmTicker] = p
	}
	return tickerToPerpetual
}

func newMarketHasDifferentCMCID(logger *zap.Logger, ticker string, actualMarket, newMarket mmtypes.Market, perp dydx.Perpetual) (bool, error) {
	if actualMarket.Ticker.Enabled {
		actualMetadataJSON := actualMarket.Ticker.GetMetadata_JSON()
		if actualMetadataJSON == "" {
			if perp.Params.MarketType == dydx.PERPETUAL_MARKET_TYPE_CROSS {
				return false, nil
			}
			logger.Warn("empty ticker metadata for existing market", zap.String("ticker", ticker))
			return false, nil
		}
		actualMetadata, err := tickermetadata.DyDxFromJSONString(actualMetadataJSON)
		if err != nil {
			return false, err
		}

		generatedMetadataJSON := newMarket.Ticker.GetMetadata_JSON()
		if generatedMetadataJSON == "" {
			return false, fmt.Errorf("empty ticker metadata for market %s", ticker)
		}
		generatedMetadata, err := tickermetadata.DyDxFromJSONString(generatedMetadataJSON)
		if err != nil {
			return false, err
		}

		if generatedMetadata.AggregateIDs[0].ID != actualMetadata.AggregateIDs[0].ID {
			logger.Warn("not adding market because the generated market has a different CMC ID than the actual market",
				zap.String("ticker", ticker),
				zap.String("generated_cmc_id", generatedMetadata.AggregateIDs[0].ID),
				zap.String("actual_cmc_id", actualMetadata.AggregateIDs[0].ID),
			)
			return true, nil
		}
	}
	return false, nil
}

func appendToProviders(actual, generated mmtypes.Market) []mmtypes.ProviderConfig {
	// create map of configs by their provider name
	actualProviderConfigsMap := make(map[string]mmtypes.ProviderConfig)
	for _, config := range actual.ProviderConfigs {
		actualProviderConfigsMap[config.Name] = config
	}

	// only update to the ProviderConfigs when they are new
	appendedProviderConfigs := actual.ProviderConfigs
	for _, generatedProviderConfig := range generated.ProviderConfigs {
		if _, found := actualProviderConfigsMap[generatedProviderConfig.Name]; !found {
			// if the provider config is not in the actual set, add it
			appendedProviderConfigs = append(appendedProviderConfigs, generatedProviderConfig)
		}
	}

	return appendedProviderConfigs
}

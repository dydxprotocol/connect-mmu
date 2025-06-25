package strategy

import (
	"encoding/json"
	"fmt"
	"strconv"

	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	"github.com/dydxprotocol/slinky/x/marketmap/types/tickermetadata"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
	"github.com/skip-mev/connect-mmu/upsert/sniff"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// GetMarketMapUpserts returns the sequence of market-map updates required to translate actual (on chain) to generated.
// Specifically, for any markets for which actual.Markets[ticker] != generated.Markets[ticker], we'll return
// a MsgUpsertMarket setting actual.Markets[ticker] to generated.Markets[ticker].
func GetMarketMapUpserts(
	logger *zap.Logger,
	actual,
	generated mmtypes.MarketMap,
	cmcIDMap map[int64]provider.AssetInfo,
	sniffClient sniff.Client,
) (updates []mmtypes.Market, additions []mmtypes.Market, err error) {
	// we need to make a copy of actual, since we'll be modifying it
	actualCopy := mmtypes.MarketMap{
		Markets: maps.Clone(actual.Markets),
	}

	// short circuit if they're equal
	if actual.Equal(generated) {
		logger.Info("both markets are equal - returning")
		return updates, additions, nil
	}

	generated, err = PruneNormalizeByPairs(logger, generated)
	if err != nil {
		logger.Error("PruneNormalizeByPairs failed", zap.Error(err))
		return nil, nil, err
	}

	// for each market in the generated market-map
	for ticker, market := range generated.Markets {
		// get the corresponding market in the actual market-map
		if actualMarket, ok := actualCopy.Markets[ticker]; ok {
			// if the market has not changed between the actual and generated market-maps, continue
			if market.Equal(actualMarket) {
				continue
			}
		} else {
			isScam, err := sniffToken(logger, cmcIDMap, market, sniffClient)
			if err != nil {
				logger.Error("TokenSniffer query failed", zap.Error(err))
				if isScam {
					logger.Info("TokenSniffer detected scam", zap.String("market", market.Ticker.String()))
					continue
				}
			}

			additions = append(additions, market)
			continue
		}

		// otherwise, for all markets pointed to by a normalize-by-pair in the generated market, check if they are in the actual market
		// if they are not, add them to the upserts + to actual (so we don't add them again)
		for _, providerConfig := range market.ProviderConfigs {
			// if the market has a normalize-by-pair market, check if it exists in the actual market-map
			if normalizeByPair := providerConfig.NormalizeByPair; normalizeByPair != nil {
				if _, ok := actualCopy.Markets[normalizeByPair.String()]; !ok {
					// find the market in generated (if this does not exist, fail)
					if normalizeByPairMarket, ok := generated.Markets[normalizeByPair.String()]; ok {
						// adjust by market exists, add the adjust-by via an upsert + add to the
						// actual market-map
						updates = append(updates, normalizeByPairMarket)
						actualCopy.Markets[normalizeByPairMarket.Ticker.String()] = normalizeByPairMarket
					} else {
						logger.Error("market normalize-by pair not found in generated marketmap",
							zap.String("market", ticker), zap.String("normalize pair", normalizeByPair.String()))
						return nil, nil, fmt.Errorf("market %s's normalize-by market %s not found in generated market-map", ticker, normalizeByPairMarket.String())
					}
				}
			}
		}

		// now that any of the necessary adjust-bys exist w/in the market-map, add the market to the upserts
		updates = append(updates, market)
		actualCopy.Markets[ticker] = market
	}

	// return all upserts + verify that the finalized market-map is valid
	if err := actualCopy.ValidateBasic(); err != nil {
		logger.Error("updated marketmap is invalid", zap.Error(err))
		return nil, nil, fmt.Errorf("updated market-map is invalid: %w", err)
	}

	return updates, additions, nil
}

func sniffToken(
	logger *zap.Logger,
	cmcIDMap map[int64]provider.AssetInfo,
	market mmtypes.Market,
	sniffClient sniff.Client,
) (bool, error) {
	var md tickermetadata.CoreMetadata
	if err := json.Unmarshal([]byte(market.Ticker.Metadata_JSON), &md); err != nil {
		return false, fmt.Errorf("failed to unmarshal market metadata for %q: %w", market.Ticker.String(), err)
	}
	for _, aggID := range md.AggregateIDs {
		if aggID.Venue == types.VenueCoinMarketcap {
			cmcID, err := strconv.ParseInt(aggID.ID, 10, 64)
			if err != nil {
				logger.Error("failed to parse CMC ID", zap.String("id", aggID.ID), zap.Error(err))
				continue
			}
			assetInfo, ok := cmcIDMap[cmcID]
			if ok {
				for _, multiAddress := range assetInfo.MultiAddresses {
					chain := multiAddress[0]
					contractAddress := multiAddress[1]
					logger.Info("checking if token is a scam", zap.String("chain", chain), zap.String("address", contractAddress), zap.String("symbol", assetInfo.Symbol))
					isScam, err := sniffClient.IsTokenAScam(chain, contractAddress)
					if err != nil {
						logger.Error("failed to check if token is a scam", zap.Error(err), zap.String("chain", chain), zap.String("address", contractAddress))
						continue
					}

					if isScam {
						logger.Info("filtering out scam token", zap.String("chain", chain), zap.String("address", contractAddress), zap.String("symbol", assetInfo.Symbol))
						return true, nil
					}

					// One pass is sufficient
					// TODO: analyze if we should be ranking chains for performance
					// TODO: cache results to avoid re-querying the same token - even between runs
					return false, nil
				}
			}
		}
	}

	logger.Info("Unable to query token asset info for scam check", zap.String("market", market.Ticker.String()), zap.String("metadata", string(market.Ticker.Metadata_JSON)))
	return false, nil
}

// PruneNormalizeByPairs removes any provider configs for enabled markets with providers with disabled normalized pairs from markets.
func PruneNormalizeByPairs(
	logger *zap.Logger,
	generated mmtypes.MarketMap,
) (mmtypes.MarketMap, error) {
	// make a copy of generated
	generatedCopy := mmtypes.MarketMap{
		Markets: maps.Clone(generated.Markets),
	}
	logger.Info("removing provider configs with disabled normalize by pairs")
	// remove any provider configs for enabled markets with providers with disabled adjust bys from markets
	for key, market := range generatedCopy.Markets {
		if market.Ticker.Enabled {
			var newProviderConfig []mmtypes.ProviderConfig
			for _, pc := range market.ProviderConfigs {
				if pc.NormalizeByPair != nil {
					norm, found := generatedCopy.Markets[pc.NormalizeByPair.String()]
					if !found {
						return mmtypes.MarketMap{}, fmt.Errorf("unable to find normalize for %s",
							pc.NormalizeByPair.String())
					}
					// only include enabled ticker that is a normalize by pair
					if norm.Ticker.Enabled {
						// include
						newProviderConfig = append(newProviderConfig, pc)
					} else {
						// exclude -> remove
						logger.Info("removing disabled provider configs",
							zap.String("market", market.Ticker.String()),
							zap.String("provider config", pc.NormalizeByPair.String()),
						)
					}
				} else {
					newProviderConfig = append(newProviderConfig, pc)
				}
			}
			// only add the market if it still has enough providers after pruning
			if uint64(len(newProviderConfig)) >= market.Ticker.MinProviderCount {
				market.ProviderConfigs = newProviderConfig
				generatedCopy.Markets[key] = market
			} else {
				delete(generatedCopy.Markets, key)
				logger.Debug("excluding market because it was pruned",
					zap.String("market", market.Ticker.String()),
					zap.Int("num providers", len(newProviderConfig)),
					zap.Uint64("required providers", market.Ticker.MinProviderCount),
				)
			}
		}
	}
	return generatedCopy, nil
}

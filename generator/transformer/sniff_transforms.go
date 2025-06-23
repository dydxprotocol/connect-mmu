package transformer

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
)

type TransformSniff func(ctx context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo, sniffClient SniffClient) (types.Feeds, types.ExclusionReasons, error)

func SniffOutScamTokens() TransformSniff {
	return func(ctx context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo, sniffClient SniffClient) (types.Feeds, types.ExclusionReasons, error) {
		logger.Info("sniffing assets", zap.Int("feeds", len(feeds)))
		out := make([]types.Feed, 0, len(feeds))
		exclusions := types.NewExclusionReasons()
		for _, feed := range feeds {
			assetInfo := cmcIDToAssetInfo[feed.CMCInfo.BaseID]
			scam := false
			for _, multiAddress := range assetInfo.MultiAddresses {
				chain := multiAddress[0]
				contractAddress := multiAddress[1]

				isScam, err := sniffClient.IsTokenAScam(ctx, chain, contractAddress)
				if err != nil {
					logger.Error("failed to check if token is a scam", zap.Error(err), zap.String("chain", chain), zap.String("address", contractAddress))
					continue
				}

				if isScam {
					logger.Info("filtering out scam token", zap.String("chain", chain), zap.String("address", contractAddress), zap.String("symbol", assetInfo.Symbol))
					exclusions.AddExclusionReasonFromFeed(feed, feed.ProviderConfig.Name, fmt.Sprintf("Filtering out scam token: ID: %d | Address: %s | Symbol: %s", feed.CMCInfo.BaseID, contractAddress, assetInfo.Symbol))
					scam = true
				}

				break // Execute scam check for just one chain
			}
			if !scam {
				out = append(out, feed)
			}
		}

		logger.Info("filtered scam tokens", zap.Int("feeds length", len(feeds)), zap.Int("out length", len(out)))
		return out, exclusions, nil
	}
}

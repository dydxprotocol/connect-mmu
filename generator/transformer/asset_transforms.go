package transformer

import (
	"context"
	"strings"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
	"go.uber.org/zap"
)

type TransformAsset func(ctx context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo) (types.Feeds, types.ExclusionReasons, error)

func FilterOutCMCTags() TransformAsset {
	return func(_ context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo) (types.Feeds, types.ExclusionReasons, error) {
		logger.Info("filtering out cmc tags", zap.Int("feeds", len(feeds)))

		tagsToFilterOut := cfg.ExcludeCMCTags
		out := make([]types.Feed, 0, len(feeds))
		exclusions := types.NewExclusionReasons()

		for _, feed := range feeds {
			baseAssetCMCID := feed.CMCInfo.BaseID
			assetInfo := cmcIDToAssetInfo[baseAssetCMCID]
			if HasCMCTag(assetInfo, tagsToFilterOut) {
				logger.Debug("dropping feed", zap.Any("feed", feed))
				exclusions.AddExclusionReasonFromFeed(feed, feed.ProviderConfig.Name,
					"FilterOutCMCTags: has CMC tags to filter out "+strings.Join(tagsToFilterOut, ", "))
				continue
			}

			out = append(out, feed)
		}

		logger.Info("filtered out cmc tags", zap.Int("feeds remaining", len(out)))
		return out, exclusions, nil
	}
}

func HasCMCTag(assetInfo provider.AssetInfo, tagsToFilterOut []string) bool {
	for _, filterTag := range tagsToFilterOut {
		for _, assetTag := range assetInfo.CMCTags {
			if assetTag == filterTag {
				return true
			}
		}
	}
	return false
}

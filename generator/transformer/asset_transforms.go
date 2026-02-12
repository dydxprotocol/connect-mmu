package transformer

import (
	"context"
	"slices"
	"strings"
	"strconv"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
)

type TransformAsset func(ctx context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo) (types.Feeds, types.ExclusionReasons, error)

func FilterOutCMCTags() TransformAsset {
	return func(_ context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo) (types.Feeds, types.ExclusionReasons, error) {
		logger.Info("filtering out cmc tags", zap.Int("feeds", len(feeds)))

		tagsToExclude := cfg.ExcludeCMCTags
		out := make([]types.Feed, 0, len(feeds))
		exclusions := types.NewExclusionReasons()

		for _, feed := range feeds {
			baseAssetCMCID := feed.CMCInfo.BaseID
			assetInfo := cmcIDToAssetInfo[baseAssetCMCID]
			if HasCMCTag(assetInfo, tagsToExclude) {
				logger.Debug("dropping feed because it has excluded CMC tags", zap.Any("feed", feed))
				exclusions.AddExclusionReasonFromFeed(feed, feed.ProviderConfig.Name,
					"FilterOutCMCTags: has CMC tags to exclude "+strings.Join(tagsToExclude, ", "))
				continue
			}

			out = append(out, feed)
		}

		logger.Info("filtered out cmc tags", zap.Int("feeds remaining", len(out)))
		return out, exclusions, nil
	}
}

func FilterOutCMCIDs() TransformAsset {
	return func(_ context.Context, logger *zap.Logger, cfg config.GenerateConfig, feeds types.Feeds, cmcIDToAssetInfo map[int64]provider.AssetInfo) (types.Feeds, types.ExclusionReasons, error) {
		idsToExclude := cfg.ExcludeCMCIDs
		logger.Info("filtering out cmc ids", zap.Int("feeds", len(feeds)), zap.Strings("ids", idsToExclude))
		out := make([]types.Feed, 0, len(feeds))
		exclusions := types.NewExclusionReasons()

		for _, feed := range feeds {
			baseAssetCMCID := feed.CMCInfo.BaseID
			if slices.Contains(idsToExclude, strconv.FormatInt(baseAssetCMCID, 10)) {
				logger.Info("dropping feed because it has excluded CMC ids", zap.Any("feed", feed))
				exclusions.AddExclusionReasonFromFeed(feed, feed.ProviderConfig.Name, "FilterOutCMCID: has cmc id to exclude")
				continue
			}
			out = append(out, feed)
		}

		logger.Info("filtering out cmc ids", zap.Int("feeds remaining", len(out)))
		return out, exclusions, nil
	}
}

func HasCMCTag(assetInfo provider.AssetInfo, tagsToExclude []string) bool {
	for _, tagToExclude := range tagsToExclude {
		for _, assetTag := range assetInfo.CMCTags {
			if assetTag == tagToExclude {
				return true
			}
		}
	}
	return false
}

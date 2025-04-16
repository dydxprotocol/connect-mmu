package generator

import (
	"context"

	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/generator/querier"
	"github.com/skip-mev/connect-mmu/generator/transformer"
	"github.com/skip-mev/connect-mmu/generator/types"
	"github.com/skip-mev/connect-mmu/store/provider"
)

type Generator struct {
	logger *zap.Logger

	q querier.Querier
	t transformer.Transformer
}

func New(logger *zap.Logger, providerStore provider.Store) Generator {
	return Generator{
		logger: logger.With(zap.String("mmu-service", "generator")),
		q:      querier.New(logger, providerStore),
		t:      transformer.New(logger),
	}
}

func (g *Generator) GenerateMarketMap(
	ctx context.Context,
	cfg config.GenerateConfig,
	onChainMarketMap mmtypes.MarketMap,
) (mmtypes.MarketMap, types.ExclusionReasons, error) {
	feeds, err := g.q.Feeds(ctx, cfg)
	if err != nil {
		g.logger.Error("Unable to query feeds", zap.Error(err))
		return mmtypes.MarketMap{}, nil, err
	}

	g.logger.Info("queried", zap.Int("feeds", len(feeds)))

	// Transform Feeds
	transformed, dropped, err := g.t.TransformFeeds(ctx, cfg, feeds, onChainMarketMap)
	if err != nil {
		g.logger.Error("Unable to transform feeds", zap.Error(err))
		return mmtypes.MarketMap{}, nil, err
	}

	g.logger.Info("feed transforms complete", zap.Int("remaining feeds", len(transformed)))

	// Transform Assets (requires additional data about the asset)
	g.logger.Info("transforming assets", zap.Int("markets", len(transformed)))
	cmcIDToAssetInfo, err := g.q.CMCIDToAssetInfo(ctx, cfg)
	if err != nil {
		g.logger.Error("Unable to query asset infos", zap.Error(err))
		return mmtypes.MarketMap{}, nil, err
	}

	transformed, droppedMarkets, err := g.t.TransformAssets(ctx, cfg, transformed, cmcIDToAssetInfo)
	if err != nil {
		g.logger.Error("Unable to transform assets in market map", zap.Error(err))
		return mmtypes.MarketMap{}, nil, err
	}
	dropped.Merge(droppedMarkets)

	// Transform Market Map
	mm, err := transformed.ToMarketMap()
	if err != nil {
		g.logger.Error("Unable to transform feeds to a MarketMap", zap.Error(err))
		return mmtypes.MarketMap{}, nil, err
	}

	mm, droppedMarkets, err = g.t.TransformMarketMap(ctx, cfg, mm)
	if err != nil {
		g.logger.Error("Unable to transform market map", zap.Error(err))
		return mm, nil, err
	}
	dropped.Merge(droppedMarkets)
	g.logger.Info("market map transforms complete", zap.Int("remaining markets", len(mm.Markets)))

	g.logger.Info("final market", zap.Int("size", len(mm.Markets)))

	return mm, dropped, nil
}

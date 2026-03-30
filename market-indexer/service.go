package indexer

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/market-indexer/coinmarketcap"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/binance"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/bitfinex"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/bitstamp"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/bybit"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/coinbase"
	crypto_com "github.com/skip-mev/connect-mmu/market-indexer/ingesters/crypto.com"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gate"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gecko"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/huobi"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/kraken"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/kucoin"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/mexc"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/okx"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/raydium"
	"github.com/skip-mev/connect-mmu/market-indexer/utils"
	"github.com/skip-mev/connect-mmu/store/provider"
)

// Indexer is the service that creates and runs a set of Ingesters
// and writes the market data to a configured database.
type Indexer struct {
	logger *zap.Logger

	igs        []ingesters.Ingester
	cmcIndexer *coinmarketcap.Indexer

	providerStore provider.Store

	config config.MarketConfig

	// knownAssets is a local cache of the known assets in the AssetsInfo table.
	knownAssets utils.AssetMap

	archiveIntermediateSteps bool
}

const coinMarketCapKey = "CMC_API_KEY"

// NewIndexer creates a new Indexer with the provided config.
func NewIndexer(cfg config.MarketConfig, logger *zap.Logger, writer provider.Store, archiveIntermediateSteps bool) (*Indexer, error) {
	envCMCKey := os.Getenv(coinMarketCapKey)
	if envCMCKey != "" {
		cfg.CoinMarketCapConfig.APIKey = envCMCKey
	}

	svc := Indexer{
		logger:                   logger.With(zap.String("mmu-service", "indexer")),
		providerStore:            writer,
		cmcIndexer:               coinmarketcap.New(logger, cfg.CoinMarketCapConfig.APIKey),
		config:                   cfg,
		knownAssets:              make(utils.AssetMap),
		archiveIntermediateSteps: archiveIntermediateSteps,
	}

	igs := make([]ingesters.Ingester, len(cfg.Ingesters))
	for i, ingestConfig := range cfg.Ingesters {
		switch ingestConfig.Name {
		case kucoin.Name:
			igs[i] = kucoin.New(logger)
		case crypto_com.Name:
			igs[i] = crypto_com.New(logger)
		case kraken.Name:
			igs[i] = kraken.New(logger)
		case bitfinex.Name:
			igs[i] = bitfinex.New(logger)
		case bybit.Name:
			igs[i] = bybit.New(logger)
		case binance.Name:
			igs[i] = binance.New(logger)
		case okx.Name:
			igs[i] = okx.New(logger)
		case coinbase.Name:
			igs[i] = coinbase.New(logger)
		case mexc.Name:
			igs[i] = mexc.New(logger)
		case gate.Name:
			igs[i] = gate.New(logger)
		case bitstamp.Name:
			igs[i] = bitstamp.New(logger)
		case huobi.Name:
			igs[i] = huobi.New(logger)
		case raydium.Name:
			igs[i] = raydium.New(logger, cfg, cfg.CoinMarketCapConfig.APIKey)
		case gecko.Name:
			igs[i] = gecko.New(logger, cfg)
		default:
			return nil, fmt.Errorf("provider %s is unsupported", ingestConfig.Name)
		}
	}
	svc.igs = igs
	return &svc, nil
}

// Index collects market data for each ingester and returns the combined data.
// Ingester fetches run in parallel, then association and store writes run sequentially.
func (idx *Indexer) Index(ctx context.Context) error {
	cmcMarketPairs, err := idx.SetupAssets(ctx)
	if err != nil {
		idx.logger.Error("error setting up known assets", zap.Error(err))
		return err
	}

	type ingesterResult struct {
		name    string
		markets []provider.CreateProviderMarket
	}
	results := make([]ingesterResult, len(idx.igs))

	// Phase 1: fetch all ingester markets in parallel.
	// Each ingester hits its own independent exchange API, so there is no shared
	// mutable state during this phase.
	g, gCtx := errgroup.WithContext(ctx)
	for i, ingester := range idx.igs {
		g.Go(func() error {
			idx.logger.Info("starting fetch", zap.String("ingester", ingester.Name()))

			markets, err := ingester.GetProviderMarkets(gCtx)
			if err != nil {
				idx.logger.Error("error getting markets", zap.Bool("mmu_datadog", true), zap.String("ingester", ingester.Name()), zap.Error(err))
				return fmt.Errorf("ingester %s: %w", ingester.Name(), err)
			}

			results[i] = ingesterResult{name: ingester.Name(), markets: markets}
			idx.logger.Info("finished fetch", zap.String("ingester", ingester.Name()), zap.Int("markets", len(markets)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// Phase 2: associate aggregator data and write to store sequentially.
	// This phase reads/writes knownAssets and cmcIndexer state that is not
	// safe for concurrent access.
	count := 0
	for _, result := range results {
		idx.logger.Info("associating coin market cap for provider", zap.String("ingester", result.name))
		transformed, err := idx.AssociateAggregator(ctx, result.markets, cmcMarketPairs)
		if err != nil {
			idx.logger.Error("error associating aggregators", zap.Bool("mmu_datadog", true), zap.String("ingester", result.name), zap.Error(err))
			return err
		}
		idx.logger.Info("associated coin market cap for provider", zap.String("ingester", result.name), zap.Int("markets", len(transformed)))

		for _, pm := range transformed {
			if _, err := idx.providerStore.AddProviderMarket(ctx, pm.Create); err != nil {
				return err
			}
		}

		count += len(transformed)
		idx.logger.Info("finished", zap.String("ingester", result.name), zap.Int("num markets", len(transformed)))
	}

	idx.logger.Info("committing provider markets tx to store...", zap.Int("total markets", count))

	return nil
}

// AssociateAggregator associates market aggregator data with each provider market to be written to the db.
func (idx *Indexer) AssociateAggregator(
	ctx context.Context,
	inputs []provider.CreateProviderMarket,
	providerMarketPairs coinmarketcap.ProviderMarketPairs,
) ([]provider.CreateProviderMarket, error) {
	return idx.AssociateCoinMarketCap(ctx, inputs, providerMarketPairs)
}

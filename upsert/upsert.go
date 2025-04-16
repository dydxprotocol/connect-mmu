package upsert

import (
	"errors"
	"fmt"
	"slices"

	"github.com/dydxprotocol/slinky/x/marketmap/types"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/upsert/strategy"
)

// Generator is a type that facilitates generating market upserts.
type Generator struct {
	logger      *zap.Logger
	cfg         config.UpsertConfig
	generatedMM types.MarketMap
	currentMM   types.MarketMap
}

// New returns a new upsert generator.
func New(
	logger *zap.Logger,
	cfg config.UpsertConfig,
	generated, current types.MarketMap,
) (*Generator, error) {
	var err error
	current, err = current.GetValidSubset()
	if err != nil {
		return nil, fmt.Errorf("failed to get valid subset of markets from on-chain marketmap: %w", err)
	}

	generated, err = generated.GetValidSubset()
	if err != nil {
		return nil, fmt.Errorf("failed to get valid subset of markets from generated marketmap: %w", err)
	}

	return &Generator{
		logger:      logger,
		cfg:         cfg,
		generatedMM: generated,
		currentMM:   current,
	}, nil
}

// GenerateUpserts generates a slice of market upserts.
func (d *Generator) GenerateUpserts() (updates []types.Market, additions []types.Market, err error) {
	updates, additions, err = strategy.GetMarketMapUpserts(d.logger, d.currentMM, d.generatedMM)
	if err != nil {
		d.logger.Error("failed to get marketmap updates and additions", zap.Error(err))
		return nil, nil, err
	}
	updates = removeFromUpdates(updates, d.cfg.RestrictedMarkets)
	d.logger.Info("determined updates", zap.Int("updates", len(updates)))

	// reorder so that any new normalize by markets are first
	updates, err = orderNormalizeMarketsFirst(updates)
	if err != nil {
		d.logger.Error("failed to reorder upserts", zap.Error(err))
		return nil, nil, err
	}

	// early exit if there are no upserts
	if len(updates) == 0 {
		d.logger.Info("no upserts found - returning")
		return updates, additions, nil
	}

	errs := make([]error, 0)
	for _, update := range updates {
		if err := update.ValidateBasic(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return nil, nil, fmt.Errorf("generated %d invalid market(s): %w", len(errs), errors.Join(errs...))
	}

	if err := validateUpdates(d.currentMM, updates); err != nil {
		return nil, nil, fmt.Errorf("generated invalid upserts in marketmap: %w", err)
	}

	return updates, additions, nil
}

// validateUpdates adds the upserts to a marketmap, and validates the configuration.
func validateUpdates(currentMM types.MarketMap, updates []types.Market) error {
	for _, update := range updates {
		currentMM.Markets[update.Ticker.String()] = update
	}
	return currentMM.ValidateBasic()
}

// removeFromUpdates removes the specified markets from the updates slice.
func removeFromUpdates(updates []types.Market, remove []string) []types.Market {
	if len(remove) == 0 {
		return updates
	}

	if len(updates) == 0 {
		return nil
	}

	filtered := make([]types.Market, 0)
	for _, update := range updates {
		if !slices.Contains(remove, update.Ticker.String()) {
			filtered = append(filtered, update)
		}
	}
	return filtered
}

// orderNormalizeMarketsFirst reorders markets such that markets that are used in "normalize_by_pair" are ordered first.
func orderNormalizeMarketsFirst(upserts []types.Market) ([]types.Market, error) {
	output := make([]types.Market, 0, len(upserts))

	// create map for checking
	upsertsAsMap := make(map[string]types.Market)
	for _, upsert := range upserts {
		upsertsAsMap[upsert.Ticker.String()] = upsert
	}

	seenNormalizeBys := make(map[string]struct{})
	for _, upsert := range upserts {
		for _, pc := range upsert.ProviderConfigs {
			if pc.NormalizeByPair != nil {
				ticker := pc.NormalizeByPair.String()

				// if the normalize pair exists in our upserts, assume it is newly added
				// it may be just being updated, but moving it to the front will have no side effects
				if market, found := upsertsAsMap[ticker]; found {
					if _, ok := seenNormalizeBys[ticker]; !ok {
						// push back this pair to our array if we have not seen it yet
						output = append(output, market)
						seenNormalizeBys[ticker] = struct{}{}
					}
				}
			}
		}
	}

	// push back remaining markets
	for ticker, market := range upsertsAsMap {
		if _, ok := seenNormalizeBys[ticker]; !ok {
			output = append(output, market)
		}
	}

	if len(output) != len(upserts) {
		return nil, fmt.Errorf("invalid reorder: expected %d outputs, got %d", len(upserts), len(output))
	}

	return output, nil
}

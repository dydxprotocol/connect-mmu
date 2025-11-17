package binance

import (
	"context"
	"strings"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/market-indexer/ingesters"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/types"
	"github.com/skip-mev/connect-mmu/store/provider"
)

const (
	Name         = "binance"
	ProviderName = Name + types.ProviderNameSuffixWS
)

var _ ingesters.Ingester = &Ingester{}

// Ingester is the binance implementation of a market data Ingester.
type Ingester struct {
	logger *zap.Logger

	client Client
}

// New creates a new binance Ingester.
func New(logger *zap.Logger) *Ingester {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Ingester{
		logger: logger.With(zap.String("ingester", Name)),
		client: NewHTTPClient(),
	}
}

// NewWithClient creates a new binance Ingester.
func NewWithClient(logger *zap.Logger, client Client) *Ingester {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Ingester{
		logger: logger.With(zap.String("ingester", Name)),
		client: client,
	}
}

func (i *Ingester) GetProviderMarkets(ctx context.Context) ([]provider.CreateProviderMarket, error) {
	i.logger.Info("fetching data")

	tickers, err := i.client.Tickers(ctx)
	if err != nil {
		return nil, err
	}

	pms := make([]provider.CreateProviderMarket, 0, len(tickers))
	for _, ticker := range tickers {
		// markets that are wound down have first id and last id -1.
		if ticker.FirstID == -1 && ticker.LastID == -1 {
			continue
		}
		// Skip tickers that start with "1000" (like 1000SATSUSDT) or "1M" (like 1MBABYDOGEUSDT) bc the price will be off from other providers
		// TODO: Remove this when sidecar supports adjusting the price for these binance markets
		if strings.HasPrefix(ticker.Symbol, "1000") || strings.HasPrefix(ticker.Symbol, "1M") {
			continue
		}

		pm, err := ticker.toProviderMarket()
		if err != nil {
			i.logger.Error("failed to convert ticker to providerMarket", zap.Error(err), zap.String("ingester", Name), zap.Any("ticker", ticker))
			continue
		}

		pms = append(pms, pm)
	}

	return pms, nil
}

// Name returns the Ingester's human-readable name.
func (i *Ingester) Name() string {
	return Name
}

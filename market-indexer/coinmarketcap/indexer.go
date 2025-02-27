package coinmarketcap

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	skipslices "github.com/skip-mev/connect-mmu/lib/slices"
	"github.com/skip-mev/connect-mmu/lib/symbols"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	cmc "github.com/skip-mev/connect-mmu/market-indexer/api/coinmarketcap"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/coinbase"
	crypto_com "github.com/skip-mev/connect-mmu/market-indexer/ingesters/crypto.com"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gate"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gecko"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/huobi"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/names"
	"github.com/skip-mev/connect-mmu/types"
)

const (
	exchangeStatusActive   = 1
	exchangeStatusInactive = 0
)

type Indexer struct {
	logger *zap.Logger

	client   cmc.Client
	quotes   map[int64]cmc.QuoteData
	cmcIDMap map[int]cmc.CryptoIDMapData
}

// New creates a new coinmarketcap Indexer.
func New(logger *zap.Logger, apiKey string) *Indexer {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Indexer{
		logger:   logger.With(zap.String("indexer", cmc.Name)),
		client:   cmc.NewHTTPClient(apiKey),
		quotes:   make(map[int64]cmc.QuoteData),
		cmcIDMap: make(map[int]cmc.CryptoIDMapData),
	}
}

// NewWithClient creates a new coinmarketcap Indexer.
func NewWithClient(logger *zap.Logger, client cmc.Client) *Indexer {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Indexer{
		logger:   logger.With(zap.String("ingester", cmc.Name)),
		client:   client,
		quotes:   make(map[int64]cmc.QuoteData),
		cmcIDMap: make(map[int]cmc.CryptoIDMapData),
	}
}

func (i *Indexer) GetClient() cmc.Client {
	return i.client
}

type WrappedCryptoIDMapData struct {
	IDMap cmc.CryptoIDMapData
	Info  cmc.InfoData
}

// CryptoIDMap is an alias for the data payload returned from the CoinMarketCap API.
type CryptoIDMap []WrappedCryptoIDMapData

// CryptoIDMap returns the map of all crypto CMC IDs to asset names using its underlying client.
func (i *Indexer) CryptoIDMap(ctx context.Context) (CryptoIDMap, error) {
	i.logger.Info("fetching crypto id data")

	resp, err := i.client.CryptoIDMap(ctx)
	if err != nil {
		return nil, err
	}

	wrapped := make(CryptoIDMap, len(resp.Data))
	ids := make([]int64, len(resp.Data))
	for i, data := range resp.Data {
		ids[i] = int64(data.ID)
	}

	const reqSize = 1000
	infoDataMap := make(cmc.InfoDataMap, len(resp.Data))
	splitIDs := skipslices.Chunk(ids, reqSize)
	for _, split := range splitIDs {
		infoResp, err := i.client.Info(ctx, split)
		if err != nil {
			return nil, err
		}

		if err := resp.Status.Validate(); err != nil {
			i.logger.Error("error in info status", zap.Error(err), zap.Any("ids", split))
			return nil, err
		}

		// copy will overwrite duplicate keys but the keys will be unique
		maps.Copy(infoDataMap, infoResp.Data)
	}

	for i, data := range resp.Data {
		info, ok := infoDataMap[strconv.Itoa(data.ID)]
		if !ok {
			return nil, fmt.Errorf("unknown crypto id %d", data.ID)
		}

		wrapped[i] = WrappedCryptoIDMapData{
			IDMap: data,
			Info:  info,
		}
	}

	return wrapped, nil
}

// FiatIDMap is an alias for the data payload returned from the CoinMarketCap API.
type FiatIDMap []cmc.FiatData

// FiatIDMap returns the map of all fiat CMC IDs to asset names using its underlying client.
func (i *Indexer) FiatIDMap(ctx context.Context) (FiatIDMap, error) {
	i.logger.Info("fetching fiat id data")

	resp, err := i.client.FiatMap(ctx)
	if err != nil {
		return nil, err
	}

	if err := resp.Status.Validate(); err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func (i *Indexer) CacheQuotes(ctx context.Context, ids []int64) (map[int64]struct{}, error) {
	failedQuotes := make(map[int64]struct{}, 0)
	for _, chunk := range skipslices.Chunk(ids, 1000) {
		resp, failedQuotesChunk, err := i.Quotes(ctx, chunk)
		if err != nil {
			return nil, err
		}

		for id, data := range resp {
			i.quotes[id] = data
		}

		if failedQuotesChunk != nil && len(failedQuotesChunk) > 0 {
			for id, _ := range failedQuotesChunk {
				failedQuotes[id] = struct{}{}
			}
		}
	}

	return failedQuotes, nil
}

// Quotes fetches the QuoteData for the given CMC IDs and returns them as a map.
// If a desired ID is not returned, we fall back to individually fetch the data for the ID,
// and return an error if that fails.
func (i *Indexer) Quotes(ctx context.Context, ids []int64) (map[int64]cmc.QuoteData, map[int64]struct{}, error) {
	i.logger.Debug("fetching quote data", zap.Any("cmc ids", ids))

	resp, err := i.client.Quotes(ctx, ids)
	if err != nil {
		i.logger.Error("unable to fetch quote data", zap.Error(err))
		return nil, nil, err
	}

	if err = resp.Status.Validate(); err != nil {
		i.logger.Error("failed to validate response", zap.Error(err))
		return nil, nil, err
	}

	quotes := make(map[int64]cmc.QuoteData)
	failedQuotes := make(map[int64]struct{}, 0)
	for _, id := range ids {
		data, ok := resp.Data[fmt.Sprintf("%d", id)]
		if !ok {
			i.logger.Error("desired symbol not found - retrying", zap.Int64("id", id))
			data, err = i.Quote(ctx, id)
			if err != nil {
				i.logger.Error("unable to fetch quote data", zap.Int64("id", id), zap.Error(err))
				failedQuotes[id] = struct{}{}
				continue
			}
		}
		quotes[id] = data
	}

	return quotes, failedQuotes, nil
}

// Quote returns a quote from the CMC ID and symbol using its underlying client.
func (i *Indexer) Quote(ctx context.Context, id int64) (cmc.QuoteData, error) {
	i.logger.Debug("fetching quote data", zap.Int64("cmc id", id))

	if data, ok := i.quotes[id]; ok {
		return data, nil
	}

	resp, err := i.client.Quote(ctx, id)
	if err != nil {
		i.logger.Error("unable to fetch quote data", zap.Error(err))
		return cmc.QuoteData{}, err
	}

	if err := resp.Status.Validate(); err != nil {
		i.logger.Error("failed to validate response", zap.Error(err))
		return cmc.QuoteData{}, err
	}

	// lookup by string rep of ID
	if data, ok := resp.Data[fmt.Sprintf("%d", id)]; ok {
		return data, nil
	}

	i.logger.Error("desired symbol not found", zap.Error(err), zap.Any("data", resp.Data))
	return cmc.QuoteData{}, fmt.Errorf("quote data not found for id: %d, %w", id, err)
}

type ProviderMarketPairs struct {
	Data map[string]ProviderMarketData `json:"data"`
}

func NewProviderMarketPairs() ProviderMarketPairs {
	return ProviderMarketPairs{
		Data: make(map[string]ProviderMarketData),
	}
}

func ProviderMarketPairKey(providerName, baseAsset, quoteAsset string) string {
	return fmt.Sprintf("%s_%s_%s", providerName, baseAsset, quoteAsset)
}

type ProviderMarketData struct {
	BaseAsset      string                  `json:"base_asset"`
	QuoteAsset     string                  `json:"quote_asset"`
	QuoteVolume    float64                 `json:"quote_volume"`
	UsdVolume      float64                 `json:"usd_volume"`
	CMCInfo        types.CoinMarketCapInfo `json:"coinmarketcap_info"`
	MetadataJSON   []byte                  `json:"metadata_json"`
	ReferencePrice float64                 `json:"reference_price"`
	LiquidityInfo  types.LiquidityInfo     `json:"liquidity_info"`
}

func (i *Indexer) GetProviderMarketsPairs(ctx context.Context, cfg config.MarketConfig) (ProviderMarketPairs, error) {
	i.logger.Info("fetching data for provider markets")

	pmps := NewProviderMarketPairs()

	ingesterIDs, err := i.getIngesterMapping(ctx, cfg)
	if err != nil {
		return pmps, err
	}

	for name, id := range ingesterIDs {
		i.logger.Info("fetching cmc market data", zap.String("exchange", name))

		markets, err := i.client.ExchangeMarkets(ctx, id)
		if err != nil {
			return pmps, err
		}

		if err := markets.Status.Validate(); err != nil {
			return pmps, err
		}

		i.logger.Info("fetched cmc market data", zap.String("exchange", name), zap.Int("num markets", markets.Data.NumMarketPairs))
		for _, pair := range markets.Data.MarketPairs {
			cmcBaseSymbol, err := symbols.ToTickerString(pair.MarketPairBase.CurrencySymbol)
			if err != nil {
				return ProviderMarketPairs{}, err
			}
			cmcQuoteSymbol, err := symbols.ToTickerString(pair.MarketPairQuote.CurrencySymbol)
			if err != nil {
				return ProviderMarketPairs{}, err
			}

			key := ProviderMarketPairKey(
				names.GetProviderName(name),
				pair.MarketPairBase.ExchangeSymbol,
				pair.MarketPairQuote.ExchangeSymbol,
			)

			idInfo := types.CoinMarketCapInfo{
				BaseID:  int64(pair.MarketPairBase.CurrencyID),
				QuoteID: int64(pair.MarketPairQuote.CurrencyID),
			}

			liquidityInfo := types.LiquidityInfo{
				NegativeDepthTwo: pair.Quote.USD.DepthNegativeTwo,
				PositiveDepthTwo: pair.Quote.USD.DepthPositiveTwo,
			}

			newMarketData := ProviderMarketData{
				BaseAsset:      cmcBaseSymbol,
				QuoteAsset:     cmcQuoteSymbol,
				QuoteVolume:    pair.Quote.ExchangeReported.Volume24HQuote,
				UsdVolume:      pair.Quote.USD.Volume24H,
				CMCInfo:        idInfo,
				MetadataJSON:   nil,
				ReferencePrice: pair.Quote.ExchangeReported.Price,
				LiquidityInfo:  liquidityInfo,
			}

			if existing, exists := pmps.Data[key]; exists {
				i.logger.Error("key already exists in pmps.Data", zap.String("key", key), zap.Any("existing", existing), zap.Any("new", newMarketData))
				continue
			}

			pmps.Data[key] = newMarketData
		}
	}

	return pmps, nil
}

type IngesterIDMap map[string]int

func (i *Indexer) getIngesterMapping(ctx context.Context, cfg config.MarketConfig) (IngesterIDMap, error) {
	exchangeMapResp, err := i.client.ExchangeIDMap(ctx)
	if err != nil {
		return nil, err
	}

	if err := exchangeMapResp.Status.Validate(); err != nil {
		return nil, err
	}

	exchangeNameToID := make(map[string]int, len(exchangeMapResp.Data))
	for _, data := range exchangeMapResp.Data {
		if data.IsActive == exchangeStatusActive {
			exchangeNameToID[data.Slug] = data.ID
		}
	}

	ingesterNameToID := make(IngesterIDMap, len(cfg.Ingesters))
	addNameToMap := func(cmcName, ingesterName string) error {
		if id, found := exchangeNameToID[cmcName]; found {
			ingesterNameToID[ingesterName] = id
		} else {
			i.logger.Error("could not find an ingester", zap.String("ingester", ingesterName), zap.Any("items", exchangeNameToID))
			return fmt.Errorf("could not find an ingester named %s", ingesterName)
		}

		return nil
	}

	for _, ingester := range cfg.Ingesters {
		if ingester.Name == gecko.Name {
			for _, pair := range cfg.GeckoNetworkDexPairs {
				name := resolveIngesterNameAliases(pair.Dex)
				err := addNameToMap(name, pair.Dex)
				if err != nil {
					return nil, err
				}
			}

			continue
		}

		name := resolveIngesterNameAliases(ingester.Name)
		err := addNameToMap(name, ingester.Name)
		if err != nil {
			return nil, err
		}
	}

	return ingesterNameToID, nil
}

// resolveIngesterNameAliases resolves any sauron ingester names to their corresponding
// slug names on cointmarketcap.
func resolveIngesterNameAliases(ingesterName string) string {
	switch ingesterName {
	case coinbase.Name:
		return "coinbase-exchange"
	case crypto_com.Name:
		return "crypto-com-exchange"
	case gate.Name:
		return "gate-io"
	case huobi.Name:
		return "htx"
	case "uniswap_v3":
		return "uniswap-v3"

	default:
		return ingesterName
	}
}

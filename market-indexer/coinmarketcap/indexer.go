package coinmarketcap

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	"github.com/skip-mev/connect-mmu/lib/file"
	"github.com/skip-mev/connect-mmu/lib/slices"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/coinbase"
	crypto_com "github.com/skip-mev/connect-mmu/market-indexer/ingesters/crypto.com"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gate"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/gecko"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/huobi"
	"github.com/skip-mev/connect-mmu/market-indexer/ingesters/names"
	"github.com/skip-mev/connect-mmu/types"
)

type Indexer struct {
	logger *zap.Logger

	client   Client
	quotes   map[int64]QuoteData
	cmcIDMap map[int]CryptoIDMapData
}

// New creates a new coinmarketcap Indexer.
func New(logger *zap.Logger, apiKey string) *Indexer {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Indexer{
		logger:   logger.With(zap.String("indexer", Name)),
		client:   NewHTTPClient(apiKey),
		quotes:   make(map[int64]QuoteData),
		cmcIDMap: make(map[int]CryptoIDMapData),
	}
}

// NewWithClient creates a new coinmarketcap Indexer.
func NewWithClient(logger *zap.Logger, client Client) *Indexer {
	if logger == nil {
		panic("cannot set nil logger")
	}

	return &Indexer{
		logger:   logger.With(zap.String("ingester", Name)),
		client:   client,
		quotes:   make(map[int64]QuoteData),
		cmcIDMap: make(map[int]CryptoIDMapData),
	}
}

func (i *Indexer) GetClient() Client {
	return i.client
}

type WrappedCryptoIDMapData struct {
	IDMap CryptoIDMapData
	Info  InfoData
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
	infoDataMap := make(InfoDataMap, len(resp.Data))
	splitIDs := slices.Chunk(ids, reqSize)
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
type FiatIDMap []FiatData

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

func (i *Indexer) CacheQuotes(ctx context.Context, ids []int64) error {
	for _, chunk := range slices.Chunk(ids, 1000) {
		resp, err := i.Quotes(ctx, chunk)
		if err != nil {
			return err
		}

		for id, data := range resp {
			i.quotes[id] = data
		}
	}

	return nil
}

// Quotes fetches the QuoteData for the given CMC IDs and returns them as a map.
// If a desired ID is not returned, we fall back to individually fetch the data for the ID,
// and return an error if that fails.
func (i *Indexer) Quotes(ctx context.Context, ids []int64) (map[int64]QuoteData, error) {
	i.logger.Debug("fetching quote data", zap.Any("cmc ids", ids))

	resp, err := i.client.Quotes(ctx, ids)
	if err != nil {
		i.logger.Error("unable to fetch quote data", zap.Error(err))
		return nil, err
	}

	if err = resp.Status.Validate(); err != nil {
		i.logger.Error("failed to validate response", zap.Error(err))
		return nil, err
	}

	quotes := make(map[int64]QuoteData)
	for _, id := range ids {
		data, ok := resp.Data[fmt.Sprintf("%d", id)]
		if !ok {
			i.logger.Error("desired symbol not found - retrying", zap.Int64("id", id))
			data, err = i.Quote(ctx, id)
			if err != nil {
				i.logger.Error("unable to fetch quote data", zap.Int64("id", id), zap.Error(err))
				return nil, fmt.Errorf("unable to fetch quote data for id %d: %w", id, err)
			}
		}
		quotes[id] = data
	}

	return quotes, nil
}

// Quote returns a quote from the CMC ID and symbol using its underlying client.
func (i *Indexer) Quote(ctx context.Context, id int64) (QuoteData, error) {
	i.logger.Debug("fetching quote data", zap.Int64("cmc id", id))

	if data, ok := i.quotes[id]; ok {
		return data, nil
	}

	resp, err := i.client.Quote(ctx, id)
	if err != nil {
		i.logger.Error("unable to fetch quote data", zap.Error(err))
		return QuoteData{}, err
	}

	if err := resp.Status.Validate(); err != nil {
		i.logger.Error("failed to validate response", zap.Error(err))
		return QuoteData{}, err
	}

	// lookup by string rep of ID
	if data, ok := resp.Data[fmt.Sprintf("%d", id)]; ok {
		return data, nil
	}

	i.logger.Error("desired symbol not found", zap.Error(err), zap.Any("data", resp.Data))
	return QuoteData{}, fmt.Errorf("quote data not found for id: %d, %w", id, err)
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

	for name, CMCIDInfo := range ingesterIDs {
		i.logger.Info("fetching cmc market data", zap.String("exchange", name))
		var exchange_pmps ProviderMarketPairs
		// if CMCIDInfo.IsDex {
		// 	exchange_pmps, err = i.GetDEXProviderMarketsPairs(ctx, name, CMCIDInfo)
		// } else {
		// 	exchange_pmps, err = i.GetCEXProviderMarketsPairs(ctx, name, CMCIDInfo.ID)
		// }
		exchange_pmps, err = i.GetCEXProviderMarketsPairs(ctx, name, CMCIDInfo.ID)
		if err != nil {
			return NewProviderMarketPairs(), err
		}

		pmps = mergeProviderMarketPairs(pmps, exchange_pmps)
	}

	return pmps, nil
}

func mergeProviderMarketPairs(a, b ProviderMarketPairs) ProviderMarketPairs {
	for k, v := range b.Data {
		a.Data[k] = v
	}
	return a
}

func (i *Indexer) GetDEXProviderMarketsPairs(ctx context.Context, name string, cmcExchangeInfo CMCIDInfo) (ProviderMarketPairs, error) {
	pmps := NewProviderMarketPairs()
	markets, err := i.client.DexMarkets(ctx, cmcExchangeInfo)

	// Write intermediate file
	filepath := fmt.Sprintf("tmp/%s.json", name)
	i.logger.Info("writing DEX markets", zap.String("file", filepath))
	file.CreateAndWriteJSONToFile(filepath, markets)

	if err != nil {
		return pmps, err
	}

	i.logger.Info("fetched cmc DEX market data", zap.String("exchange", name), zap.Int("num markets", len(markets.Data)))

	for _, market := range markets.Data {
		var cmcBaseID int
		var cmcQuoteID int

		if market.BaseAssetUCID == "" {
			i.logger.Debug(("no base asset ucid, skipping"), zap.Any("market", market))
			continue
		} else {
			cmcBaseID, err = strconv.Atoi(market.BaseAssetUCID)
			if err != nil {
				i.logger.Error("failed to parse base asset ucid", zap.Any("market", market), zap.Error(err))
				continue
			}
		}
		if market.QuoteAssetUCID == "" {
			i.logger.Debug(("no quote asset ucid, skipping"), zap.Any("market", market))
			continue
		} else {
			cmcQuoteID, err = strconv.Atoi(market.QuoteAssetUCID)
			if err != nil {
				i.logger.Error("failed to parse quote asset ucid", zap.Any("market", market), zap.Error(err))
				continue
			}
		}

		var cmcBaseRank = 0
		var cmcQuoteRank = 0

		if cmcBaseData, ok := i.cmcIDMap[cmcBaseID]; ok {
			cmcBaseRank = cmcBaseData.Rank
		}
		if cmcQuoteData, ok := i.cmcIDMap[cmcQuoteID]; ok {
			cmcQuoteRank = cmcQuoteData.Rank
		}

		key := ProviderMarketPairKey(
			names.GetProviderName(name),
			market.BaseAssetSymbol,
			market.QuoteAssetSymbol,
		)

		idInfo := types.CoinMarketCapInfo{
			BaseID:    int64(cmcBaseID),
			QuoteID:   int64(cmcQuoteID),
			BaseRank:  int64(cmcBaseRank),
			QuoteRank: int64(cmcQuoteRank),
		}

		liquidityInfo := types.LiquidityInfo{
			NegativeDepthTwo: market.Quote[0].Liquidity / 2,
			PositiveDepthTwo: market.Quote[0].Liquidity / 2,
		}

		newMarketData := ProviderMarketData{
			BaseAsset:      market.BaseAssetSymbol,
			QuoteAsset:     market.QuoteAssetSymbol,
			QuoteVolume:    market.Quote[0].Volume24H,
			CMCInfo:        idInfo,
			MetadataJSON:   nil,
			ReferencePrice: market.Quote[0].Price,
			LiquidityInfo:  liquidityInfo,
		}

		if existing, exists := pmps.Data[key]; exists {
			shouldReplace := shouldReplaceMarketPair(existing, newMarketData)
			i.logger.Warn("key already exists in pmps.Data", zap.String("key", key), zap.Any("existing", existing), zap.Any("new", newMarketData))

			if shouldReplace {
				pmps.Data[key] = newMarketData
				i.logger.Info("replacing market pair", zap.String("key", key), zap.Any("existing", existing), zap.Any("new", newMarketData))
			}
			continue
		}

		pmps.Data[key] = newMarketData
	}

	return pmps, nil
}

func (i *Indexer) GetCEXProviderMarketsPairs(ctx context.Context, name string, exchangeID int) (ProviderMarketPairs, error) {
	pmps := NewProviderMarketPairs()
	markets, err := i.client.ExchangeMarkets(ctx, exchangeID)
	if err != nil {
		return pmps, err
	}

	if err := markets.Status.Validate(); err != nil {
		return pmps, err
	}

	i.logger.Info("fetched cmc market data", zap.String("exchange", name), zap.Int("num markets", markets.Data.NumMarketPairs))
	for _, pair := range markets.Data.MarketPairs {
		cmcBaseID := pair.MarketPairBase.CurrencyID
		cmcQuoteID := pair.MarketPairQuote.CurrencyID
		var cmcBaseRank = 0
		var cmcQuoteRank = 0

		if cmcBaseData, ok := i.cmcIDMap[cmcBaseID]; ok {
			cmcBaseRank = cmcBaseData.Rank
		}
		if cmcQuoteData, ok := i.cmcIDMap[cmcQuoteID]; ok {
			cmcQuoteRank = cmcQuoteData.Rank
		}

		key := ProviderMarketPairKey(
			names.GetProviderName(name),
			pair.MarketPairBase.ExchangeSymbol,
			pair.MarketPairQuote.ExchangeSymbol,
		)

		idInfo := types.CoinMarketCapInfo{
			BaseID:    int64(cmcBaseID),
			QuoteID:   int64(cmcQuoteID),
			BaseRank:  int64(cmcBaseRank),
			QuoteRank: int64(cmcQuoteRank),
		}

		liquidityInfo := types.LiquidityInfo{
			NegativeDepthTwo: pair.Quote.USD.DepthNegativeTwo,
			PositiveDepthTwo: pair.Quote.USD.DepthPositiveTwo,
		}

		newMarketData := ProviderMarketData{
			BaseAsset:      pair.MarketPairBase.CurrencySymbol,
			QuoteAsset:     pair.MarketPairQuote.CurrencySymbol,
			QuoteVolume:    pair.Quote.ExchangeReported.Volume24HQuote,
			CMCInfo:        idInfo,
			MetadataJSON:   nil,
			ReferencePrice: pair.Quote.ExchangeReported.Price,
			LiquidityInfo:  liquidityInfo,
		}

		if existing, exists := pmps.Data[key]; exists {
			shouldReplace := shouldReplaceMarketPair(existing, newMarketData)
			i.logger.Warn("key already exists in pmps.Data", zap.String("key", key), zap.Any("existing", existing), zap.Any("new", newMarketData))

			if shouldReplace {
				pmps.Data[key] = newMarketData
				i.logger.Info("replacing market pair", zap.String("key", key), zap.Any("existing", existing), zap.Any("new", newMarketData))
			}
			continue
		}

		pmps.Data[key] = newMarketData
	}
	return pmps, nil
}

// shouldReplaceMarketPair determines if an existing market pair should be replaced with a new one
// based on their base asset rank values. Returns true if:
// 1. Existing base asset rank is 0 and new base asset rank is non-zero
// 2. Both ranks are non-zero but new base asset rank is lower
func shouldReplaceMarketPair(existing, new ProviderMarketData) bool {
	if existing.CMCInfo.BaseRank == 0 && new.CMCInfo.BaseRank > 0 {
		return true
	}
	if existing.CMCInfo.BaseRank > 0 && new.CMCInfo.BaseRank > 0 &&
		new.CMCInfo.BaseRank < existing.CMCInfo.BaseRank {
		return true
	}
	return false
}

type CMCIDInfo struct {
	IsDex        bool   `json:"is_dex"`
	ID           int    `json:"id"`
	NetworkSlug  string `json:"network_slug,omitempty"`
	MinVolume    int    `json:"min_volume"`
	MinLiquidity int    `json:"min_liquidity"`
}
type IngesterIDMap map[string]CMCIDInfo

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
			info := CMCIDInfo{
				ID:    id,
				IsDex: false,
			}

			// Check if this is a DEX
			switch cmcName {
			case "raydium":
				info.IsDex = true
				info.NetworkSlug = "solana"
				info.MinVolume = cfg.DexMinVolume
				info.MinLiquidity = cfg.DexMinLiquidity
			case "uniswap-v3":
				info.IsDex = true
				info.NetworkSlug = "ethereum"
				info.MinVolume = cfg.DexMinVolume
				info.MinLiquidity = cfg.DexMinLiquidity
			case "uniswap-v3-base":
				info.IsDex = true
				info.NetworkSlug = "base"
				info.MinVolume = cfg.DexMinVolume
				info.MinLiquidity = cfg.DexMinLiquidity
			}

			ingesterNameToID[ingesterName] = info
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

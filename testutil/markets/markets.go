package markets

import (
	connecttypes "github.com/dydxprotocol/slinky/pkg/types"
	mmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
)

var UsdtUsd = mmtypes.Market{
	Ticker: mmtypes.Ticker{
		CurrencyPair: connecttypes.CurrencyPair{
			Base:  "USDT",
			Quote: "USD",
		},
		Decimals:         8,
		MinProviderCount: 1,
		Enabled:          true,
	},
	ProviderConfigs: []mmtypes.ProviderConfig{
		{
			Name:           "okx_ws",
			OffChainTicker: "USDC-USDT",
			Invert:         true,
		},
	},
}

package config

import (
	"fmt"

	connecttypes "github.com/dydxprotocol/slinky/pkg/types"
)

type ValidateConfig struct {
	// Markets with known price volatility that will allowed double the configured reference price allowance.
	FlexibleRefPriceMarkets []string `json:"flexible_ref_price_markets" mapstructure:"flexible_ref_price_markets"`
}

func DefaultValidateConfig() ValidateConfig {
	return ValidateConfig{
		FlexibleRefPriceMarkets: []string{},
	}
}

func (c *ValidateConfig) Validate() error {
	for _, market := range c.FlexibleRefPriceMarkets {
		if _, err := connecttypes.CurrencyPairFromString(market); err != nil {
			return fmt.Errorf("invalid market format %q in FlexibleRefPriceMarkets: %w", market, err)
		}
	}
	return nil
}

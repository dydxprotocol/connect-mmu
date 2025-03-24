package validator

type Option func(*Validator)

func WithCMCKey(key string) Option {
	return func(v *Validator) {
		v.cmcAPIKey = key
	}
}

func WithFlexibleRefPriceMarkets(markets []string) Option {
	return func(v *Validator) {
		v.flexibleRefPriceMarkets = markets
	}
}

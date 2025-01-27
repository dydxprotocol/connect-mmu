package basic

// Inputs
const (
	// all basic commands
	ConfigPathFlag        = "config"
	ConfigPathDefault     = "./local/config-dydx-testnet.json"
	ConfigPathDescription = "path to market map updater configuration"

	// generate
	ProviderDataPathFlag        = "provider-data"
	ProviderDataPathDefault     = "./tmp/indexed-provider-data.json"
	ProviderDataPathDescription = "path to indexed markets and providers"

	// override
	MarketMapGeneratedFlag        = "market-map"
	MarketMapGeneratedDefault     = "./tmp/generated-market-map.json"
	MarketMapGeneratedDescription = "path to market map"

	UpdateEnabledFlag        = "update-enabled"
	UpdateEnabledDefault     = false
	UpdateEnabledDescription = "should update providers on enabled markets"

	OverwriteProvidersFlag        = "overwrite-providers"
	OverwriteProvidersDefault     = false
	OverwriteProvidersDescription = "should overwrite existing providers instead of only appending new providers"

	ExistingOnlyFlag        = "existing-only"
	ExistingOnlyDefault     = false
	ExistingOnlyDescription = "should update only markets that exist in the current market map"

	DisableDeFiMarketMerging            = "disable-defi-market-merging"
	DisableDeFiMarketMergingDefault     = false
	DisableDeFiMarketMergingDescription = "disables the merging of DeFi markets into markets that have the same CMC ID"

	// upserts
	MarketMapOverrideFlag        = "market-map"
	MarketMapOverrideDefault     = "./tmp/override-market-map.json"
	MarketMapOverrideDescription = "path to market map"

	WarnOnInvalidMarketMapFlag        = "warn-on-invalid-market-map"
	WarnOnInvalidMarketMapDefault     = false
	WarnOnInvalidMarketMapDescription = "warn then the on-chain market map is invalid instead of failing"

	// dispatch
	UpdatesPathFlag        = "updates"
	UpdatesPathDefault     = "./tmp/market-map-updates.json"
	UpdatesPathDescription = "path to list of markets to be updated"

	AdditionsPathFlag        = "additions"
	AdditionsPathDefault     = "./tmp/market-map-additions.json"
	AdditionsPathDescription = "path to list of markets to be added to the market map"

	RemovalsPathFlag        = "removals"
	RemovalsPathDefault     = "./tmp/market-map-removals.json"
	RemovalsPathDescription = "path to list of markets to be removed from the marketmap"

	SimulateFlag        = "simulate"
	SimulateDefault     = false
	SimulateDescription = "simulate transaction without submitting"

	SimulateAddressFlag        = "simulate-address"
	SimulateAddressDefault     = ""
	SimulateAddressDescription = "bech32 encoded address to simulate transaction without submitting"
)

// Outputs
const (
	// applies to all steps
	ArchiveIntermediateStepsFlag        = "archive-intermediate-steps"
	ArchiveIntermediateStepsDefault     = false
	ArchiveIntermediateStepsDescription = "should archive intermediate steps (e.g. write CMC data to files)"

	// index
	ProviderDataOutPathFlag        = "provider-data-out"
	ProviderDataOutPathDefault     = ProviderDataPathDefault
	ProviderDataOutPathDescription = "path to output indexed markets and providers"

	// generate
	MarketMapOutPathGeneratedFlag         = "generated-market-map-out"
	MarketMapOutPathGeneratedDefault      = MarketMapGeneratedDefault
	MarketMapOutPathGenderatedDescription = "path to output generated market map"

	MarketMapExclusionsOutPathFlag        = "generated-market-map-exclusions-out"
	MarketMapExclusionsOutPathDefault     = "./tmp/generated-market-map-exclusions.json"
	MarketMapExclusionsOutPathDescription = "path to output markets excluded from market map"

	// override
	MarketMapOutPathOverrideFlag        = "override-market-map-out"
	MarketMapOutPathOverrideDefault     = MarketMapOverrideDefault
	MarketMapOutPathOverrideDescription = "path to output override market map"

	MarketMapRemovalsOutPathFlag        = "override-market-map-removals-out"
	MarketMapRemovalsOutPathDefault     = "./tmp/market-map-removals.json"
	MarketMapRemovalsOutPathDescription = "path to output markets to remove from market map"

	// upserts
	UpdatesOutPathFlag        = "updates-out"
	UpdatesOutPathDefault     = "./tmp/market-map-updates.json"
	UpdatesOutPathDescription = "path to output markets to be updated"

	AdditionsOutPathFlag        = "additions-out"
	AdditionsOutPathDefault     = "./tmp/market-map-additions.json"
	AdditionsOutPathDescription = "path to output markets to be added"
)

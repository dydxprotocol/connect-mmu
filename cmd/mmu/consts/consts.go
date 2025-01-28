package consts

import (
	"fmt"
	"os"
	"slices"
)

// Filename of the file to write to S3 containing the latest MM transactions
const LatestTransactionsFilename = "latest-transactions.json"

// URL to fetch the latest transactions output by the Staging MMU.
// Only used for internal dev / testing of the MMU itself.
const StagingAPIURL = "https://ievd6jluve.execute-api.ap-northeast-1.amazonaws.com/staging/market-map-updater/v1/tx"

// URL to fetch the latest transactions output by the Prod MMU.
// Used by external MM Operator to fetch transactions for updating Testnet and Mainnet MMs.
const ProdAPIURL = "https://fdviqy4mbk.execute-api.ap-northeast-1.amazonaws.com/mainnet/market-map-updater/v1/tx"

// Path of the oracle config file used by the validate command
const OracleConfigFilePath = "local/fixtures/e2e/oracle.json"

// Get list of valid envs for the MMU pipeline
func GetSupportedEnvironments() []string {
	return []string{"staging", "mainnet"}
}

// Get list of dYdX networks / chains supported by the MMU pipeline
func GetSupportedNetworks() []string {
	return []string{"testnet", "mainnet"}
}

func GetOracleAPIKeySecretNames() (map[string]string, error) {
	env := os.Getenv("ENVIRONMENT")
	supportedEnvs := GetSupportedEnvironments()
	if !slices.Contains(supportedEnvs, env) {
		return nil, fmt.Errorf("invalid env: %s", env)
	}

	namePrefix := fmt.Sprintf("%s-market-map-updater", env)
	nameSuffix := "api-key"
	return map[string]string{
		"https://solana.polkachu.com":          fmt.Sprintf("%s-raydium-solana-polkachu-%s", namePrefix, nameSuffix),
		"https://connect-solana.kingnodes.com": fmt.Sprintf("%s-raydium-solana-kingnodes-%s", namePrefix, nameSuffix),
		"https://solana.lavenderfive.com":      fmt.Sprintf("%s-raydium-solana-lavenderfive-%s", namePrefix, nameSuffix),
		"https://solana-rpc.rhino-apis.com":    fmt.Sprintf("%s-raydium-solana-rhino-%s", namePrefix, nameSuffix),
		"https://ethereum-rpc.polkachu.com":    fmt.Sprintf("%s-uniswap-eth-polkachu-%s", namePrefix, nameSuffix),
		"https://connect-eth.kingnodes.com":    fmt.Sprintf("%s-uniswap-eth-kingnodes-%s", namePrefix, nameSuffix),
		"https://ethereum.lavenderfive.com":    fmt.Sprintf("%s-uniswap-eth-lavenderfive-%s", namePrefix, nameSuffix),
		"https://ethereum-rpc.rhino-apis.com":  fmt.Sprintf("%s-uniswap-eth-rhino-%s", namePrefix, nameSuffix),
		"https://connect-base.kingnodes.com":   fmt.Sprintf("%s-uniswap-base-kingnodes-%s", namePrefix, nameSuffix),
		"https://base.lavenderfive.com":        fmt.Sprintf("%s-uniswap-base-lavenderfive-%s", namePrefix, nameSuffix),
		"https://base-rpc.rhino-apis.com":      fmt.Sprintf("%s-uniswap-base-rhino-%s", namePrefix, nameSuffix),
	}, nil
}

package consts

// Filename of the file to write to S3 containing the latest MM transactions
const LatestTransactionsFilename = "latest-transactions.json"

// URL to fetch the latest transactions output by the Staging MMU.
// Only used for internal dev / testing of the MMU itself.
const StagingAPIURL = "https://ievd6jluve.execute-api.ap-northeast-1.amazonaws.com/staging/market-map-updater/v1/tx"

// URL to fetch the latest transactions output by the Prod MMU.
// Used by external MM Operator to fetch transactions for updating Testnet and Mainnet MMs.
const ProdAPIURL = "https://fdviqy4mbk.execute-api.ap-northeast-1.amazonaws.com/mainnet/market-map-updater/v1/tx"

// Get list of valid envs for the MMU pipeline
func GetSupportedEnvironments() []string {
	return []string{"staging", "mainnet"}
}

// Get list of dYdX networks / chains supported by the MMU pipeline
func GetSupportedNetworks() []string {
	return []string{"testnet", "mainnet"}
}

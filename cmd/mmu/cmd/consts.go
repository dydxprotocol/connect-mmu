package cmd

// Get list of dYdX networks / chains supported by the MMU pipeline
func GetSupportedNetworks() []string {
	return []string{"testnet", "mainnet"}
}

// Get list of valid envs for the MMU pipeline
func GetSupportedEnvironments() []string {
	return []string{"staging", "mainnet"}
}

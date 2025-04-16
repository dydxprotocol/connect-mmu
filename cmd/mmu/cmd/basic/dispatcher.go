package basic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	auth "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	slinkymmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/cmd/mmu/consts"
	"github.com/skip-mev/connect-mmu/cmd/mmu/logging"
	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/dispatcher"
	"github.com/skip-mev/connect-mmu/dispatcher/transaction/generator"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/file"
	"github.com/skip-mev/connect-mmu/lib/slack"
	"github.com/skip-mev/connect-mmu/signing"
	"github.com/skip-mev/connect-mmu/signing/simulate"
)

// DispatchCmd returns a command to DispatchCmd market updates, additions, and removals.
func DispatchCmd(signingRegistry *signing.Registry) *cobra.Command {
	var flags dispatchCmdFlags

	cmd := &cobra.Command{
		Use:     "dispatch",
		Short:   "dispatch updates, additions and/or removals to a chain",
		Example: "dispatch --config path/to/config.json --updates path/to/updates.json --simulate",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := logging.Logger(cmd.Context())

			// Validate that at least one of updates, additions, or removals is set
			hasUpdates := flags.updatesPath != ""
			hasAdditions := flags.additionsPath != ""
			hasRemovals := flags.removalsPath != ""
			if !hasUpdates && !hasAdditions && !hasRemovals {
				return fmt.Errorf("must specify at least one of --updates, --additions, or --removals")
			}

			cfg, err := config.ReadConfig(flags.configPath)
			if err != nil {
				logger.Error("failed to load config", zap.Error(err))
				return err
			}

			if cfg.Dispatch == nil {
				return errors.New("dispatch configuration missing from mmu config")
			}

			if cfg.Chain == nil {
				return errors.New("chain configuration missing from mmu config")
			}

			signerConfig := cfg.Dispatch.SigningConfig
			if flags.simulateAddress != "" {
				signerConfig = config.SigningConfig{
					Type:   simulate.TypeName,
					Config: simulate.SigningAgentConfig{Address: flags.simulateAddress},
				}
			}

			signer, err := signingRegistry.CreateSigner(signerConfig, *cfg.Chain)
			if err != nil {
				return fmt.Errorf("failed to create signer: %w", err)
			}

			dp, err := dispatcher.New(*cfg.Dispatch, *cfg.Chain, signer, logger)
			if err != nil {
				return fmt.Errorf("failed to create dispatcher: %w", err)
			}

			signerAddress, err := getSignerAddress(cmd.Context(), signer)
			if err != nil {
				return fmt.Errorf("failed to get signer address: %w", err)
			}

			var txs []cmttypes.Tx
			if hasUpdates {
				updateTxs, err := generateUpdateTransactions(cmd.Context(), logger, dp, &cfg, signerAddress, flags.updatesPath)
				if err != nil {
					return err
				}
				if updateTxs != nil {
					txs = append(txs, updateTxs...)
				}
			}
			if hasAdditions {
				additionTxs, err := generateAdditionTransactions(cmd.Context(), logger, dp, &cfg, signerAddress, flags.additionsPath)
				if err != nil {
					return err
				}
				if additionTxs != nil {
					txs = append(txs, additionTxs...)
				}
			}
			if hasRemovals {
				removalTxs, err := generateRemovalTransactions(cmd.Context(), logger, dp, &cfg, signerAddress, flags.removalsPath)
				if err != nil {
					return err
				}
				if removalTxs != nil {
					txs = append(txs, removalTxs...)
				}
			}

			decodedTxs, err := decodeTxs(txs)
			if err != nil {
				return err
			}

			err = file.WriteJSONToFile("transactions.json", decodedTxs)
			if err != nil {
				return err
			}

			if aws.IsLambda() {
				err = writeLatestTransactions(decodedTxs)
				if err != nil {
					return err
				}
				return notifySlack()
			}

			return nil
		},
	}

	dispatchCmdConfigureFlags(cmd, &flags)

	return cmd
}

func generateUpdateTransactions(
	ctx context.Context,
	logger *zap.Logger,
	dp *dispatcher.Dispatcher,
	cfg *config.Config,
	signerAddress string,
	updatesPath string,
) ([]cmttypes.Tx, error) {
	updates, err := file.ReadJSONFromFile[[]mmtypes.Market](updatesPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read updates file: %w", err)
	}

	if len(updates) == 0 {
		return nil, nil
	}

	updateMsgs, err := generator.ConvertUpdatesToMessages(
		logger,
		cfg.Dispatch.TxConfig,
		cfg.Chain.Version,
		signerAddress,
		updates,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert updates to messages: %w", err)
	}

	txs, err := dp.GenerateTransactions(ctx, updateMsgs)
	if err != nil {
		return nil, err
	}

	return txs, nil
}

func generateAdditionTransactions(
	ctx context.Context,
	logger *zap.Logger,
	dp *dispatcher.Dispatcher,
	cfg *config.Config,
	signerAddress string,
	additionsPath string,
) ([]cmttypes.Tx, error) {
	additions, err := file.ReadJSONFromFile[[]mmtypes.Market](additionsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read additions file: %w", err)
	}

	if len(additions) == 0 {
		return nil, nil
	}

	additionMsgs, err := generator.ConvertAdditionsToMessages(
		logger,
		cfg.Dispatch.TxConfig,
		cfg.Chain.Version,
		signerAddress,
		additions,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert additions to messages: %w", err)
	}

	txs, err := dp.GenerateTransactions(ctx, additionMsgs)
	if err != nil {
		return nil, err
	}

	return txs, nil
}

func generateRemovalTransactions(
	ctx context.Context,
	logger *zap.Logger,
	dp *dispatcher.Dispatcher,
	cfg *config.Config,
	signerAddress string,
	removalsPath string,
) ([]cmttypes.Tx, error) {
	removals, err := file.ReadJSONFromFile[[]string](removalsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read marketmap removals file: %w", err)
	}

	if len(removals) == 0 {
		return nil, nil
	}

	removalMsgs, err := generator.ConvertRemovalsToMessages(
		logger,
		cfg.Chain.Version,
		signerAddress,
		removals,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert removals to messages: %w", err)
	}

	txs, err := dp.GenerateTransactions(ctx, removalMsgs)
	if err != nil {
		return nil, err
	}

	return txs, nil
}

type DecodedTx struct {
	Body       *codectypes.Any `json:"body"`
	AuthInfo   *codectypes.Any `json:"auth_info"`
	Signatures [][]byte        `json:"signatures"`
}

func decodeTxs(txs []cmttypes.Tx) ([]DecodedTx, error) {
	registry := codectypes.NewInterfaceRegistry()
	cdc := codec.NewProtoCodec(registry)
	decoder := auth.DefaultTxDecoder(cdc)
	jsonEncoder := auth.DefaultJSONTxEncoder(cdc)
	slinkymmtypes.RegisterInterfaces(registry)

	decodedTxs := make([]DecodedTx, len(txs))

	for i, tx := range txs {
		decodedTx, err := decoder(tx)
		if err != nil {
			return nil, err
		}

		jsonBz, err := jsonEncoder(decodedTx)
		if err != nil {
			return nil, err
		}

		var decodedJSON DecodedTx

		if err := json.Unmarshal(jsonBz, &decodedJSON); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tx json: %w", err)
		}

		decodedTxs[i] = decodedJSON
	}

	return decodedTxs, nil
}

func getSignerAddress(ctx context.Context, signer signing.SigningAgent) (string, error) {
	acc, err := signer.GetSigningAccount(ctx)
	if err != nil {
		return "", err
	}

	// convert to a base account
	baseAcc, ok := acc.(*authtypes.BaseAccount)
	if !ok {
		return "", fmt.Errorf("expected BaseAccount but got %T", acc)
	}
	return baseAcc.Address, nil
}

// Write latest-transactions.json to S3 for the MM Operator
func writeLatestTransactions(decodedTxs []DecodedTx) error {
	// Check if new transactions differ from the existing "latest-transactions.json" in S3
	newLatestTransactionsJSON, err := json.MarshalIndent(decodedTxs, "", "  ")
	if err != nil {
		return err
	}
	existingLatestTransactionsJSON, err := aws.ReadFromS3(consts.LatestTransactionsFilename, false)
	if err == nil && bytes.Equal(newLatestTransactionsJSON, existingLatestTransactionsJSON) {
		return nil
	}

	// If we have new transactions, write them to "latest-transactions.json"
	err = aws.WriteToS3(consts.LatestTransactionsFilename, newLatestTransactionsJSON, false)
	if err != nil {
		return err
	}

	return nil
}

// Send a Slack message with links to latest transaction + pipeline data
func notifySlack() error {
	// Get current env of the MMU itself
	mmuEnv := os.Getenv("ENVIRONMENT")

	// Get target network of the current MMU run
	network := os.Getenv("NETWORK")

	// Construct full API URL to fetch the latest transactions for the target network,
	// and construct name of the secret in Secrets Manager that contains the correct Slack Webhook URL for this env + network
	var apiURLBase string
	var slackWebhookURLSecretNameModifier string
	if mmuEnv == "staging" {
		apiURLBase = consts.StagingAPIURL
		slackWebhookURLSecretNameModifier = "internal"
	} else if mmuEnv == "mainnet" {
		apiURLBase = consts.ProdAPIURL
		slackWebhookURLSecretNameModifier = "external"
	}
	slackWebhookURLSecretName := fmt.Sprintf("%s-market-map-updater-%s-slack-webhook-url", mmuEnv, slackWebhookURLSecretNameModifier)

	slackMsg := fmt.Sprintf("New Market Map TX available for `%s`:", strings.ToUpper(network))
	slackMsg += fmt.Sprintf("\n- Transactions: %s", constructSlackTextLink(apiURLBase, "tx", network, "TXs"))
	slackMsg += fmt.Sprintf("\n- Markets: %s, %s, %s", constructSlackTextLink(apiURLBase, "new-markets", network, "New"), constructSlackTextLink(apiURLBase, "removed-markets", network, "Removed"), constructSlackTextLink(apiURLBase, "updated-markets", network, "Updated"))
	slackMsg += fmt.Sprintf("\n- Validation: %s, %s", constructSlackTextLink(apiURLBase, "validation-errors", network, "Errors"), constructSlackTextLink(apiURLBase, "health-reports", network, "Reports"))

	// Send notif to Slack
	return slack.SendNotification(slackMsg, slackWebhookURLSecretName)
}

func constructSlackTextLink(apiURLBase string, apiEndpoint string, network string, linkText string) string {
	apiURLFull := fmt.Sprintf("%s/%s?network=%s", apiURLBase, apiEndpoint, network)
	return fmt.Sprintf("<%s|%s>", apiURLFull, linkText)
}

type dispatchCmdFlags struct {
	configPath      string
	updatesPath     string
	additionsPath   string
	removalsPath    string
	simulate        bool
	simulateAddress string
}

func dispatchCmdConfigureFlags(cmd *cobra.Command, flags *dispatchCmdFlags) {
	cmd.Flags().StringVar(&flags.configPath, ConfigPathFlag, ConfigPathDefault, ConfigPathDescription)
	cmd.Flags().StringVar(&flags.updatesPath, UpdatesPathFlag, "", UpdatesPathDescription)
	cmd.Flags().StringVar(&flags.additionsPath, AdditionsPathFlag, "", AdditionsPathDescription)
	cmd.Flags().StringVar(&flags.removalsPath, RemovalsPathFlag, "", RemovalsPathDescription)
	cmd.Flags().BoolVar(&flags.simulate, SimulateFlag, SimulateDefault, SimulateDescription)
	cmd.Flags().StringVar(&flags.simulateAddress, SimulateAddressFlag, SimulateAddressDefault, SimulateAddressDescription)
}

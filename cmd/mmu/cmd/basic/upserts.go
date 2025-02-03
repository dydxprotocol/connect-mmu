package basic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"

	"github.com/skip-mev/connect-mmu/client/marketmap"
	"github.com/skip-mev/connect-mmu/cmd/mmu/consts"
	"github.com/skip-mev/connect-mmu/cmd/mmu/logging"
	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/file"
	"github.com/skip-mev/connect-mmu/upsert"
)

func UpsertsCmd() *cobra.Command {
	var flags upsertsCmdFlags

	cmd := &cobra.Command{
		Use:     "upserts",
		Short:   "generate upserts from a marketmap",
		Example: "mmu upserts --config config.json --market-map market-map.json --upserts-out upserts.json --warn-on-invalid-market-map false",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := logging.Logger(cmd.Context())

			generatedMM, err := file.ReadJSONFromFile[mmtypes.MarketMap](flags.marketMapPath)
			if err != nil {
				return fmt.Errorf("failed to read generated marketmap: %w", err)
			}

			logger.Info("successfully read generated marketmap", zap.Int("markets", len(generatedMM.Markets)))

			cfg, err := config.ReadConfig(flags.configPath)
			if err != nil {
				return fmt.Errorf("failed to read upsert config at %s: %w", flags.configPath, err)
			}

			if cfg.Upsert == nil {
				return errors.New("upsert configuration missing from mmu config")
			}

			if cfg.Chain == nil {
				return errors.New("chain configuration missing from mmu config")
			}

			updates, additions, err := UpsertsFromConfigs(
				cmd.Context(),
				logger,
				generatedMM,
				*cfg.Chain,
				*cfg.Upsert,
				flags.warnOnInvalidMarketMap,
			)
			if err != nil {
				return fmt.Errorf("failed to read upsert config at %s: %w", flags.configPath, err)
			}

			// TODO write to Latest also (where do removals get written?)
			// Should you add a func in file module to write latest (or a new param, writeLatest)?
			err = file.WriteJSONToFile(flags.updatesOutPath, updates)
			if err != nil {
				return fmt.Errorf("failed to write updates: %w", err)
			}
			logger.Info("updates written to file", zap.String("file", flags.updatesOutPath))

			err = file.WriteJSONToFile(flags.additionsOutPath, additions)
			if err != nil {
				return fmt.Errorf("failed to write additions: %w", err)
			}
			logger.Info("additions written to file", zap.String("file", flags.additionsOutPath))

			if aws.IsLambda() {
				outputs := map[string]any{
					consts.LatestUpdatedMarketsFilename: updates,
					consts.LatestNewMarketsFilename:     additions,
				}
				for filename, data := range outputs {
					latestJSON, err := json.MarshalIndent(data, "", "  ")
					if err != nil {
						return err
					}
					err = aws.WriteToS3(filename, latestJSON, false)
					if err != nil {
						return err
					}
				}
			}

			return nil
		},
	}

	upsertsCmdConfigureFlags(cmd, &flags)

	return cmd
}

type upsertsCmdFlags struct {
	configPath             string
	marketMapPath          string
	updatesOutPath         string
	additionsOutPath       string
	warnOnInvalidMarketMap bool
}

func upsertsCmdConfigureFlags(cmd *cobra.Command, flags *upsertsCmdFlags) {
	cmd.Flags().StringVar(&flags.configPath, ConfigPathFlag, ConfigPathDefault, ConfigPathDescription)
	cmd.Flags().StringVar(&flags.marketMapPath, MarketMapOverrideFlag, MarketMapOverrideDefault, MarketMapOverrideDescription)
	cmd.Flags().BoolVar(&flags.warnOnInvalidMarketMap, WarnOnInvalidMarketMapFlag, WarnOnInvalidMarketMapDefault, WarnOnInvalidMarketMapDescription)

	cmd.Flags().StringVar(&flags.updatesOutPath, UpdatesOutPathFlag, UpdatesOutPathDefault, UpdatesOutPathDescription)
	cmd.Flags().StringVar(&flags.additionsOutPath, AdditionsOutPathFlag, AdditionsOutPathDefault, AdditionsOutPathDescription)
}

func UpsertsFromConfigs(
	ctx context.Context,
	logger *zap.Logger,
	generatedMarketMap mmtypes.MarketMap,
	chainCfg config.ChainConfig,
	cfg config.UpsertConfig,
	warnOnInvalidMarketMap bool,
) (updates []mmtypes.Market, additions []mmtypes.Market, err error) {
	mmClient, err := marketmap.NewClientFromChainConfig(logger, chainCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create MarketMap client from chain config: %w", err)
	}

	if err := generatedMarketMap.ValidateBasic(); err != nil {
		if warnOnInvalidMarketMap {
			logger.Warn("failed validate generated marketmap - will use a valid subset", zap.Error(err))
		} else {
			return nil, nil, fmt.Errorf("failed to validate generated marketmap: %w", err)
		}
	}

	onChainMarketMap, err := mmClient.GetMarketMap(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get marketmap: %w", err)
	}

	if err := onChainMarketMap.ValidateBasic(); err != nil {
		if warnOnInvalidMarketMap {
			logger.Warn("failed validate on chain marketmap - will use a valid subset", zap.Error(err))
		} else {
			return nil, nil, fmt.Errorf("failed to validate on-chain marketmap: %w", err)
		}
	}

	logger.Info("successfully retrieved current market map", zap.Int("markets", len(onChainMarketMap.Markets)))

	gen, err := upsert.New(logger, cfg, generatedMarketMap, onChainMarketMap)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create upsert generator: %w", err)
	}
	updates, additions, err = gen.GenerateUpserts()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create upserts: %w", err)
	}

	return updates, additions, nil
}

package generator

import (
	"fmt"

	sdk "github.com/cosmos/cosmos-sdk/types"
	slinkymmtypes "github.com/dydxprotocol/slinky/x/marketmap/types"
	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/client/marketmap"
	"github.com/skip-mev/connect-mmu/config"
)

// ConvertUpdatesToMessages converts a set of update markets to a slice of sdk.Messages respecting the configured
// max size of a transaction.
func ConvertUpdatesToMessages(
	logger *zap.Logger,
	cfg config.TransactionConfig,
	version config.Version,
	authorityAddress string,
	updates []mmtypes.Market,
) ([]sdk.Msg, error) {
	msgs := make([]sdk.Msg, 0)

	// create the update txs, such that the size of all markets per tx is optimized, while
	// not exceeding the max tx size
	currentTxSize := 0
	start := 0
	for i, market := range updates {
		// fail if the market is invalid
		if err := market.ValidateBasic(); err != nil {
			logger.Error("invalid market", zap.Error(err))
			return nil, fmt.Errorf("invalid market: %w", err)
		}

		// validity check for market
		if market.Size() > cfg.MaxBytesPerTx {
			// if the market size exceeds the max tx size, then we can't create a tx for it (fail)
			logger.Error("market size exceeds max tx size", zap.Any("market", market), zap.Int("size",
				market.Size()), zap.Int("max_size", cfg.MaxBytesPerTx))
			return nil, fmt.Errorf("market size exceeds max tx size: %d > %d", market.Size(), cfg.MaxBytesPerTx)
		}

		// update the currentTxSize
		if currentTxSize+market.Size() > cfg.MaxBytesPerTx {
			// create the tx
			txMarkets := updates[start:i]
			logger.Info("creating update msg", zap.Int("markets", len(txMarkets)))

			var msg sdk.Msg
			switch version {
			case config.VersionSlinky:
				msg = &slinkymmtypes.MsgUpdateMarkets{
					Authority:     authorityAddress,
					UpdateMarkets: marketmap.ConnectToSlinkyMarkets(updates),
				}
			case config.VersionConnect:
				msg = &mmtypes.MsgUpdateMarkets{
					Authority:     authorityAddress,
					UpdateMarkets: updates,
				}
			default:
				return nil, fmt.Errorf("unsupported version %s", version)
			}

			msgs = append(msgs, msg)

			// reset the currentTxSize
			currentTxSize = 0
			start = i
		}

		// add to the current group
		currentTxSize += market.Size()
	}

	// create the last tx
	if currentTxSize > 0 {
		var msg sdk.Msg
		switch version {
		case config.VersionSlinky:
			msg = &slinkymmtypes.MsgUpdateMarkets{
				Authority:     authorityAddress,
				UpdateMarkets: marketmap.ConnectToSlinkyMarkets(updates[start:]),
			}
		case config.VersionConnect:
			msg = &mmtypes.MsgUpdateMarkets{
				Authority:     authorityAddress,
				UpdateMarkets: updates[start:],
			}
		default:
			return nil, fmt.Errorf("unsupported version %s", version)
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// ConvertAdditionsToMessages converts a set of new markets to a slice of sdk.Messages respecting the configured
// max size of a transaction.
func ConvertAdditionsToMessages(
	logger *zap.Logger,
	cfg config.TransactionConfig,
	version config.Version,
	authorityAddress string,
	additions []mmtypes.Market,
) ([]sdk.Msg, error) {
	msgs := make([]sdk.Msg, 0)

	// create the creation txs, such that the size of all markets per tx is optimized, while
	// not exceeding the max tx size
	currentTxSize := 0
	start := 0
	for i, market := range additions {
		// fail if the market is invalid
		if err := market.ValidateBasic(); err != nil {
			logger.Error("invalid market", zap.Error(err))
			return nil, fmt.Errorf("invalid market: %w", err)
		}

		// validity check for market
		if market.Size() > cfg.MaxBytesPerTx {
			// if the market size exceeds the max tx size, then we can't create a tx for it (fail)
			logger.Error("market size exceeds max tx size", zap.Any("market", market), zap.Int("size",
				market.Size()), zap.Int("max_size", cfg.MaxBytesPerTx))
			return nil, fmt.Errorf("market size exceeds max tx size: %d > %d", market.Size(), cfg.MaxBytesPerTx)
		}

		// update the currentTxSize
		if currentTxSize+market.Size() > cfg.MaxBytesPerTx {
			// create the tx
			txMarkets := additions[start:i]
			logger.Info("creating update msg", zap.Int("markets", len(txMarkets)))

			var msg sdk.Msg
			switch version {
			case config.VersionSlinky:
				msg = &slinkymmtypes.MsgCreateMarkets{
					Authority:     authorityAddress,
					CreateMarkets: marketmap.ConnectToSlinkyMarkets(txMarkets),
				}
			case config.VersionConnect:
				msg = &mmtypes.MsgCreateMarkets{
					Authority:     authorityAddress,
					CreateMarkets: txMarkets,
				}
			default:
				return nil, fmt.Errorf("unsupported version %s", version)
			}

			msgs = append(msgs, msg)

			// reset the currentTxSize
			currentTxSize = 0
			start = i
		}

		// add to the current group
		currentTxSize += market.Size()
	}

	// create the last tx
	if currentTxSize > 0 {
		var msg sdk.Msg
		switch version {
		case config.VersionSlinky:
			msg = &slinkymmtypes.MsgCreateMarkets{
				Authority:     authorityAddress,
				CreateMarkets: marketmap.ConnectToSlinkyMarkets(additions[start:]),
			}
		case config.VersionConnect:
			msg = &mmtypes.MsgCreateMarkets{
				Authority:     authorityAddress,
				CreateMarkets: additions[start:],
			}
		default:
			return nil, fmt.Errorf("unsupported version %s", version)
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// ConvertRemovalsToMessage converts a set of market tickers to remove to a slice of sdk.Message.
func ConvertRemovalsToMessages(
	logger *zap.Logger,
	version config.Version,
	authorityAddress string,
	removals []string,
) ([]sdk.Msg, error) {
	var msg sdk.Msg
	switch version {
	case config.VersionSlinky:
		msg = &slinkymmtypes.MsgRemoveMarkets{
			Authority: authorityAddress,
			Markets:   removals,
		}
	case config.VersionConnect:
		msg = &mmtypes.MsgRemoveMarkets{
			Authority: authorityAddress,
			Markets:   removals,
		}
	default:
		return nil, fmt.Errorf("unsupported version %s", version)
	}
	logger.Info("created remove msg", zap.Int("num markets to remove", len(removals)))
	return []sdk.Msg{msg}, nil
}

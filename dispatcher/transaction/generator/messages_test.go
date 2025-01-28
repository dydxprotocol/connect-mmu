package generator_test

import (
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/dispatcher/transaction/generator"
	"github.com/skip-mev/connect-mmu/testutil/markets"
)

func TestConvertUpdatesToMessages(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.TransactionConfig
		updates []mmtypes.Market
		want    []sdk.Msg
		wantErr bool
	}{
		{
			name: "empty updates",
			cfg: config.TransactionConfig{
				MaxBytesPerTx: 2000,
			},
			want: make([]sdk.Msg, 0),
		},
		{
			name: "fail due to invalid tx size",
			cfg: config.TransactionConfig{
				MaxBytesPerTx: 0,
			},
			updates: []mmtypes.Market{
				markets.UsdtUsd,
			},
			want:    make([]sdk.Msg, 0),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generator.ConvertUpdatesToMessages(zaptest.NewLogger(t), tt.cfg, config.VersionConnect, "", tt.updates)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, tt.want, got)
		})
	}
}

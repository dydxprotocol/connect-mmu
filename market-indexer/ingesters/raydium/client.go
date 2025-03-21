package raydium

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/config"
	"github.com/skip-mev/connect-mmu/cmd/mmu/consts"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/http"
)

const (
	EndpointPairs = "https://api.raydium.io/v2/main/pairs"
	//nolint:gosec
	EndpointTokenMetadata = "https://token-list-api.solana.cloud/v1/list"
)

var _ Client = &client{}

// Client is a simple client for accessing the Raydium API and Solana nodes.
//
//go:generate mockery --name Client --filename mock_raydium_client.go
type Client interface {
	// Pairs fetches all pairs from the raydium api.
	Pairs(ctx context.Context) (Pairs, error)
	// TokenMetadata gets all token metadata from a solana node.
	TokenMetadata(ctx context.Context) (TokenMetadataResponse, error)
	// GetMultipleAccounts gets multiple accounts from a solana node.
	GetMultipleAccounts(ctx context.Context, accounts []solana.PublicKey) ([]*rpc.Account, error)
	// ValidateClientConfiguration ensures client is configured correctly.
	ValidateClientConfiguration() error
}

type client struct {
	httpClient     *http.Client
	multiRPCClient multiRPC
}

type multiRPC struct {
	logger *zap.Logger

	rpcs []*rpc.Client
}

func newMultiRPC(logger *zap.Logger, cfg config.MarketConfig) multiRPC {
	mRPC := multiRPC{
		logger: logger.Named("multi-rpc"),
		rpcs:   make([]*rpc.Client, len(cfg.RaydiumNodes)),
	}

	if aws.IsLambda() {
		mRPC.logger.Info("instantiating multi-rpc client for raydium with AWS keys")

		apiKeySecretsMap, err := consts.GetOracleAPIKeySecretNames()
		if err != nil {
			mRPC.logger.Error("error getting oracle keys", zap.Error(err))
			return mRPC
		}

		for i, node := range cfg.RaydiumNodes {
			awsKey, ok := apiKeySecretsMap[node.Endpoint]
			if !ok {
				mRPC.logger.Error("oracle config does not contain key for endpoint", zap.String("endpoint", node.Endpoint))
				continue
			}

			secret, err := aws.GetSecret(context.Background(), awsKey)
			if err != nil {
				mRPC.logger.Error("unable to find api-key - skipping raydium node", zap.String("endpoint", node.Endpoint), zap.Error(err))
				continue
			} else {
				mRPC.logger.Info("successfully instantiated raydium node", zap.String("endpoint", node.Endpoint))
			}

			mRPC.rpcs[i] = rpc.NewWithHeaders(node.Endpoint, map[string]string{
				"x-api-key": secret,
			})
		}

		return mRPC
	}

	for i, node := range cfg.RaydiumNodes {
		mRPC.rpcs[i] = rpc.NewWithHeaders(node.Endpoint, map[string]string{
			"x-api-key": node.NodeKey,
		})
	}

	return mRPC
}

func NewClient(logger *zap.Logger, cfg config.MarketConfig) Client {
	return &client{
		httpClient:     http.NewClient(),
		multiRPCClient: newMultiRPC(logger, cfg),
	}
}

func (h *client) Pairs(ctx context.Context) (Pairs, error) {
	resp, err := h.httpClient.GetWithContext(ctx, EndpointPairs)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairs Pairs
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		return nil, err
	}

	return pairs, nil
}

func (h *client) TokenMetadata(ctx context.Context) (TokenMetadataResponse, error) {
	resp, err := h.httpClient.GetWithContext(ctx, EndpointTokenMetadata)
	if err != nil {
		return TokenMetadataResponse{}, err
	}
	defer resp.Body.Close()

	var tmd TokenMetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&tmd); err != nil {
		return TokenMetadataResponse{}, err
	}

	return tmd, nil
}

func (h *client) ValidateClientConfiguration() error {
	if len(h.multiRPCClient.rpcs) == 0 {
		return fmt.Errorf("no raydium RPC nodes configured")
	}

	return nil
}

func (h *client) GetMultipleAccounts(ctx context.Context, accounts []solana.PublicKey) ([]*rpc.Account, error) {
	// choose random endpoint to use. pre-validate raydium client configuration with ValidateClientConfiguration.
	cycleValue := len(h.multiRPCClient.rpcs)
	i := rand.Intn(cycleValue)

	for i < i+cycleValue {
		rpcClient := h.multiRPCClient.rpcs[i%cycleValue]
		accountsResp, err := rpcClient.GetMultipleAccountsWithOpts(ctx, accounts, &rpc.GetMultipleAccountsOpts{
			Commitment: rpc.CommitmentProcessed,
		})
		if err != nil {
			i++
			continue
		}

		if accountsResp == nil || accountsResp.Value == nil {
			i++
			h.multiRPCClient.logger.Error("error getting multiple accounts", zap.String("error", "nil response"))
			continue
		}

		if len(accountsResp.Value) != len(accounts) {
			i++
			h.multiRPCClient.logger.Error("error getting multiple accounts", zap.String("error", "invalid account number"))
			continue
		}

		return accountsResp.Value, nil
	}

	return nil, fmt.Errorf("all rpc attempts failed %v", accounts)
}

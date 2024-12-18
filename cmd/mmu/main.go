package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/skip-mev/connect-mmu/cmd/mmu/cmd"
	"github.com/skip-mev/connect-mmu/cmd/mmu/logging"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/signing"
	"github.com/skip-mev/connect-mmu/signing/local"
	"github.com/skip-mev/connect-mmu/signing/simulate"

	"github.com/aws/aws-lambda-go/lambda"
	"go.uber.org/zap"
)

type LambdaEvent struct {
	Command   string `json:"command"`
	Timestamp string `json:"timestamp,omitempty"`
}

type LambdaResponse struct {
	Timestamp string `json:"timestamp"`
}

func createSigningRegistry() *signing.Registry {
	r := signing.NewRegistry()
	err := errors.Join(
		r.RegisterSigner(simulate.TypeName, simulate.NewSigningAgent),
		r.RegisterSigner(local.TypeName, local.NewSigningAgent),
	)
	if err != nil {
		panic(err)
	}
	return r
}

func getArgsFromLambdaEvent(ctx context.Context, event json.RawMessage, cmcAPIKey string) ([]string, error) {
	logger := logging.Logger(ctx)

	var lambdaEvent LambdaEvent
	if err := json.Unmarshal(event, &lambdaEvent); err != nil {
		logger.Error("failed to unmarshal Lambda event", zap.Error(err))
		return nil, err
	}

	if lambdaEvent.Command != "index" && lambdaEvent.Timestamp == "" {
		return nil, fmt.Errorf("lambda commands require a timestamp of the input file(s) to use")
	}

	// Set TIMESTAMP env var for file I/O prefixes in S3
	var timestamp string
	if lambdaEvent.Command == "index" {
		timestamp = time.Now().UTC().Format(time.RFC3339)
	} else {
		timestamp = lambdaEvent.Timestamp
	}
	os.Setenv("TIMESTAMP", timestamp)

	args := []string{lambdaEvent.Command}

	switch command := lambdaEvent.Command; command {
	case "validate":
		args = append(args, "--market-map", "generated-market-map.json", "--cmc-api-key", cmcAPIKey, "--start-delay", "10s", "--duration", "1m", "--enable-all")
	case "upserts":
		args = append(args, "--warn-on-invalid-market-map")
	case "diff":
		args = append(args, "--market-map", "generated-market-map.json", "--network", "dydx-staging", "--output", "diff", "--slinky-api")
	case "dispatch":
		args = append(args, "--config", "./local/config-dydx-staging.json", "--upserts", "upserts.json", "--simulate")
	}

	logger.Info("received Lambda command", zap.Strings("args", args))

	return args, nil
}

func lambdaHandler(ctx context.Context, event json.RawMessage) (resp LambdaResponse, err error) {
	logger := logging.Logger(ctx)

	// Fetch CMC API Key from Secrets Manager and set it as env var
	// so it can be used by the Indexer HTTP client
	cmcAPIKey, err := aws.GetSecret(ctx, "market-map-updater-cmc-api-key")
	if err != nil {
		logger.Error("failed to get CMC API key from Secrets Manager", zap.Error(err))
		return resp, err
	}
	os.Setenv("CMC_API_KEY", cmcAPIKey)

	args, err := getArgsFromLambdaEvent(ctx, event, cmcAPIKey)
	if err != nil {
		logger.Error("failed to get args from Lambda event", zap.Error(err))
		return resp, err
	}

	r := createSigningRegistry()
	rootCmd := cmd.RootCmd(r)
	rootCmd.SetArgs(args)
	if err := rootCmd.Execute(); err != nil {
		logger.Error("command returned errors", zap.Strings("command", args), zap.Error(err))
		// Return errors for all commands other than "validate"
		if args[0] != "validate" {
			return resp, err
		}
	}

	return LambdaResponse{
		Timestamp: os.Getenv("TIMESTAMP"),
	}, nil
}

func main() {
	if aws.IsLambda() {
		// Running in AWS Lambda
		lambda.Start(lambdaHandler)
	} else {
		// Running locally
		r := createSigningRegistry()
		if err := cmd.RootCmd(r).Execute(); err != nil {
			os.Exit(1)
		}
	}
}

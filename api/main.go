package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	cmd "github.com/skip-mev/connect-mmu/cmd/mmu/cmd/basic"

	"github.com/skip-mev/connect-mmu/lib/aws"
)

func lambdaHandler(_ context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	network := request.QueryStringParameters["network"]
	if network != "testnet" && network != "mainnet" {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprintf("Invalid network: %s, must be one of: {testnet | mainnet}", network),
		}, nil
	}
	os.Setenv("NETWORK", network)

	txJSON, err := aws.ReadFromS3(cmd.LatestTransactionsFilename, false)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Failed to read file from S3: %s-%s", network, cmd.LatestTransactionsFilename),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(txJSON),
	}, nil
}

func serverError(err error) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusInternalServerError,
		Body:       http.StatusText(http.StatusInternalServerError),
	}, nil
}

func clientError(status int, body string) (events.APIGatewayProxyResponse, error) {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       body,
	}, nil
}

func main() {
	lambda.Start(lambdaHandler)
}

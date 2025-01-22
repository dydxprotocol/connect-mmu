package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/skip-mev/connect-mmu/cmd/mmu/consts"

	"github.com/skip-mev/connect-mmu/lib/aws"
)

func lambdaHandler(_ context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	network := request.QueryStringParameters["network"]
	supportedNetworks := consts.GetSupportedNetworks()
	if !slices.Contains(supportedNetworks, network) {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       fmt.Sprintf("Invalid network: %s", network),
		}, nil
	}
	os.Setenv("NETWORK", network)

	txJSON, err := aws.ReadFromS3(consts.LatestTransactionsFilename, false)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Failed to read file from S3: %s-%s", network, consts.LatestTransactionsFilename),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(txJSON),
	}, nil
}

func main() {
	lambda.Start(lambdaHandler)
}

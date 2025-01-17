package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	cmd "github.com/skip-mev/connect-mmu/cmd/mmu/cmd/basic"

	"github.com/skip-mev/connect-mmu/lib/aws"
)

func lambdaHandler(_ context.Context, _ events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	txJSON, err := aws.ReadFromS3(cmd.LatestTransactionsFilename, false)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to read %s", cmd.LatestTransactionsFilename),
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

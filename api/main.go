package mmuapi

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/skip-mev/connect-mmu/lib/aws"
)

var FILENAME = "latest-mmu-tx.json"

func lambdaHandler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	txJson, err := aws.ReadFromS3("latest-mmu-tx.json")
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to read %s", FILENAME),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(txJson),
	}, nil
}

func main() {
	lambda.Start(lambdaHandler)
}

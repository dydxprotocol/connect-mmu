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

const EndpointRoot = "/market-map-updater/v1"

type Endpoint int

const (
	Tx Endpoint = iota
	ValidationErrors
	HealthReports
	NewMarkets
	RemovedMarkets
	UpdatedMarkets
)

func getSupportedEndpoints() map[string]Endpoint {
	return map[string]Endpoint{
		fmt.Sprintf("%s/tx", EndpointRoot):                Tx,
		fmt.Sprintf("%s/validation-errors", EndpointRoot): ValidationErrors,
		fmt.Sprintf("%s/health-reports", EndpointRoot):    HealthReports,
		fmt.Sprintf("%s/new-markets", EndpointRoot):       NewMarkets,
		fmt.Sprintf("%s/removed-markets", EndpointRoot):   RemovedMarkets,
		fmt.Sprintf("%s/updated-markets", EndpointRoot):   UpdatedMarkets,
	}
}

func getFilenamesForEndpoints() map[Endpoint]string {
	return map[Endpoint]string{
		Tx:               consts.LatestTransactionsFilename,
		ValidationErrors: consts.LatestValidationErrorsFilename,
		HealthReports:    consts.LatestHealthReportsFilename,
		NewMarkets:       consts.LatestNewMarketsFilename,
		RemovedMarkets:   consts.LatestRemovedMarketsFilename,
		UpdatedMarkets:   consts.LatestUpdatedMarketsFilename,
	}
}

func response(statusCode int, body string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Body:       body,
	}
}

func lambdaHandler(_ context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Get requested endpoint
	endpointString := request.Resource
	supportedEndpoints := getSupportedEndpoints()
	endpoint, ok := supportedEndpoints[endpointString]
	if !ok {
		return response(http.StatusBadRequest, fmt.Sprintf("Invalid endpoint: %s", endpointString)), nil
	}
	filenamesForEndpoints := getFilenamesForEndpoints()
	filename := filenamesForEndpoints[endpoint]

	// Get requested network
	network := request.QueryStringParameters["network"]
	supportedNetworks := consts.GetSupportedNetworks()
	if !slices.Contains(supportedNetworks, network) {
		return response(http.StatusBadRequest, fmt.Sprintf("Invalid network: %s", network)), nil
	}
	os.Setenv("NETWORK", network)

	// Read requested file from S3
	fileJSON, err := aws.ReadFromS3(filename, false)
	if err != nil {
		return response(http.StatusInternalServerError, fmt.Sprintf("Failed to read file from S3: %s-%s", network, filename)), nil
	}

	// Return data to client
	return response(http.StatusOK, string(fileJSON)), nil
}

func main() {
	lambda.Start(lambdaHandler)
}

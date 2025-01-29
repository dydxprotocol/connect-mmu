package file

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/skip-mev/connect-mmu/lib/aws"

	mmtypes "github.com/skip-mev/connect/v2/x/marketmap/types"
)

/*
This module provides utils for reading / writing files, both locally and in AWS S3.
If the MMU is running in AWS, files are read / written to S3.
If the MMU is running locally, files are read / written to local disk.
*/

func ReadBytesFromFile(path string) ([]byte, error) {
	if aws.IsLambda() {
		// Read from S3
		return aws.ReadFromS3(path, true)
	}
	// Read from local file
	return os.ReadFile(path)
}

func WriteBytesToFile(path string, bz []byte) error {
	if aws.IsLambda() {
		// Write output to S3
		return aws.WriteToS3(path, bz, true)
	}
	// Write output to local file
	return os.WriteFile(path, bz, 0o600)
}

func ReadJSONFromFile[T any](path string) (t T, err error) {
	bz, err := ReadBytesFromFile(path)
	if err != nil {
		return t, err
	}
	err = json.Unmarshal(bz, &t)
	return t, err
}

func WriteJSONToFile(path string, data any) error {
	bz, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return WriteBytesToFile(path, bz)
}

func CreateAndWriteJSONToFile(path string, data any) error {
	if !aws.IsLambda() {
		// Create local file before writing
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("error creating file %s: %w", path, err)
		}
		defer file.Close()
	}
	return WriteJSONToFile(path, data)
}

func ReadMarketMapFromFile(path string) (marketMap mmtypes.MarketMap, err error) {
	if aws.IsLambda() {
		// Read from S3
		marketMap, err = ReadJSONFromFile[mmtypes.MarketMap](path)
		if err != nil {
			return marketMap, err
		}

		if err := marketMap.ValidateBasic(); err != nil {
			return marketMap, fmt.Errorf("error validating market map: %w", err)
		}
		return marketMap, nil

	}
	// Read from local file
	return mmtypes.ReadMarketMapFromFile(path)
}

func WriteMarketMapToFile(path string, marketMap mmtypes.MarketMap) error {
	if aws.IsLambda() {
		// Write output to S3
		return WriteJSONToFile(path, marketMap)
	}
	// Write output to local file
	if err := mmtypes.WriteMarketMapToFile(marketMap, path); err != nil {
		return fmt.Errorf("failed to write market map: %w", err)
	}
	return nil
}

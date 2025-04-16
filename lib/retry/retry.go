package retry

import (
	"fmt"
	"time"
)

var schedule = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	10 * time.Second,
	30 * time.Second,
	90 * time.Second,
}

type Options struct {
	OnRetry      func(attempt int, err error)
	FunctionName string
}

var DefaultOptions = &Options{
	OnRetry:      func(_ int, _ error) {},
	FunctionName: "unknown",
}

func NewOptions(onRetry func(attempt int, err error), functionName string) *Options {
	return &Options{
		OnRetry:      onRetry,
		FunctionName: functionName,
	}
}

func WithBackoffAndOptions[T any](operation func() (T, error), opts *Options) (T, error) {
	var result T
	var err error

	// if opts is nil, use the default options
	if opts == nil {
		opts = DefaultOptions
	}

	for i, delay := range schedule {
		result, err = operation()
		if err == nil {
			return result, nil
		}

		opts.OnRetry(i, err)

		if i == len(schedule)-1 {
			break
		}

		time.Sleep(delay)
	}

	return result, fmt.Errorf("all retries failed for %s: %w", opts.FunctionName, err)
}

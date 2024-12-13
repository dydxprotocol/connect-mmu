package log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/skip-mev/connect-mmu/lib/aws"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ErrAPIResponse is the error returned when the DataDog API returns a non-200 response
var ErrAPIResponse = errors.New("error writing logs, bad response from API")

const maxRetryWaitSeconds = 10

// DataDogLog is a log message in DataDog format
type DataDogLog struct {
	Message string `json:"message"`
}

// Options are options for writing to DataDog
type Options struct {
	Host     string
	Source   string
	Service  string
	Hostname string
	Tags     []string
}

// DataDogLogger is a logger that writes to DataDog
type DataDogLogger struct {
	Context context.Context
	URL     string
	APIKey  string
	Options Options
	client  *http.Client
	Lines   []DataDogLog
	mutex   sync.Mutex
}

func NewZapDataDogLogger(config ZapConfig, options Options) (*zap.Logger, error) {
	ddApiKeySecretArn := os.Getenv("DD_API_KEY_SECRET_ARN")
	ddApiKey, err := aws.GetSecret(context.TODO(), ddApiKeySecretArn)
	if err != nil {
		return nil, err
	}

	datadog, err := newDataDogLogger(context.TODO(), ddApiKey, options)
	if err != nil {
		return nil, err
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	// set up the primary output to always include os.Stderr.
	logLevel := zapcore.InfoLevel
	if err := logLevel.Set(config.StdOutLogLevel); err != nil {
		fmt.Fprintf(os.Stderr, "failed to set log level on std out: %v\nfalling back to info", err)
		logLevel = zapcore.InfoLevel // Fallback to info if setting fails
	}

	datadogCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		datadog,
		logLevel,
	)

	return zap.New(
		datadogCore,
		zap.AddCaller(),
		zap.Fields(zapcore.Field{Key: "pid", Type: zapcore.Int64Type, Integer: int64(os.Getpid())}),
	), nil
}

// newDataDogLogger creates a new logger that writes to DataDog
func newDataDogLogger(ctx context.Context, apiKey string, options Options) (*DataDogLogger, error) {
	h := "https://http-intake.logs.datadoghq.com/v1/input"
	if options.Host != "" {
		h = options.Host
	}
	u, err := ddURL(h, options)
	if err != nil {
		return nil, err
	}
	retryClient := retryablehttp.NewClient()
	retryClient.RetryWaitMax = maxRetryWaitSeconds * time.Second
	return &DataDogLogger{
		Context: ctx,
		URL:     u,
		APIKey:  apiKey,
		Options: options,
		Lines:   []DataDogLog{},
		client:  retryClient.StandardClient(),
	}, nil
}

// ddURL creates a url with embedded options
func ddURL(base string, options Options) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	parameters := url.Values{}
	if options.Source != "" {
		parameters.Add("ddsource", options.Source)
	}
	if len(options.Tags) > 0 {
		parameters.Add("ddtags", strings.Join(options.Tags, ","))
	}
	if options.Hostname != "" {
		parameters.Add("hostname", options.Hostname)
	}
	if options.Service != "" {
		parameters.Add("service", options.Service)
	}
	u.RawQuery = parameters.Encode()
	return u.String(), nil
}

// Write adds bytes to buffer prior to sync
func (d *DataDogLogger) Write(p []byte) (n int, err error) {
	d.mutex.Lock()
	d.Lines = append(d.Lines, DataDogLog{
		Message: string(p),
	})
	d.mutex.Unlock()
	return len(p), nil
}

// Sync posts data all available data to the DataDog API
func (d *DataDogLogger) Sync() error {
	if len(d.Lines) > 0 {
		d.mutex.Lock()
		body, err := json.Marshal(d.Lines)

		if err != nil {
			_, wErr := fmt.Fprintf(os.Stderr, "error serializing logs %v", err)
			if wErr != nil {
				return wErr
			}
			return err
		}

		err = d.Post(body)
		if err != nil {
			return err
		}

		d.Lines = []DataDogLog{}
		d.mutex.Unlock()
	}
	return nil
}

// Post writes body to the DataDog api
func (d *DataDogLogger) Post(body []byte) error {
	req, err := http.NewRequestWithContext(d.Context, http.MethodPost, d.URL, bytes.NewBuffer(body))
	if err != nil {
		_, wErr := fmt.Fprintf(os.Stderr, "error writing logs %v", err)
		if wErr != nil {
			return wErr
		}
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("DD-API-KEY", d.APIKey)
	resp, respErr := d.client.Do(req)
	if respErr != nil {
		_, wErr := fmt.Fprintf(os.Stderr, "error writing logs %v", respErr)
		if wErr != nil {
			return wErr
		}
		return respErr
	}
	// nolint: errcheck
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	default:
		_, wErr := fmt.Fprintf(os.Stderr, "error writing logs %d status code returned", resp.StatusCode)
		if wErr != nil {
			return wErr
		}
		return ErrAPIResponse
	}
}

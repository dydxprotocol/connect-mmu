package logging

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/skip-mev/connect-mmu/lib/aws"
	"github.com/skip-mev/connect-mmu/lib/log"
)

type loggerKey struct{}

func ConfigureLogger(level string) {
	config := log.NewDefaultZapConfig()
	config.StdOutLogLevel = level
	var logger *zap.Logger
	var err error
	if aws.IsLambda() {
		logger, err = log.NewZapDataDogLogger(config, log.Options{
			Host:     "",
			Source:   "",
			Service:  os.Getenv("ENVIRONMENT") + "-market-map-updater",
			Hostname: "",
			Tags:     []string{},
		})
		if err != nil {
			panic(fmt.Errorf("failed to create Zap DataDog logger: %w", err))
		}
	} else {
		logger = log.NewZapLogger(config)
	}
	zap.ReplaceGlobals(logger)
}

func Logger(ctx context.Context) *zap.Logger {
	logger, ok := ctx.Value(loggerKey{}).(*zap.Logger)
	if !ok {
		return zap.L()
	}
	return logger
}

func LoggerContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey{}, zap.L())
}

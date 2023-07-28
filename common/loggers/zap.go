package loggers

import (
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ceramicnetwork/go-cas"
	"github.com/ceramicnetwork/go-cas/models"
)

func NewLogger() models.Logger {
	level := zap.NewAtomicLevelAt(zap.DebugLevel)

	logLevel := os.Getenv(cas.Env_LogLevel)
	if len(logLevel) > 0 {
		if parsedLevel, err := zap.ParseAtomicLevel(logLevel); err != nil {
			log.Fatalf("Error parsing log level %s: %v", logLevel, err)
		} else {
			level = parsedLevel
		}
	}

	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.Level = level
	cfg.EncoderConfig.TimeKey = "timestamp"
	baseLogger := zap.Must(cfg.Build())
	return baseLogger.Sugar()
}

func NewTestLogger() models.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.TimeKey = "timestamp"
	baseLogger := zap.Must(cfg.Build())
	return baseLogger.Sugar()
}

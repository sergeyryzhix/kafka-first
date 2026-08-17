package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level string) (*zap.Logger, error) {

	zapLevel, err := zap.ParseAtomicLevel(level)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга уровня логирования %q: %w", level, err)
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zapLevel
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	cfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder

	log, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("ошибка сборки логгера zap: %w", err)
	}

	return log, nil
}

package config

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type App struct {
	HTTPPort        string        `env:"HTTP_PORT" envDefault:"8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

type Log struct {
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

type Kafka struct {
	Brokers []string `env:"BROKERS" envSeparator:"," envDefault:"localhost:9092"`
	Topic   string   `env:"TOPIC" envDefault:"demo-events"`
	GroupID string   `env:"GROUP_ID" envDefault:"demo-consumer"`
}

type Config struct {
	App   App
	Log   Log
	Kafka Kafka `envPrefix:"KAFKA_"`
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load .env: %w", err)
	}

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}

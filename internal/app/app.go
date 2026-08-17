package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/sergeyryzhix/kafka-first/internal/config"
	"github.com/sergeyryzhix/kafka-first/internal/domain"
	"github.com/sergeyryzhix/kafka-first/internal/kafka/consumer"
	"github.com/sergeyryzhix/kafka-first/internal/kafka/producer"
	"github.com/sergeyryzhix/kafka-first/internal/logger"
	"github.com/sergeyryzhix/kafka-first/internal/server"
)

type App struct {
	cfg      *config.Config
	log      *zap.Logger
	producer *producer.Producer
	consumer *consumer.Consumer
	fiber    *fiber.App
}

func New() (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки конфига: %w", err)
	}

	log, err := logger.New(cfg.Log.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("ошибка инициализации логгера: %w", err)
	}
	log.Info("конфиг загружен",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("group", cfg.Kafka.GroupID),
		zap.String("port", cfg.App.HTTPPort),
		zap.Duration("shutdown_timeout", cfg.App.ShutdownTimeout),
	)

	prod, err := producer.NewProducer(cfg.Kafka)
	if err != nil {
		return nil, fmt.Errorf("ошибка инициализации producer: %w", err)
	}

	handler := func(msg domain.Message) {
		log.Info("получено сообщение",
			zap.Int("id", msg.ID),
			zap.String("content", msg.Content),
			zap.Time("timestamp", msg.Timestamp),
		)
	}

	cons, err := consumer.NewConsumer(cfg.Kafka, handler, log)
	if err != nil {
		_ = prod.Close()
		return nil, fmt.Errorf("ошибка инициализации consumer: %w", err)
	}

	msgHandler := server.NewMessageHandler(prod, log)

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("сервер работает")
	})
	app.Post("/send", msgHandler.SendMessages)

	return &App{
		cfg:      cfg,
		log:      log,
		producer: prod,
		consumer: cons,
		fiber:    app,
	}, nil
}

func (a *App) Start(ctx context.Context) error {
	defer a.log.Sync()

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		a.log.Info("consumer запущен")
		if err := a.consumer.Start(runCtx); err != nil {
			a.log.Error("consumer остановился", zap.Error(err))
		}
	}()

	go func() {
		addr := ":" + a.cfg.App.HTTPPort
		a.log.Info("сервер запущен", zap.String("addr", addr))
		if err := a.fiber.Listen(addr); err != nil {
			a.log.Error("ошибка сервера", zap.Error(err))
		}
	}()

	<-runCtx.Done()
	a.log.Info("завершение работы")

	shutCtx, cancel := context.WithTimeout(context.Background(), a.cfg.App.ShutdownTimeout)
	defer cancel()

	if err := a.fiber.ShutdownWithContext(shutCtx); err != nil {
		a.log.Error("ошибка shutdown", zap.Error(err))
	}

	return a.Close()
}

func (a *App) Close() error {
	if a.producer != nil {
		if err := a.producer.Close(); err != nil {
			return fmt.Errorf("ошибка закрытия producer: %w", err)
		}
	}
	if a.consumer != nil {
		if err := a.consumer.Close(); err != nil {
			return fmt.Errorf("ошибка закрытия consumer: %w", err)
		}
	}

	return nil
}

func Run(ctx context.Context) error {
	app, err := New()
	if err != nil {
		return err
	}
	return app.Start(ctx)
}

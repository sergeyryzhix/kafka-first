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

func Start(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("ошибка загрузки конфига: %w", err)
	}

	log, err := logger.New(cfg.Log.LogLevel)
	if err != nil {
		return fmt.Errorf("ошибка инициализации логгера: %w", err)
	}
	defer log.Sync()

	prod, err := producer.NewProducer(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("ошибка инициализации producer: %w", err)
	}
	defer prod.Close()

	handler := func(msg domain.Message) {
		log.Info("получено сообщение",
			zap.Int("id", msg.ID),
			zap.String("content", msg.Content),
			zap.Time("timestamp", msg.Timestamp),
		)
	}

	cons, err := consumer.NewConsumer(cfg.Kafka, handler, log)
	if err != nil {
		return fmt.Errorf("ошибка инициализации consumer: %w", err)
	}
	defer cons.Close()

	msgHandler := server.NewMessageHandler(prod, log)

	fiberApp := fiber.New()
	server.SetupRoutes(fiberApp, msgHandler)

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("consumer запущен")
		if err := cons.Start(runCtx); err != nil {
			log.Error("consumer остановился", zap.Error(err))
		}
	}()

	go func() {
		addr := ":" + cfg.App.HTTPPort
		log.Info("сервер запущен", zap.String("addr", addr))
		if err := fiberApp.Listen(addr); err != nil {
			log.Error("ошибка сервера", zap.Error(err))
		}
	}()

	<-runCtx.Done()
	log.Info("завершение работы")

	shutCtx, cancel := context.WithTimeout(ctx, cfg.App.ShutdownTimeout)
	defer cancel()

	if err := fiberApp.ShutdownWithContext(shutCtx); err != nil {
		log.Error("ошибка shutdown", zap.Error(err))
	}

	return nil
}

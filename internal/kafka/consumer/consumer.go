package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/sergeyryzhix/kafka-first/internal/config"
	"github.com/sergeyryzhix/kafka-first/internal/domain"
	"go.uber.org/zap"
)

type MessageHandler func(msg domain.Message)

type Consumer struct {
	reader  *kafkago.Reader
	handler MessageHandler
	log     *zap.Logger
}

func NewConsumer(cfg config.Kafka, handler MessageHandler, log *zap.Logger) (*Consumer, error) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: kafkago.FirstOffset,
	})

	return &Consumer{
		reader:  reader,
		handler: handler,
		log:     log,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	for {
		kafkaMsg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("чтение сообщения: %w", err)
		}

		var msg domain.Message
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			c.log.Error("ошибка парсинга сообщения",
				zap.Error(err),
				zap.ByteString("value", kafkaMsg.Value),
				zap.Int("partition", kafkaMsg.Partition),
				zap.Int64("offset", kafkaMsg.Offset),
			)
			continue
		}

		c.handler(msg)
	}
}

func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("закрытие consumer: %w", err)
	}
	return nil
}

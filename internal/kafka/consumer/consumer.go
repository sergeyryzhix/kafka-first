package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/sergeyryzhix/kafka-first/internal/domain"
	"go.uber.org/zap"
)

type MessageHandler func(msg domain.Message)

type Consumer struct {
	reader  *kafkago.Reader
	handler MessageHandler
	log     *zap.Logger
}

func NewConsumer(brokers []string, topic, groupID string, handler MessageHandler, log *zap.Logger) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers is empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}
	if groupID == "" {
		return nil, fmt.Errorf("groupID is empty")
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is nil")
	}
	if log == nil {
		return nil, fmt.Errorf("logger is nil")
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
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

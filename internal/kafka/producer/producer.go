package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/sergeyryzhix/kafka-first/internal/config"
	"github.com/sergeyryzhix/kafka-first/internal/domain"
)

type Producer struct {
	writer *kafkago.Writer
	topic  string
}

func NewProducer(cfg config.Kafka) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("brokers is empty")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("topic is empty")
	}

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafkago.LeastBytes{},
		RequiredAcks:           kafkago.RequireAll,
		AllowAutoTopicCreation: true,
	}

	return &Producer{
		writer: writer,
		topic:  cfg.Topic,
	}, nil
}

func (p *Producer) SendMessage(msg domain.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	kafkaMsg := kafkago.Message{
		Topic: p.topic,
		Value: data,
	}

	if err := p.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("ошибка отправки: %w", err)
	}

	return nil
}

func (p *Producer) SendBatch(messages []domain.Message) error {
	if len(messages) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	kafkaMessages := make([]kafkago.Message, len(messages))

	for i, message := range messages {
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("ошибка сериализации %d: %w", message.ID, err)
		}
		kafkaMessages[i] = kafkago.Message{
			Topic: p.topic,
			Value: data,
		}
	}
	if err := p.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		return fmt.Errorf("ошибка отправки сообщений: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("ошибка закрытия продюсера: %w", err)
	}

	return nil
}

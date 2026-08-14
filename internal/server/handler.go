package server

import (
	"fmt"
	"math/rand"

	"github.com/gofiber/fiber/v2"
	"github.com/sergeyryzhix/kafka-first/internal/domain"
	"github.com/sergeyryzhix/kafka-first/internal/kafka/producer"
	"go.uber.org/zap"
)

type MessageHandler struct {
	producer *producer.Producer
	log      *zap.Logger
}

func NewMessageHandler(p *producer.Producer, log *zap.Logger) *MessageHandler {
	return &MessageHandler{
		producer: p,
		log:      log,
	}
}

func (h *MessageHandler) SendMessages(c *fiber.Ctx) error {
	num := 100 + rand.Intn(101)

	msgs := make([]domain.Message, 0, num)
	for i := 1; i <= num; i++ {
		msgs = append(msgs, domain.NewMessage(i, fmt.Sprintf("сообщение #%d", i)))
	}

	if err := h.producer.SendBatch(msgs); err != nil {
		h.log.Error("ошибка отправки батча продюсеру",
			zap.Error(err),
			zap.Int("messages_count", len(msgs)),
			zap.Int("random_num", num),
			zap.String("path", c.Path()),
			zap.String("method", c.Method()),
		)
		return fiber.NewError(fiber.StatusInternalServerError, "внутренняя ошибка сервера")
	}

	return c.Status(fiber.StatusOK).JSON(domain.Response{
		Status:  "success",
		Message: fmt.Sprintf("отправлено %d сообщений", len(msgs)),
		Count:   len(msgs),
	})

}

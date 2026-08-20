package server

import "github.com/gofiber/fiber/v2"

func SetupRoutes(app *fiber.App, h *MessageHandler) {
	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("сервер работает")
	})
	app.Post("/send", h.SendMessages)
}

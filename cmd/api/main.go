package main

import (
	"context"
	"log"

	"github.com/sergeyryzhix/kafka-first/internal/app"
)

func main() {
	if err := app.Start(context.Background()); err != nil {
		log.Fatalf("критическая ошибка запуска: %v", err)
	}
}

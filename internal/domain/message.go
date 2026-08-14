package domain

import "time"

type Message struct {
	ID        int       `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Count   int    `json:"count"`
}

func NewMessage(id int, content string) Message {
	return Message{
		ID:        id,
		Content:   content,
		Timestamp: time.Now().UTC(),
	}
}

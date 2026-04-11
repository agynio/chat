package store

import (
	"time"

	"github.com/google/uuid"
)

type Chat struct {
	ThreadID       uuid.UUID
	OrganizationID uuid.UUID
	CreatedAt      time.Time
	Status         string
	Summary        *string
}

type PageCursor struct {
	AfterID uuid.UUID
}

type ChatListResult struct {
	Chats      []Chat
	NextCursor *PageCursor
}

type UpdateChatParams struct {
	Status       *string
	Summary      *string
	ClearSummary bool
}

type ChatListFilter struct {
	Status *string
}

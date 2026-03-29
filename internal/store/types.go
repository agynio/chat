package store

import (
	"time"

	"github.com/google/uuid"
)

type Chat struct {
	ThreadID       uuid.UUID
	OrganizationID uuid.UUID
	CreatedAt      time.Time
}

type PageCursor struct {
	AfterID uuid.UUID
}

type ChatListResult struct {
	Chats      []Chat
	NextCursor *PageCursor
}

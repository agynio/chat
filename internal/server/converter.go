package server

import (
	"fmt"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
)

const (
	chatStatusOpen   = "open"
	chatStatusClosed = "closed"
)

func threadToChat(thread *threadsv1.Thread, organizationID string, status chatv1.ChatStatus, summary *string) *chatv1.Chat {
	participants := make([]*chatv1.ChatParticipant, len(thread.GetParticipants()))
	for i, p := range thread.GetParticipants() {
		participants[i] = &chatv1.ChatParticipant{
			Id:       p.GetId(),
			JoinedAt: p.GetJoinedAt(),
		}
	}

	return &chatv1.Chat{
		Id:             thread.GetId(),
		Participants:   participants,
		CreatedAt:      thread.GetCreatedAt(),
		UpdatedAt:      thread.GetUpdatedAt(),
		OrganizationId: organizationID,
		Status:         status,
		Summary:        summary,
	}
}

func chatStatusToString(status chatv1.ChatStatus) string {
	switch status {
	case chatv1.ChatStatus_CHAT_STATUS_OPEN:
		return chatStatusOpen
	case chatv1.ChatStatus_CHAT_STATUS_CLOSED:
		return chatStatusClosed
	default:
		panic(fmt.Sprintf("unsupported chat status: %s", status))
	}
}

func stringToChatStatus(status string) chatv1.ChatStatus {
	switch status {
	case chatStatusOpen:
		return chatv1.ChatStatus_CHAT_STATUS_OPEN
	case chatStatusClosed:
		return chatv1.ChatStatus_CHAT_STATUS_CLOSED
	default:
		panic(fmt.Sprintf("unsupported chat status: %s", status))
	}
}

func threadMessageToChatMessage(msg *threadsv1.Message) *chatv1.ChatMessage {
	return &chatv1.ChatMessage{
		Id:        msg.GetId(),
		ChatId:    msg.GetThreadId(),
		SenderId:  msg.GetSenderId(),
		Body:      msg.GetBody(),
		FileIds:   msg.GetFileIds(),
		CreatedAt: msg.GetCreatedAt(),
	}
}

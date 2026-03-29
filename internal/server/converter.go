package server

import (
	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
)

func threadToChat(thread *threadsv1.Thread, organizationID string) *chatv1.Chat {
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

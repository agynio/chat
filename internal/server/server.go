package server

import (
	"context"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/chat/internal/identity"
)

type Server struct {
	chatv1.UnimplementedChatServiceServer
	threads threadsv1.ThreadsServiceClient
}

func New(threads threadsv1.ThreadsServiceClient) *Server {
	return &Server{threads: threads}
}

func (s *Server) CreateChat(ctx context.Context, req *chatv1.CreateChatRequest) (*chatv1.CreateChatResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	if len(req.GetParticipantIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "participant_ids must not be empty")
	}

	participantIDs := make([]string, 0, len(req.GetParticipantIds())+1)
	participantIDs = append(participantIDs, id.IdentityID)
	for _, pid := range req.GetParticipantIds() {
		if pid == id.IdentityID {
			continue
		}
		participantIDs = append(participantIDs, pid)
	}

	resp, err := s.threads.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		ParticipantIds: participantIDs,
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	return &chatv1.CreateChatResponse{
		Chat: threadToChat(resp.GetThread()),
	}, nil
}

func (s *Server) GetChats(ctx context.Context, req *chatv1.GetChatsRequest) (*chatv1.GetChatsResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	resp, err := s.threads.GetThreads(ctx, &threadsv1.GetThreadsRequest{
		ParticipantId: id.IdentityID,
		PageSize:      req.GetPageSize(),
		PageToken:     req.GetPageToken(),
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	chats := make([]*chatv1.Chat, len(resp.GetThreads()))
	for i, thread := range resp.GetThreads() {
		chats[i] = threadToChat(thread)
	}

	return &chatv1.GetChatsResponse{
		Chats:         chats,
		NextPageToken: resp.GetNextPageToken(),
	}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *chatv1.GetMessagesRequest) (*chatv1.GetMessagesResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	if req.GetChatId() == "" {
		return nil, status.Error(codes.InvalidArgument, "chat_id is required")
	}

	msgResp, err := s.threads.GetMessages(ctx, &threadsv1.GetMessagesRequest{
		ThreadId:  req.GetChatId(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	unreadCount, err := s.countUnread(ctx, id.IdentityID, req.GetChatId())
	if err != nil {
		return nil, mapThreadsError(err)
	}

	messages := make([]*chatv1.ChatMessage, len(msgResp.GetMessages()))
	for i, msg := range msgResp.GetMessages() {
		messages[i] = threadMessageToChatMessage(msg)
	}

	return &chatv1.GetMessagesResponse{
		Messages:      messages,
		NextPageToken: msgResp.GetNextPageToken(),
		UnreadCount:   unreadCount,
	}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *chatv1.SendMessageRequest) (*chatv1.SendMessageResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	if req.GetChatId() == "" {
		return nil, status.Error(codes.InvalidArgument, "chat_id is required")
	}
	if req.GetBody() == "" && len(req.GetFileIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "body or file_ids must be provided")
	}

	resp, err := s.threads.SendMessage(ctx, &threadsv1.SendMessageRequest{
		ThreadId: req.GetChatId(),
		SenderId: id.IdentityID,
		Body:     req.GetBody(),
		FileIds:  req.GetFileIds(),
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	return &chatv1.SendMessageResponse{
		Message: threadMessageToChatMessage(resp.GetMessage()),
	}, nil
}

func (s *Server) MarkAsRead(ctx context.Context, req *chatv1.MarkAsReadRequest) (*chatv1.MarkAsReadResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	if req.GetChatId() == "" {
		return nil, status.Error(codes.InvalidArgument, "chat_id is required")
	}
	if len(req.GetMessageIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "message_ids must not be empty")
	}

	resp, err := s.threads.AckMessages(ctx, &threadsv1.AckMessagesRequest{
		ParticipantId: id.IdentityID,
		MessageIds:    req.GetMessageIds(),
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	return &chatv1.MarkAsReadResponse{
		ReadCount: resp.GetAckedCount(),
	}, nil
}

func (s *Server) countUnread(ctx context.Context, participantID, chatID string) (int32, error) {
	var count int32
	var pageToken string

	for {
		resp, err := s.threads.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{
			ParticipantId: participantID,
			PageSize:      100,
			PageToken:     pageToken,
		})
		if err != nil {
			return 0, err
		}

		for _, msg := range resp.GetMessages() {
			if msg.GetThreadId() == chatID {
				count++
			}
		}

		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	return count, nil
}

func mapThreadsError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return status.Errorf(codes.Internal, "threads: %v", err)
	}
	return st.Err()
}

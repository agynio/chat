package server

import (
	"context"
	"errors"
	"fmt"
	"log"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/chat/internal/identity"
	"github.com/agynio/chat/internal/store"
)

type Server struct {
	chatv1.UnimplementedChatServiceServer
	threads threadsv1.ThreadsServiceClient
	store   chatStore
}

const unackedPageSize = 100

const threadsPageSize = 100

const maxThreadsPages = 10

type chatStore interface {
	CreateChat(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error)
	ListChats(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error)
}

func New(threads threadsv1.ThreadsServiceClient, store chatStore) *Server {
	return &Server{threads: threads, store: store}
}

func (s *Server) CreateChat(ctx context.Context, req *chatv1.CreateChatRequest) (*chatv1.CreateChatResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
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

	threadID, err := uuid.Parse(resp.GetThread().GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "threads returned invalid thread id: %v", err)
	}
	if _, err := s.store.CreateChat(ctx, threadID, organizationID); err != nil {
		log.Printf("chat: store thread %s for org %s failed (returning thread only): %v", threadID, organizationID, err)
	}

	return &chatv1.CreateChatResponse{
		Chat: threadToChat(resp.GetThread(), organizationID.String()),
	}, nil
}

func (s *Server) GetChats(ctx context.Context, req *chatv1.GetChatsRequest) (*chatv1.GetChatsResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	cursor, err := decodePageCursor(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
	}

	pageSize := store.NormalizePageSize(req.GetPageSize())
	chats := make([]*chatv1.Chat, 0, int(pageSize))
	var nextCursor *store.PageCursor
	for len(chats) < int(pageSize) {
		remaining := pageSize - int32(len(chats))
		result, err := s.store.ListChats(ctx, organizationID, remaining, cursor)
		if err != nil {
			return nil, toStatusError(err)
		}
		if len(result.Chats) == 0 {
			nextCursor = result.NextCursor
			break
		}

		threadIDs := make([]uuid.UUID, len(result.Chats))
		for i, chat := range result.Chats {
			threadIDs[i] = chat.ThreadID
		}

		threadsByID, err := s.fetchThreads(ctx, id.IdentityID, threadIDs)
		if err != nil {
			return nil, mapThreadsError(err)
		}

		for _, chat := range result.Chats {
			thread, ok := threadsByID[chat.ThreadID]
			if !ok {
				continue
			}
			if !threadHasParticipant(thread, id.IdentityID) {
				continue
			}
			chats = append(chats, threadToChat(thread, chat.OrganizationID.String()))
		}

		nextCursor = result.NextCursor
		if len(chats) >= int(pageSize) || result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
	}

	nextToken := ""
	if nextCursor != nil {
		nextToken = store.EncodePageToken(nextCursor.AfterID)
	}

	return &chatv1.GetChatsResponse{
		Chats:         chats,
		NextPageToken: nextToken,
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

	// TODO: Threads.GetUnackedMessages lacks a thread-scoped filter, so we scan
	// all unacked messages across chats; a thread filter upstream would avoid
	// this full scan.
	for {
		resp, err := s.threads.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{
			ParticipantId: participantID,
			PageSize:      unackedPageSize,
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

func decodePageCursor(token string) (*store.PageCursor, error) {
	if token == "" {
		return nil, nil
	}
	id, err := store.DecodePageToken(token)
	if err != nil {
		return nil, err
	}
	return &store.PageCursor{AfterID: id}, nil
}

func parseUUID(value string) (uuid.UUID, error) {
	return uuid.Parse(value)
}

func toStatusError(err error) error {
	var notFound *store.NotFoundError
	if errors.As(err, &notFound) {
		return status.Error(codes.NotFound, notFound.Error())
	}
	var alreadyExists *store.AlreadyExistsError
	if errors.As(err, &alreadyExists) {
		return status.Error(codes.AlreadyExists, alreadyExists.Error())
	}
	var invalidToken *store.InvalidPageTokenError
	if errors.As(err, &invalidToken) {
		return status.Error(codes.InvalidArgument, invalidToken.Error())
	}
	return status.Errorf(codes.Internal, "store: %v", err)
}

func (s *Server) fetchThreads(ctx context.Context, participantID string, threadIDs []uuid.UUID) (map[uuid.UUID]*threadsv1.Thread, error) {
	if len(threadIDs) == 0 {
		return map[uuid.UUID]*threadsv1.Thread{}, nil
	}

	pending := make(map[string]struct{}, len(threadIDs))
	for _, id := range threadIDs {
		pending[id.String()] = struct{}{}
	}

	threads := make(map[uuid.UUID]*threadsv1.Thread, len(threadIDs))
	pageToken := ""
	// TODO: Threads needs a GetThreadsByIds RPC to avoid paginated scans.
	for page := 0; len(pending) > 0; page++ {
		if page >= maxThreadsPages {
			log.Printf("chat: fetchThreads exceeded %d pages for participant %s (%d pending)", maxThreadsPages, participantID, len(pending))
			break
		}
		resp, err := s.threads.GetThreads(ctx, &threadsv1.GetThreadsRequest{
			ParticipantId: participantID,
			PageSize:      threadsPageSize,
			PageToken:     pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, thread := range resp.GetThreads() {
			if _, ok := pending[thread.GetId()]; !ok {
				continue
			}
			threadID, err := uuid.Parse(thread.GetId())
			if err != nil {
				return nil, fmt.Errorf("threads returned invalid thread id %q: %w", thread.GetId(), err)
			}
			threads[threadID] = thread
			delete(pending, thread.GetId())
		}
		if len(pending) == 0 {
			break
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	return threads, nil
}

func threadHasParticipant(thread *threadsv1.Thread, participantID string) bool {
	for _, participant := range thread.GetParticipants() {
		if participant.GetId() == participantID {
			return true
		}
	}
	return false
}

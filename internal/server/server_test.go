package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/agynio/chat/internal/identity"
	"github.com/agynio/chat/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockThreadsClient struct {
	createThreadFunc       func(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error)
	archiveThreadFunc      func(ctx context.Context, req *threadsv1.ArchiveThreadRequest, opts ...grpc.CallOption) (*threadsv1.ArchiveThreadResponse, error)
	addParticipantFunc     func(ctx context.Context, req *threadsv1.AddParticipantRequest, opts ...grpc.CallOption) (*threadsv1.AddParticipantResponse, error)
	sendMessageFunc        func(ctx context.Context, req *threadsv1.SendMessageRequest, opts ...grpc.CallOption) (*threadsv1.SendMessageResponse, error)
	getThreadsFunc         func(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error)
	getMessagesFunc        func(ctx context.Context, req *threadsv1.GetMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetMessagesResponse, error)
	getUnackedMessagesFunc func(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error)
	ackMessagesFunc        func(ctx context.Context, req *threadsv1.AckMessagesRequest, opts ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error)
}

func (m *mockThreadsClient) CreateThread(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
	if m.createThreadFunc == nil {
		return nil, unexpectedCall("CreateThread")
	}
	return m.createThreadFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) ArchiveThread(ctx context.Context, req *threadsv1.ArchiveThreadRequest, opts ...grpc.CallOption) (*threadsv1.ArchiveThreadResponse, error) {
	if m.archiveThreadFunc == nil {
		return nil, unexpectedCall("ArchiveThread")
	}
	return m.archiveThreadFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) AddParticipant(ctx context.Context, req *threadsv1.AddParticipantRequest, opts ...grpc.CallOption) (*threadsv1.AddParticipantResponse, error) {
	if m.addParticipantFunc == nil {
		return nil, unexpectedCall("AddParticipant")
	}
	return m.addParticipantFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) SendMessage(ctx context.Context, req *threadsv1.SendMessageRequest, opts ...grpc.CallOption) (*threadsv1.SendMessageResponse, error) {
	if m.sendMessageFunc == nil {
		return nil, unexpectedCall("SendMessage")
	}
	return m.sendMessageFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) GetThreads(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
	if m.getThreadsFunc == nil {
		return nil, unexpectedCall("GetThreads")
	}
	return m.getThreadsFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) GetMessages(ctx context.Context, req *threadsv1.GetMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetMessagesResponse, error) {
	if m.getMessagesFunc == nil {
		return nil, unexpectedCall("GetMessages")
	}
	return m.getMessagesFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) GetUnackedMessages(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
	if m.getUnackedMessagesFunc == nil {
		return nil, unexpectedCall("GetUnackedMessages")
	}
	return m.getUnackedMessagesFunc(ctx, req, opts...)
}

func (m *mockThreadsClient) AckMessages(ctx context.Context, req *threadsv1.AckMessagesRequest, opts ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error) {
	if m.ackMessagesFunc == nil {
		return nil, unexpectedCall("AckMessages")
	}
	return m.ackMessagesFunc(ctx, req, opts...)
}

type mockStore struct {
	createChatFunc func(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error)
	listChatsFunc  func(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error)
}

func (m *mockStore) CreateChat(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error) {
	if m.createChatFunc == nil {
		return store.Chat{}, unexpectedStoreCall("CreateChat")
	}
	return m.createChatFunc(ctx, threadID, organizationID)
}

func (m *mockStore) ListChats(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error) {
	if m.listChatsFunc == nil {
		return store.ChatListResult{}, unexpectedStoreCall("ListChats")
	}
	return m.listChatsFunc(ctx, organizationID, pageSize, cursor)
}

func unexpectedCall(method string) error {
	return status.Errorf(codes.Internal, "unexpected %s call", method)
}

func unexpectedStoreCall(method string) error {
	return fmt.Errorf("unexpected store %s call", method)
}

func contextWithIdentity(identityID string) context.Context {
	md := metadata.New(map[string]string{
		identity.MetadataKeyIdentityID:   identityID,
		identity.MetadataKeyIdentityType: "user",
	})
	return metadata.NewIncomingContext(context.Background(), md)
}

func requireStatusCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s", code)
	}
	if status.Code(err) != code {
		t.Fatalf("expected code %s, got %s", code, status.Code(err))
	}
}

func requireTimestamp(t *testing.T, got *timestamppb.Timestamp, want time.Time) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected timestamp %s, got nil", want)
	}
	if !got.AsTime().Equal(want) {
		t.Fatalf("expected timestamp %s, got %s", want, got.AsTime())
	}
}

func TestCreateChatRequiresIdentity(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.CreateChat(context.Background(), &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2"}, OrganizationId: uuid.NewString()})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestCreateChatRejectsEmptyParticipants(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.CreateChat(contextWithIdentity("user-1"), &chatv1.CreateChatRequest{OrganizationId: uuid.NewString()})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChatRequiresOrganizationID(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.CreateChat(contextWithIdentity("user-1"), &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2"}})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChatRejectsInvalidOrganizationID(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.CreateChat(contextWithIdentity("user-1"), &chatv1.CreateChatRequest{
		ParticipantIds: []string{"user-2"},
		OrganizationId: "not-a-uuid",
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChatDeduplicatesParticipants(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	orgID := uuid.New()
	var gotRequest *threadsv1.CreateThreadRequest
	threadID := uuid.New()
	thread := &threadsv1.Thread{Id: threadID.String()}
	var storedThreadID uuid.UUID
	var storedOrgID uuid.UUID
	threads := &mockThreadsClient{
		createThreadFunc: func(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
			gotRequest = req
			return &threadsv1.CreateThreadResponse{Thread: thread}, nil
		},
	}
	chatStore := &mockStore{
		createChatFunc: func(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error) {
			storedThreadID = threadID
			storedOrgID = organizationID
			return store.Chat{ThreadID: threadID, OrganizationID: organizationID, CreatedAt: time.Now()}, nil
		},
	}

	srv := New(threads, chatStore)
	resp, err := srv.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2", "user-1", "user-3"}, OrganizationId: orgID.String()})
	if err != nil {
		t.Fatalf("CreateChat returned error: %v", err)
	}
	if gotRequest == nil {
		t.Fatalf("CreateThread was not called")
	}
	expectedParticipants := []string{"user-1", "user-2", "user-3"}
	if !reflect.DeepEqual(gotRequest.ParticipantIds, expectedParticipants) {
		t.Fatalf("expected participants %v, got %v", expectedParticipants, gotRequest.ParticipantIds)
	}
	if resp.GetChat().GetId() != thread.GetId() {
		t.Fatalf("expected chat id %q, got %q", thread.GetId(), resp.GetChat().GetId())
	}
	if resp.GetChat().GetOrganizationId() != orgID.String() {
		t.Fatalf("expected organization id %q, got %q", orgID, resp.GetChat().GetOrganizationId())
	}
	if storedThreadID != threadID {
		t.Fatalf("expected stored thread id %s, got %s", threadID, storedThreadID)
	}
	if storedOrgID != orgID {
		t.Fatalf("expected stored org id %s, got %s", orgID, storedOrgID)
	}
}

func TestCreateChatReturnsThreadWhenStoreFails(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	orgID := uuid.New()
	threadID := uuid.New()
	thread := &threadsv1.Thread{Id: threadID.String()}
	var storedThreadID uuid.UUID
	threads := &mockThreadsClient{
		createThreadFunc: func(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
			return &threadsv1.CreateThreadResponse{Thread: thread}, nil
		},
	}
	chatStore := &mockStore{
		createChatFunc: func(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error) {
			storedThreadID = threadID
			return store.Chat{}, fmt.Errorf("store unavailable")
		},
	}

	srv := New(threads, chatStore)
	resp, err := srv.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2"}, OrganizationId: orgID.String()})
	if err != nil {
		t.Fatalf("CreateChat returned error: %v", err)
	}
	if resp.GetChat().GetId() != threadID.String() {
		t.Fatalf("expected chat id %q, got %q", threadID.String(), resp.GetChat().GetId())
	}
	if resp.GetChat().GetOrganizationId() != orgID.String() {
		t.Fatalf("expected organization id %q, got %q", orgID, resp.GetChat().GetOrganizationId())
	}
	if storedThreadID != threadID {
		t.Fatalf("expected stored thread id %s, got %s", threadID, storedThreadID)
	}
}

func TestGetChatsRequiresOrganizationID(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.GetChats(contextWithIdentity("user-1"), &chatv1.GetChatsRequest{})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestGetChatsRejectsInvalidPageToken(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.GetChats(contextWithIdentity("user-1"), &chatv1.GetChatsRequest{
		OrganizationId: uuid.NewString(),
		PageToken:      "not-a-token",
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestGetChatsUsesStoreAndThreads(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	orgID := uuid.New()
	cursorID := uuid.New()
	threadID1 := uuid.New()
	threadID2 := uuid.New()
	createdAt := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

	var gotOrgID uuid.UUID
	var gotPageSize int32
	var gotCursor *store.PageCursor
	chatStore := &mockStore{
		listChatsFunc: func(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error) {
			gotOrgID = organizationID
			gotPageSize = pageSize
			gotCursor = cursor
			return store.ChatListResult{
				Chats: []store.Chat{
					{ThreadID: threadID2, OrganizationID: orgID, CreatedAt: createdAt},
					{ThreadID: threadID1, OrganizationID: orgID, CreatedAt: createdAt},
				},
				NextCursor: &store.PageCursor{AfterID: threadID1},
			}, nil
		},
	}

	threads := &mockThreadsClient{
		getThreadsFunc: func(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
			if req.GetParticipantId() != "user-1" {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected participant %q", req.GetParticipantId())
			}
			return &threadsv1.GetThreadsResponse{
				Threads: []*threadsv1.Thread{
					{
						Id: threadID1.String(),
						Participants: []*threadsv1.Participant{
							{Id: "user-1", JoinedAt: timestamppb.New(createdAt)},
							{Id: "user-2", JoinedAt: timestamppb.New(createdAt)},
						},
						CreatedAt: timestamppb.New(createdAt),
						UpdatedAt: timestamppb.New(createdAt),
					},
					{
						Id: threadID2.String(),
						Participants: []*threadsv1.Participant{
							{Id: "user-1", JoinedAt: timestamppb.New(createdAt)},
						},
						CreatedAt: timestamppb.New(createdAt),
						UpdatedAt: timestamppb.New(createdAt),
					},
				},
			}, nil
		},
	}

	srv := New(threads, chatStore)
	pageToken := store.EncodePageToken(cursorID)
	resp, err := srv.GetChats(ctx, &chatv1.GetChatsRequest{
		OrganizationId: orgID.String(),
		PageSize:       2,
		PageToken:      pageToken,
	})
	if err != nil {
		t.Fatalf("GetChats returned error: %v", err)
	}
	if gotOrgID != orgID {
		t.Fatalf("expected org id %s, got %s", orgID, gotOrgID)
	}
	if gotPageSize != 2 {
		t.Fatalf("expected page size 2, got %d", gotPageSize)
	}
	if gotCursor == nil || gotCursor.AfterID != cursorID {
		t.Fatalf("expected cursor %s, got %#v", cursorID, gotCursor)
	}
	if resp.GetNextPageToken() != store.EncodePageToken(threadID1) {
		t.Fatalf("expected next page token %q, got %q", store.EncodePageToken(threadID1), resp.GetNextPageToken())
	}
	if len(resp.GetChats()) != 2 {
		t.Fatalf("expected 2 chats, got %d", len(resp.GetChats()))
	}
	if resp.GetChats()[0].GetId() != threadID2.String() || resp.GetChats()[1].GetId() != threadID1.String() {
		t.Fatalf("unexpected chat order: %v", []string{resp.GetChats()[0].GetId(), resp.GetChats()[1].GetId()})
	}
	if resp.GetChats()[0].GetOrganizationId() != orgID.String() {
		t.Fatalf("expected org id %q, got %q", orgID, resp.GetChats()[0].GetOrganizationId())
	}
}

func TestGetChatsFillsPageAfterFiltering(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	orgID := uuid.New()
	threadID1 := uuid.New()
	threadID2 := uuid.New()
	threadID3 := uuid.New()
	createdAt := time.Date(2024, 5, 7, 8, 9, 10, 0, time.UTC)

	listCalls := 0
	chatStore := &mockStore{
		listChatsFunc: func(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error) {
			listCalls++
			switch listCalls {
			case 1:
				if cursor != nil {
					return store.ChatListResult{}, fmt.Errorf("expected nil cursor")
				}
				if pageSize != 2 {
					return store.ChatListResult{}, fmt.Errorf("expected page size 2, got %d", pageSize)
				}
				return store.ChatListResult{
					Chats: []store.Chat{
						{ThreadID: threadID1, OrganizationID: orgID, CreatedAt: createdAt},
						{ThreadID: threadID2, OrganizationID: orgID, CreatedAt: createdAt},
					},
					NextCursor: &store.PageCursor{AfterID: threadID2},
				}, nil
			case 2:
				if cursor == nil || cursor.AfterID != threadID2 {
					return store.ChatListResult{}, fmt.Errorf("expected cursor %s, got %#v", threadID2, cursor)
				}
				if pageSize != 1 {
					return store.ChatListResult{}, fmt.Errorf("expected page size 1, got %d", pageSize)
				}
				return store.ChatListResult{
					Chats:      []store.Chat{{ThreadID: threadID3, OrganizationID: orgID, CreatedAt: createdAt}},
					NextCursor: &store.PageCursor{AfterID: threadID3},
				}, nil
			default:
				return store.ChatListResult{}, fmt.Errorf("unexpected ListChats call %d", listCalls)
			}
		},
	}

	threadCalls := 0
	threads := &mockThreadsClient{
		getThreadsFunc: func(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
			threadCalls++
			switch threadCalls {
			case 1:
				return &threadsv1.GetThreadsResponse{
					Threads: []*threadsv1.Thread{
						{
							Id: threadID1.String(),
							Participants: []*threadsv1.Participant{
								{Id: "user-1", JoinedAt: timestamppb.New(createdAt)},
							},
							CreatedAt: timestamppb.New(createdAt),
							UpdatedAt: timestamppb.New(createdAt),
						},
						{
							Id: threadID2.String(),
							Participants: []*threadsv1.Participant{
								{Id: "user-2", JoinedAt: timestamppb.New(createdAt)},
							},
							CreatedAt: timestamppb.New(createdAt),
							UpdatedAt: timestamppb.New(createdAt),
						},
					},
				}, nil
			case 2:
				return &threadsv1.GetThreadsResponse{
					Threads: []*threadsv1.Thread{
						{
							Id: threadID3.String(),
							Participants: []*threadsv1.Participant{
								{Id: "user-1", JoinedAt: timestamppb.New(createdAt)},
							},
							CreatedAt: timestamppb.New(createdAt),
							UpdatedAt: timestamppb.New(createdAt),
						},
					},
				}, nil
			default:
				return nil, status.Errorf(codes.Internal, "unexpected GetThreads call %d", threadCalls)
			}
		},
	}

	srv := New(threads, chatStore)
	resp, err := srv.GetChats(ctx, &chatv1.GetChatsRequest{OrganizationId: orgID.String(), PageSize: 2})
	if err != nil {
		t.Fatalf("GetChats returned error: %v", err)
	}
	if listCalls != 2 {
		t.Fatalf("expected 2 ListChats calls, got %d", listCalls)
	}
	if len(resp.GetChats()) != 2 {
		t.Fatalf("expected 2 chats, got %d", len(resp.GetChats()))
	}
	if resp.GetChats()[0].GetId() != threadID1.String() || resp.GetChats()[1].GetId() != threadID3.String() {
		t.Fatalf("unexpected chat order: %v", []string{resp.GetChats()[0].GetId(), resp.GetChats()[1].GetId()})
	}
	if resp.GetNextPageToken() != store.EncodePageToken(threadID3) {
		t.Fatalf("expected next page token %q, got %q", store.EncodePageToken(threadID3), resp.GetNextPageToken())
	}
}

func TestGetChatsFiltersNonParticipantThreads(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	orgID := uuid.New()
	threadID := uuid.New()
	createdAt := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)

	chatStore := &mockStore{
		listChatsFunc: func(ctx context.Context, organizationID uuid.UUID, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error) {
			return store.ChatListResult{
				Chats: []store.Chat{{ThreadID: threadID, OrganizationID: orgID, CreatedAt: createdAt}},
			}, nil
		},
	}

	threads := &mockThreadsClient{
		getThreadsFunc: func(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
			return &threadsv1.GetThreadsResponse{
				Threads: []*threadsv1.Thread{
					{
						Id: threadID.String(),
						Participants: []*threadsv1.Participant{
							{Id: "user-2", JoinedAt: timestamppb.New(createdAt)},
						},
						CreatedAt: timestamppb.New(createdAt),
						UpdatedAt: timestamppb.New(createdAt),
					},
				},
			}, nil
		},
	}

	srv := New(threads, chatStore)
	resp, err := srv.GetChats(ctx, &chatv1.GetChatsRequest{OrganizationId: orgID.String()})
	if err != nil {
		t.Fatalf("GetChats returned error: %v", err)
	}
	if len(resp.GetChats()) != 0 {
		t.Fatalf("expected no chats, got %d", len(resp.GetChats()))
	}
}

func TestGetMessagesAggregatesUnread(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	chatID := "chat-1"
	msgTime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	threadsMessages := []*threadsv1.Message{
		{Id: "m1", ThreadId: chatID, SenderId: "user-2", Body: "hello", FileIds: []string{"f1"}, CreatedAt: timestamppb.New(msgTime)},
		{Id: "m2", ThreadId: chatID, SenderId: "user-3", Body: "second", FileIds: nil, CreatedAt: timestamppb.New(msgTime)},
	}
	var gotMessagesReq *threadsv1.GetMessagesRequest
	var gotPageTokens []string
	threads := &mockThreadsClient{
		getMessagesFunc: func(ctx context.Context, req *threadsv1.GetMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetMessagesResponse, error) {
			gotMessagesReq = req
			return &threadsv1.GetMessagesResponse{Messages: threadsMessages, NextPageToken: "next-token"}, nil
		},
		getUnackedMessagesFunc: func(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
			if req.GetParticipantId() != "user-1" {
				t.Fatalf("expected participant user-1, got %q", req.GetParticipantId())
			}
			if req.GetPageSize() != unackedPageSize {
				t.Fatalf("expected page size %d, got %d", unackedPageSize, req.GetPageSize())
			}
			gotPageTokens = append(gotPageTokens, req.GetPageToken())
			switch len(gotPageTokens) {
			case 1:
				return &threadsv1.GetUnackedMessagesResponse{
					Messages: []*threadsv1.Message{
						{Id: "u1", ThreadId: chatID},
						{Id: "u2", ThreadId: "chat-2"},
					},
					NextPageToken: "page-2",
				}, nil
			case 2:
				return &threadsv1.GetUnackedMessagesResponse{
					Messages: []*threadsv1.Message{
						{Id: "u3", ThreadId: chatID},
						{Id: "u4", ThreadId: chatID},
					},
				}, nil
			default:
				t.Fatalf("unexpected GetUnackedMessages call %d", len(gotPageTokens))
				return nil, nil
			}
		},
	}

	srv := New(threads, &mockStore{})
	resp, err := srv.GetMessages(ctx, &chatv1.GetMessagesRequest{ChatId: chatID, PageSize: 2, PageToken: "page-1"})
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	if gotMessagesReq == nil {
		t.Fatalf("GetMessages did not call Threads.GetMessages")
	}
	if gotMessagesReq.GetThreadId() != chatID {
		t.Fatalf("expected thread id %q, got %q", chatID, gotMessagesReq.GetThreadId())
	}
	if gotMessagesReq.GetPageSize() != 2 {
		t.Fatalf("expected page size 2, got %d", gotMessagesReq.GetPageSize())
	}
	if gotMessagesReq.GetPageToken() != "page-1" {
		t.Fatalf("expected page token page-1, got %q", gotMessagesReq.GetPageToken())
	}
	if resp.GetUnreadCount() != 3 {
		t.Fatalf("expected unread count 3, got %d", resp.GetUnreadCount())
	}
	if resp.GetNextPageToken() != "next-token" {
		t.Fatalf("expected next page token next-token, got %q", resp.GetNextPageToken())
	}
	if len(resp.GetMessages()) != len(threadsMessages) {
		t.Fatalf("expected %d messages, got %d", len(threadsMessages), len(resp.GetMessages()))
	}
	if len(gotPageTokens) != 2 || gotPageTokens[0] != "" || gotPageTokens[1] != "page-2" {
		t.Fatalf("unexpected page tokens %v", gotPageTokens)
	}
	first := resp.GetMessages()[0]
	if first.GetId() != "m1" || first.GetChatId() != chatID || first.GetSenderId() != "user-2" || first.GetBody() != "hello" {
		t.Fatalf("unexpected first message: %+v", first)
	}
	if !reflect.DeepEqual(first.GetFileIds(), []string{"f1"}) {
		t.Fatalf("expected file ids [f1], got %v", first.GetFileIds())
	}
	requireTimestamp(t, first.GetCreatedAt(), msgTime)
}

func TestSendMessageValidation(t *testing.T) {
	srv := New(&mockThreadsClient{}, &mockStore{})
	_, err := srv.SendMessage(contextWithIdentity("user-1"), &chatv1.SendMessageRequest{ChatId: "chat-1"})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestSendMessageDelegates(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	msgTime := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	threads := &mockThreadsClient{
		sendMessageFunc: func(ctx context.Context, req *threadsv1.SendMessageRequest, opts ...grpc.CallOption) (*threadsv1.SendMessageResponse, error) {
			if req.GetThreadId() != "chat-1" {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected thread id %q", req.GetThreadId())
			}
			if req.GetSenderId() != "user-1" {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected sender id %q", req.GetSenderId())
			}
			if req.GetBody() != "hello" {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected body %q", req.GetBody())
			}
			if !reflect.DeepEqual(req.GetFileIds(), []string{"file-1"}) {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected file ids %v", req.GetFileIds())
			}
			return &threadsv1.SendMessageResponse{Message: &threadsv1.Message{
				Id:        "msg-1",
				ThreadId:  "chat-1",
				SenderId:  "user-1",
				Body:      "hello",
				FileIds:   []string{"file-1"},
				CreatedAt: timestamppb.New(msgTime),
			}}, nil
		},
	}

	srv := New(threads, &mockStore{})
	resp, err := srv.SendMessage(ctx, &chatv1.SendMessageRequest{ChatId: "chat-1", Body: "hello", FileIds: []string{"file-1"}})
	if err != nil {
		t.Fatalf("SendMessage returned error: %v", err)
	}
	if resp.GetMessage().GetId() != "msg-1" || resp.GetMessage().GetChatId() != "chat-1" {
		t.Fatalf("unexpected message response: %+v", resp.GetMessage())
	}
	requireTimestamp(t, resp.GetMessage().GetCreatedAt(), msgTime)
}

func TestMarkAsReadValidation(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	for name, req := range map[string]*chatv1.MarkAsReadRequest{
		"missing chat id":     {MessageIds: []string{"msg-1"}},
		"missing message ids": {ChatId: "chat-1"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := New(&mockThreadsClient{}, &mockStore{})
			_, err := srv.MarkAsRead(ctx, req)
			requireStatusCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestMarkAsReadDelegates(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	threads := &mockThreadsClient{
		ackMessagesFunc: func(ctx context.Context, req *threadsv1.AckMessagesRequest, opts ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error) {
			if req.GetParticipantId() != "user-1" {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected participant %q", req.GetParticipantId())
			}
			if !reflect.DeepEqual(req.GetMessageIds(), []string{"msg-1", "msg-2"}) {
				return nil, status.Errorf(codes.InvalidArgument, "unexpected message ids %v", req.GetMessageIds())
			}
			return &threadsv1.AckMessagesResponse{AckedCount: 2}, nil
		},
	}

	srv := New(threads, &mockStore{})
	resp, err := srv.MarkAsRead(ctx, &chatv1.MarkAsReadRequest{ChatId: "chat-1", MessageIds: []string{"msg-1", "msg-2"}})
	if err != nil {
		t.Fatalf("MarkAsRead returned error: %v", err)
	}
	if resp.GetReadCount() != 2 {
		t.Fatalf("expected read count 2, got %d", resp.GetReadCount())
	}
}

func TestThreadToChat(t *testing.T) {
	joinedAt := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	createdAt := time.Date(2024, 3, 4, 5, 6, 8, 0, time.UTC)
	updatedAt := time.Date(2024, 3, 4, 5, 6, 9, 0, time.UTC)
	thread := &threadsv1.Thread{
		Id: "thread-1",
		Participants: []*threadsv1.Participant{
			{Id: "user-1", JoinedAt: timestamppb.New(joinedAt)},
			{Id: "user-2", JoinedAt: timestamppb.New(joinedAt)},
		},
		CreatedAt: timestamppb.New(createdAt),
		UpdatedAt: timestamppb.New(updatedAt),
	}

	chat := threadToChat(thread, "org-1")
	if chat.GetId() != "thread-1" {
		t.Fatalf("expected chat id thread-1, got %q", chat.GetId())
	}
	if len(chat.GetParticipants()) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(chat.GetParticipants()))
	}
	if chat.GetParticipants()[0].GetId() != "user-1" {
		t.Fatalf("expected participant user-1, got %q", chat.GetParticipants()[0].GetId())
	}
	if chat.GetOrganizationId() != "org-1" {
		t.Fatalf("expected organization id org-1, got %q", chat.GetOrganizationId())
	}
	requireTimestamp(t, chat.GetParticipants()[0].GetJoinedAt(), joinedAt)
	requireTimestamp(t, chat.GetCreatedAt(), createdAt)
	requireTimestamp(t, chat.GetUpdatedAt(), updatedAt)
}

func TestThreadMessageToChatMessage(t *testing.T) {
	createdAt := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	msg := &threadsv1.Message{
		Id:        "msg-1",
		ThreadId:  "thread-1",
		SenderId:  "user-1",
		Body:      "hello",
		FileIds:   []string{"file-1"},
		CreatedAt: timestamppb.New(createdAt),
	}

	chatMsg := threadMessageToChatMessage(msg)
	if chatMsg.GetId() != "msg-1" || chatMsg.GetChatId() != "thread-1" || chatMsg.GetSenderId() != "user-1" {
		t.Fatalf("unexpected chat message: %+v", chatMsg)
	}
	if chatMsg.GetBody() != "hello" {
		t.Fatalf("expected body hello, got %q", chatMsg.GetBody())
	}
	if !reflect.DeepEqual(chatMsg.GetFileIds(), []string{"file-1"}) {
		t.Fatalf("expected file ids [file-1], got %v", chatMsg.GetFileIds())
	}
	requireTimestamp(t, chatMsg.GetCreatedAt(), createdAt)
}

var _ threadsv1.ThreadsServiceClient = (*mockThreadsClient)(nil)

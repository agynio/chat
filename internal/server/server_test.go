package server

import (
	"context"
	"reflect"
	"testing"
	"time"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/agynio/chat/internal/identity"
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

func unexpectedCall(method string) error {
	return status.Errorf(codes.Internal, "unexpected %s call", method)
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
	srv := New(&mockThreadsClient{})
	_, err := srv.CreateChat(context.Background(), &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2"}})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestCreateChatRejectsEmptyParticipants(t *testing.T) {
	srv := New(&mockThreadsClient{})
	_, err := srv.CreateChat(contextWithIdentity("user-1"), &chatv1.CreateChatRequest{})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChatDeduplicatesParticipants(t *testing.T) {
	ctx := contextWithIdentity("user-1")
	var gotRequest *threadsv1.CreateThreadRequest
	thread := &threadsv1.Thread{Id: "thread-1"}
	threads := &mockThreadsClient{
		createThreadFunc: func(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
			gotRequest = req
			return &threadsv1.CreateThreadResponse{Thread: thread}, nil
		},
	}

	srv := New(threads)
	resp, err := srv.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{"user-2", "user-1", "user-3"}})
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

	srv := New(threads)
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
	srv := New(&mockThreadsClient{})
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

	srv := New(threads)
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
			srv := New(&mockThreadsClient{})
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

	srv := New(threads)
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

	chat := threadToChat(thread)
	if chat.GetId() != "thread-1" {
		t.Fatalf("expected chat id thread-1, got %q", chat.GetId())
	}
	if len(chat.GetParticipants()) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(chat.GetParticipants()))
	}
	if chat.GetParticipants()[0].GetId() != "user-1" {
		t.Fatalf("expected participant user-1, got %q", chat.GetParticipants()[0].GetId())
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

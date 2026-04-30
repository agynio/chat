package e2e

import (
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	identityv1 "github.com/agynio/chat/gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/chat/gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/agynio/chat/internal/server"
	"github.com/agynio/chat/internal/store"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupInProcessEnv(t *testing.T) *testEnv {
	t.Helper()

	threads := newInMemoryThreads()
	runners := newInMemoryRunners()
	identity := newInMemoryIdentity()
	chatStore := newInMemoryStore()
	grpcServer := grpc.NewServer()
	chatv1.RegisterChatServiceServer(grpcServer, server.New(threads, runners, identity, chatStore))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		if closeErr := lis.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Fatalf("dial chat: %v (close listener: %v)", err, closeErr)
		}
		t.Fatalf("dial chat: %v", err)
	}

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close chat connection: %v", err)
		}
		grpcServer.GracefulStop()
		if err := lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("close listener: %v", err)
		}
	})

	return &testEnv{
		client: chatv1.NewChatServiceClient(conn),
		conn:   conn,
	}
}

type inMemoryStore struct {
	mu    sync.Mutex
	chats map[uuid.UUID]store.Chat
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{chats: make(map[uuid.UUID]store.Chat)}
}

func (s *inMemoryStore) CreateChat(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chats[threadID]; ok {
		return store.Chat{}, store.AlreadyExists("chat")
	}
	now := time.Now().UTC()
	chat := store.Chat{ThreadID: threadID, OrganizationID: organizationID, CreatedAt: now, Status: "open"}
	s.chats[threadID] = chat
	return chat, nil
}

func (s *inMemoryStore) GetChat(ctx context.Context, threadID uuid.UUID) (store.Chat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chat, ok := s.chats[threadID]
	if !ok {
		return store.Chat{}, store.NotFound("chat")
	}
	return chat, nil
}

func (s *inMemoryStore) UpdateChat(ctx context.Context, threadID uuid.UUID, params store.UpdateChatParams) (store.Chat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	chat, ok := s.chats[threadID]
	if !ok {
		return store.Chat{}, store.NotFound("chat")
	}
	if params.Status != nil {
		chat.Status = *params.Status
	}
	if params.ClearSummary {
		chat.Summary = nil
	} else if params.Summary != nil {
		chat.Summary = params.Summary
	}
	s.chats[threadID] = chat
	return chat, nil
}

func (s *inMemoryStore) ListChats(ctx context.Context, organizationID uuid.UUID, filter store.ChatListFilter, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	chats := make([]store.Chat, 0, len(s.chats))
	for _, chat := range s.chats {
		if chat.OrganizationID != organizationID {
			continue
		}
		if filter.Status != nil && chat.Status != *filter.Status {
			continue
		}
		chats = append(chats, chat)
	}

	sort.Slice(chats, func(i, j int) bool {
		if chats[i].CreatedAt.Equal(chats[j].CreatedAt) {
			return chats[i].ThreadID.String() > chats[j].ThreadID.String()
		}
		return chats[i].CreatedAt.After(chats[j].CreatedAt)
	})

	if cursor != nil {
		index := -1
		for i, chat := range chats {
			if chat.ThreadID == cursor.AfterID {
				index = i
				break
			}
		}
		if index == -1 {
			return store.ChatListResult{}, store.InvalidPageToken(errors.New("cursor not found"))
		}
		chats = chats[index+1:]
	}

	limit := store.NormalizePageSize(pageSize)
	var nextCursor *store.PageCursor
	if int32(len(chats)) > limit {
		nextCursor = &store.PageCursor{AfterID: chats[limit-1].ThreadID}
		chats = chats[:limit]
	}

	return store.ChatListResult{Chats: chats, NextCursor: nextCursor}, nil
}

type inMemoryThreads struct {
	mu      sync.Mutex
	threads map[string]*threadState
}

type threadState struct {
	thread   *threadsv1.Thread
	messages []*threadsv1.Message
	acked    map[string]map[string]bool
}

func newInMemoryThreads() *inMemoryThreads {
	return &inMemoryThreads{threads: make(map[string]*threadState)}
}

type inMemoryRunners struct{}

func newInMemoryRunners() *inMemoryRunners {
	return &inMemoryRunners{}
}

func (r *inMemoryRunners) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	return &runnersv1.ListWorkloadsByThreadResponse{}, nil
}

type inMemoryIdentity struct{}

func newInMemoryIdentity() *inMemoryIdentity {
	return &inMemoryIdentity{}
}

func (i *inMemoryIdentity) BatchGetIdentityTypes(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	entries := make([]*identityv1.IdentityTypeEntry, 0, len(req.GetIdentityIds()))
	for _, identityID := range req.GetIdentityIds() {
		entries = append(entries, &identityv1.IdentityTypeEntry{IdentityId: identityID, IdentityType: identityv1.IdentityType_IDENTITY_TYPE_USER})
	}
	return &identityv1.BatchGetIdentityTypesResponse{Entries: entries}, nil
}

func (t *inMemoryThreads) CreateThread(ctx context.Context, req *threadsv1.CreateThreadRequest, opts ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
	callerID, err := outgoingIdentityID(ctx)
	if err != nil {
		return nil, err
	}

	participantIDs := make([]string, 0, len(req.GetParticipants())+1)
	seen := make(map[string]struct{})
	addParticipant := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		participantIDs = append(participantIDs, id)
	}
	addParticipant(callerID)
	for _, participant := range req.GetParticipants() {
		addParticipant(participant.GetParticipantId())
	}

	now := time.Now().UTC()
	threadID := uuid.NewString()
	thread := &threadsv1.Thread{
		Id:             threadID,
		OrganizationId: req.GetOrganizationId(),
		CreatedAt:      timestamppb.New(now),
		UpdatedAt:      timestamppb.New(now),
		Participants:   make([]*threadsv1.Participant, len(participantIDs)),
	}
	for i, id := range participantIDs {
		thread.Participants[i] = &threadsv1.Participant{Id: id, JoinedAt: timestamppb.New(now)}
	}

	state := &threadState{
		thread: thread,
		acked:  make(map[string]map[string]bool),
	}
	for _, id := range participantIDs {
		state.acked[id] = make(map[string]bool)
	}

	t.mu.Lock()
	t.threads[threadID] = state
	t.mu.Unlock()

	return &threadsv1.CreateThreadResponse{Thread: thread}, nil
}

func (t *inMemoryThreads) ArchiveThread(ctx context.Context, req *threadsv1.ArchiveThreadRequest, opts ...grpc.CallOption) (*threadsv1.ArchiveThreadResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "ArchiveThread not implemented")
}

func (t *inMemoryThreads) DegradeThread(ctx context.Context, req *threadsv1.DegradeThreadRequest, opts ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	threadID := req.GetThreadId()
	if threadID == "" {
		return nil, status.Error(codes.InvalidArgument, "thread_id is required")
	}

	t.mu.Lock()
	state, ok := t.threads[threadID]
	if ok {
		state.thread.Status = threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED
	}
	t.mu.Unlock()

	if !ok {
		return nil, status.Error(codes.NotFound, "thread not found")
	}
	return &threadsv1.DegradeThreadResponse{Thread: state.thread}, nil
}

func (t *inMemoryThreads) AddParticipant(ctx context.Context, req *threadsv1.AddParticipantRequest, opts ...grpc.CallOption) (*threadsv1.AddParticipantResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "AddParticipant not implemented")
}

func (t *inMemoryThreads) SendMessage(ctx context.Context, req *threadsv1.SendMessageRequest, opts ...grpc.CallOption) (*threadsv1.SendMessageResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	if req.GetThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "thread_id is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	state, ok := t.threads[req.GetThreadId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "thread not found")
	}

	now := time.Now().UTC()
	msg := &threadsv1.Message{
		Id:        uuid.NewString(),
		ThreadId:  req.GetThreadId(),
		SenderId:  req.GetSenderId(),
		Body:      req.GetBody(),
		FileIds:   append([]string(nil), req.GetFileIds()...),
		CreatedAt: timestamppb.New(now),
	}
	state.messages = append(state.messages, msg)
	if _, ok := state.acked[req.GetSenderId()]; !ok {
		state.acked[req.GetSenderId()] = make(map[string]bool)
	}
	state.acked[req.GetSenderId()][msg.Id] = true
	state.thread.UpdatedAt = timestamppb.New(now)

	return &threadsv1.SendMessageResponse{Message: msg}, nil
}

func (t *inMemoryThreads) GetThreads(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	participantID := req.GetParticipantId()
	if participantID == "" {
		return nil, status.Error(codes.InvalidArgument, "participant_id is required")
	}

	items := make([]*threadsv1.Thread, 0)
	t.mu.Lock()
	for _, state := range t.threads {
		if threadHasParticipant(state.thread, participantID) {
			items = append(items, state.thread)
		}
	}
	t.mu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].GetId() < items[j].GetId()
	})

	start, err := parsePageOffset(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	items, nextToken := paginateSlice(items, start, pageSize)
	return &threadsv1.GetThreadsResponse{Threads: items, NextPageToken: nextToken}, nil
}

func (t *inMemoryThreads) ListOrganizationThreads(ctx context.Context, req *threadsv1.ListOrganizationThreadsRequest, opts ...grpc.CallOption) (*threadsv1.ListOrganizationThreadsResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "ListOrganizationThreads not implemented")
}

func (t *inMemoryThreads) GetOrganizationThreads(ctx context.Context, req *threadsv1.GetOrganizationThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetOrganizationThreadsResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unimplemented, "GetOrganizationThreads not implemented")
}

func (t *inMemoryThreads) GetThread(ctx context.Context, req *threadsv1.GetThreadRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	threadID := req.GetThreadId()
	if threadID == "" {
		return nil, status.Error(codes.InvalidArgument, "thread_id is required")
	}

	t.mu.Lock()
	state, ok := t.threads[threadID]
	t.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "thread not found")
	}
	return &threadsv1.GetThreadResponse{Thread: state.thread}, nil
}

func (t *inMemoryThreads) GetMessages(ctx context.Context, req *threadsv1.GetMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetMessagesResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	threadID := req.GetThreadId()
	if threadID == "" {
		return nil, status.Error(codes.InvalidArgument, "thread_id is required")
	}

	t.mu.Lock()
	state, ok := t.threads[threadID]
	if !ok {
		t.mu.Unlock()
		return nil, status.Error(codes.NotFound, "thread not found")
	}
	items := append([]*threadsv1.Message(nil), state.messages...)
	t.mu.Unlock()

	start, err := parsePageOffset(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	items, nextToken := paginateSlice(items, start, pageSize)
	return &threadsv1.GetMessagesResponse{Messages: items, NextPageToken: nextToken}, nil
}

func (t *inMemoryThreads) GetUnackedMessages(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	participantID := req.GetParticipantId()
	if participantID == "" {
		return nil, status.Error(codes.InvalidArgument, "participant_id is required")
	}

	threadID := req.GetThreadId()
	items := make([]*threadsv1.Message, 0)

	t.mu.Lock()
	for _, state := range t.threads {
		if threadID != "" && state.thread.GetId() != threadID {
			continue
		}
		if !threadHasParticipant(state.thread, participantID) {
			continue
		}
		ackedByParticipant := state.acked[participantID]
		for _, msg := range state.messages {
			if ackedByParticipant != nil && ackedByParticipant[msg.GetId()] {
				continue
			}
			items = append(items, msg)
		}
	}
	t.mu.Unlock()

	start, err := parsePageOffset(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	items, nextToken := paginateSlice(items, start, pageSize)
	return &threadsv1.GetUnackedMessagesResponse{Messages: items, NextPageToken: nextToken}, nil
}

func (t *inMemoryThreads) GetUnackedMessageCounts(ctx context.Context, req *threadsv1.GetUnackedMessageCountsRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessageCountsResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	participantID := req.GetParticipantId()
	if participantID == "" {
		return nil, status.Error(codes.InvalidArgument, "participant_id is required")
	}

	counts := make(map[string]int32)
	t.mu.Lock()
	for _, state := range t.threads {
		if !threadHasParticipant(state.thread, participantID) {
			continue
		}
		var count int32
		ackedByParticipant := state.acked[participantID]
		for _, msg := range state.messages {
			if ackedByParticipant != nil && ackedByParticipant[msg.GetId()] {
				continue
			}
			count++
		}
		if count > 0 {
			counts[state.thread.GetId()] = count
		}
	}
	t.mu.Unlock()

	return &threadsv1.GetUnackedMessageCountsResponse{CountsByThreadId: counts}, nil
}

func (t *inMemoryThreads) AckMessages(ctx context.Context, req *threadsv1.AckMessagesRequest, opts ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error) {
	if _, err := outgoingIdentityID(ctx); err != nil {
		return nil, err
	}

	participantID := req.GetParticipantId()
	if participantID == "" {
		return nil, status.Error(codes.InvalidArgument, "participant_id is required")
	}

	acked := int32(0)
	t.mu.Lock()
	for _, state := range t.threads {
		if !threadHasParticipant(state.thread, participantID) {
			continue
		}
		if _, ok := state.acked[participantID]; !ok {
			state.acked[participantID] = make(map[string]bool)
		}
		for _, msgID := range req.GetMessageIds() {
			if !state.acked[participantID][msgID] {
				state.acked[participantID][msgID] = true
				acked++
			}
		}
	}
	t.mu.Unlock()

	return &threadsv1.AckMessagesResponse{AckedCount: acked}, nil
}

func outgoingIdentityID(ctx context.Context) (string, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	identityID := md.Get("x-identity-id")
	if len(identityID) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing identity id")
	}
	identityType := md.Get("x-identity-type")
	if len(identityType) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing identity type")
	}
	return identityID[0], nil
}

func threadHasParticipant(thread *threadsv1.Thread, participantID string) bool {
	for _, participant := range thread.GetParticipants() {
		if participant.GetId() == participantID {
			return true
		}
	}
	return false
}

func parsePageOffset(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, status.Error(codes.InvalidArgument, "invalid page token")
	}
	return offset, nil
}

func paginateSlice[T any](items []T, start, pageSize int) ([]T, string) {
	if start >= len(items) {
		return []T{}, ""
	}
	if pageSize <= 0 {
		pageSize = len(items)
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	nextToken := ""
	if end < len(items) {
		nextToken = strconv.Itoa(end)
	}
	return items[start:end], nextToken
}

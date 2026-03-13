package e2e

import (
	"context"
	"sort"
	"strconv"
	"sync"

	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeThreadsServer struct {
	threadsv1.UnimplementedThreadsServiceServer

	mu       sync.Mutex
	threads  []*threadsv1.Thread
	messages []*threadsv1.Message
	acked    map[string]map[string]struct{}
}

func newFakeThreadsServer() *fakeThreadsServer {
	return &fakeThreadsServer{
		acked: make(map[string]map[string]struct{}),
	}
}

func (f *fakeThreadsServer) CreateThread(_ context.Context, req *threadsv1.CreateThreadRequest) (*threadsv1.CreateThreadResponse, error) {
	if len(req.GetParticipantIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "participant_ids must not be empty")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	now := timestamppb.Now()
	participants := make([]*threadsv1.Participant, len(req.GetParticipantIds()))
	for i, pid := range req.GetParticipantIds() {
		participants[i] = &threadsv1.Participant{
			Id:       pid,
			JoinedAt: now,
		}
	}

	thread := &threadsv1.Thread{
		Id:           uuid.NewString(),
		Participants: participants,
		Status:       threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	f.threads = append(f.threads, thread)

	return &threadsv1.CreateThreadResponse{Thread: thread}, nil
}

func (f *fakeThreadsServer) GetThreads(_ context.Context, req *threadsv1.GetThreadsRequest) (*threadsv1.GetThreadsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var matching []*threadsv1.Thread
	for _, thread := range f.threads {
		for _, p := range thread.GetParticipants() {
			if p.GetId() == req.GetParticipantId() {
				matching = append(matching, thread)
				break
			}
		}
	}

	sort.Slice(matching, func(i, j int) bool {
		return matching[i].GetCreatedAt().AsTime().After(matching[j].GetCreatedAt().AsTime())
	})

	start := 0
	if req.GetPageToken() != "" {
		idx, err := strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		start = idx
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 50
	}

	end := start + pageSize
	if end > len(matching) {
		end = len(matching)
	}

	var nextPageToken string
	if end < len(matching) {
		nextPageToken = strconv.Itoa(end)
	}

	return &threadsv1.GetThreadsResponse{
		Threads:       matching[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func (f *fakeThreadsServer) GetMessages(_ context.Context, req *threadsv1.GetMessagesRequest) (*threadsv1.GetMessagesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var matching []*threadsv1.Message
	for _, msg := range f.messages {
		if msg.GetThreadId() == req.GetThreadId() {
			matching = append(matching, msg)
		}
	}

	start := 0
	if req.GetPageToken() != "" {
		idx, err := strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		start = idx
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 50
	}

	end := start + pageSize
	if end > len(matching) {
		end = len(matching)
	}

	var nextPageToken string
	if end < len(matching) {
		nextPageToken = strconv.Itoa(end)
	}

	return &threadsv1.GetMessagesResponse{
		Messages:      matching[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func (f *fakeThreadsServer) SendMessage(_ context.Context, req *threadsv1.SendMessageRequest) (*threadsv1.SendMessageResponse, error) {
	if req.GetThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "thread_id is required")
	}
	if req.GetBody() == "" && len(req.GetFileIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "body or file_ids must be provided")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var thread *threadsv1.Thread
	for _, t := range f.threads {
		if t.GetId() == req.GetThreadId() {
			thread = t
			break
		}
	}
	if thread == nil {
		return nil, status.Errorf(codes.NotFound, "thread %s not found", req.GetThreadId())
	}

	now := timestamppb.Now()
	msg := &threadsv1.Message{
		Id:        uuid.NewString(),
		ThreadId:  req.GetThreadId(),
		SenderId:  req.GetSenderId(),
		Body:      req.GetBody(),
		FileIds:   req.GetFileIds(),
		CreatedAt: now,
	}
	f.messages = append(f.messages, msg)

	return &threadsv1.SendMessageResponse{Message: msg}, nil
}

func (f *fakeThreadsServer) GetUnackedMessages(_ context.Context, req *threadsv1.GetUnackedMessagesRequest) (*threadsv1.GetUnackedMessagesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var unacked []*threadsv1.Message
	for _, msg := range f.messages {
		if msg.GetSenderId() == req.GetParticipantId() {
			continue
		}
		ackedBy := f.acked[msg.GetId()]
		if _, ok := ackedBy[req.GetParticipantId()]; !ok {
			unacked = append(unacked, msg)
		}
	}

	start := 0
	if req.GetPageToken() != "" {
		idx, err := strconv.Atoi(req.GetPageToken())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		start = idx
	}

	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 100
	}

	end := start + pageSize
	if end > len(unacked) {
		end = len(unacked)
	}

	var nextPageToken string
	if end < len(unacked) {
		nextPageToken = strconv.Itoa(end)
	}

	return &threadsv1.GetUnackedMessagesResponse{
		Messages:      unacked[start:end],
		NextPageToken: nextPageToken,
	}, nil
}

func (f *fakeThreadsServer) AckMessages(_ context.Context, req *threadsv1.AckMessagesRequest) (*threadsv1.AckMessagesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var ackedCount int32
	for _, msgID := range req.GetMessageIds() {
		if f.acked[msgID] == nil {
			f.acked[msgID] = make(map[string]struct{})
		}
		if _, already := f.acked[msgID][req.GetParticipantId()]; !already {
			f.acked[msgID][req.GetParticipantId()] = struct{}{}
			ackedCount++
		}
	}

	return &threadsv1.AckMessagesResponse{AckedCount: ackedCount}, nil
}

package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	identityv1 "github.com/agynio/chat/gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/chat/gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/chat/internal/identity"
	"github.com/agynio/chat/internal/store"
)

type Server struct {
	chatv1.UnimplementedChatServiceServer
	threads  threadsv1.ThreadsServiceClient
	runners  runnersClient
	identity identityClient
	store    chatStore
}

const unackedPageSize = 100

const threadsPageSize = 100

const maxThreadsPages = 10

const latestWorkloadPageSize = 1

const workloadsConcurrencyLimit = 8

type chatStore interface {
	CreateChat(ctx context.Context, threadID, organizationID uuid.UUID) (store.Chat, error)
	GetChat(ctx context.Context, threadID uuid.UUID) (store.Chat, error)
	UpdateChat(ctx context.Context, threadID uuid.UUID, params store.UpdateChatParams) (store.Chat, error)
	ListChats(ctx context.Context, organizationID uuid.UUID, filter store.ChatListFilter, pageSize int32, cursor *store.PageCursor) (store.ChatListResult, error)
}

type chatEntry struct {
	chat   store.Chat
	thread *threadsv1.Thread
}

type chatActivity struct {
	status            chatv1.ChatActivityStatus
	activeWorkloadIDs []string
}

type runnersClient interface {
	ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, opts ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error)
}

type identityClient interface {
	BatchGetIdentityTypes(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error)
}

func New(threads threadsv1.ThreadsServiceClient, runners runnersClient, identity identityClient, store chatStore) *Server {
	return &Server{threads: threads, runners: runners, identity: identity, store: store}
}

func (s *Server) CreateChat(ctx context.Context, req *chatv1.CreateChatRequest) (*chatv1.CreateChatResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}
	threadsCtx := identity.AppendToOutgoingContext(ctx, id)

	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	if len(req.GetParticipantIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "participant_ids must not be empty")
	}

	participantIDs := make([]string, 0, len(req.GetParticipantIds()))
	for _, pid := range req.GetParticipantIds() {
		if pid == id.IdentityID {
			continue
		}
		participantIDs = append(participantIDs, pid)
	}
	participants := make([]*threadsv1.ParticipantIdentifier, len(participantIDs))
	for i, pid := range participantIDs {
		participants[i] = &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: pid},
		}
	}
	orgIDValue := organizationID.String()

	resp, err := s.threads.CreateThread(threadsCtx, &threadsv1.CreateThreadRequest{
		Participants:   participants,
		OrganizationId: &orgIDValue,
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	threadID, err := uuid.Parse(resp.GetThread().GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "threads returned invalid thread id: %v", err)
	}
	storedChat, err := s.store.CreateChat(ctx, threadID, organizationID)
	if err != nil {
		log.Printf("chat: store thread %s for org %s failed (returning thread only): %v", threadID, organizationID, err)
	}
	chatStatus := chatv1.ChatStatus_CHAT_STATUS_OPEN
	var summary *string
	if err == nil {
		chatStatus = stringToChatStatus(storedChat.Status)
		summary = storedChat.Summary
	}

	return &chatv1.CreateChatResponse{
		Chat: threadToChat(resp.GetThread(), organizationID.String(), chatStatus, summary),
	}, nil
}

func (s *Server) GetChats(ctx context.Context, req *chatv1.GetChatsRequest) (*chatv1.GetChatsResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}
	threadsCtx := identity.AppendToOutgoingContext(ctx, id)

	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	cursor, err := decodePageCursor(req.GetPageToken())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page_token: %v", err)
	}

	pageSize := store.NormalizePageSize(req.GetPageSize())
	filter := store.ChatListFilter{}
	if req.GetStatus() != chatv1.ChatStatus_CHAT_STATUS_UNSPECIFIED {
		statusValue := chatStatusToString(req.GetStatus())
		filter.Status = &statusValue
	}
	countsResp, err := s.threads.GetUnackedMessageCounts(threadsCtx, &threadsv1.GetUnackedMessageCountsRequest{
		ParticipantId: id.IdentityID,
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}
	unreadCounts := countsResp.GetCountsByThreadId()

	chatEntries := make([]chatEntry, 0, int(pageSize))
	var nextCursor *store.PageCursor
	for len(chatEntries) < int(pageSize) {
		remaining := pageSize - int32(len(chatEntries))
		result, err := s.store.ListChats(ctx, organizationID, filter, remaining, cursor)
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

		threadsByID, err := s.fetchThreads(threadsCtx, id.IdentityID, threadIDs)
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
			chatEntries = append(chatEntries, chatEntry{chat: chat, thread: thread})
		}

		nextCursor = result.NextCursor
		if len(chatEntries) >= int(pageSize) || result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
	}

	participantIDs := make(map[string]struct{})
	threadsForActivity := make([]*threadsv1.Thread, 0, len(chatEntries))
	for _, entry := range chatEntries {
		thread := entry.thread
		threadsForActivity = append(threadsForActivity, thread)
		for _, participant := range thread.GetParticipants() {
			participantID := participant.GetId()
			if participantID == "" {
				continue
			}
			participantIDs[participantID] = struct{}{}
		}
	}

	identityTypes := map[string]identityv1.IdentityType{}
	if len(participantIDs) > 0 {
		ids := make([]string, 0, len(participantIDs))
		for pid := range participantIDs {
			ids = append(ids, pid)
		}
		resolved, err := s.fetchIdentityTypes(threadsCtx, ids)
		if err != nil {
			log.Printf("chat: identity types lookup failed: %v", err)
		} else {
			identityTypes = resolved
		}
	}

	activityByThread := s.fetchChatActivities(threadsCtx, threadsForActivity, identityTypes)
	chats := make([]*chatv1.Chat, 0, len(chatEntries))
	for _, entry := range chatEntries {
		threadChat := threadToChat(entry.thread, entry.chat.OrganizationID.String(), stringToChatStatus(entry.chat.Status), entry.chat.Summary)
		threadID := entry.thread.GetId()
		threadChat.UnreadCount = unreadCounts[threadID]
		activity, ok := activityByThread[threadID]
		if !ok {
			activity = chatActivity{status: chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_UNSPECIFIED}
		}
		threadChat.ActivityStatus = activity.status
		threadChat.ActiveWorkloadIds = activity.activeWorkloadIDs
		chats = append(chats, threadChat)
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

func (s *Server) UpdateChat(ctx context.Context, req *chatv1.UpdateChatRequest) (*chatv1.UpdateChatResponse, error) {
	id, err := identity.FromContext(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "identity: %v", err)
	}

	if req.GetChatId() == "" {
		return nil, status.Error(codes.InvalidArgument, "chat_id is required")
	}
	threadID, err := parseUUID(req.GetChatId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "chat_id: %v", err)
	}

	params := store.UpdateChatParams{}
	if req.GetStatus() != chatv1.ChatStatus_CHAT_STATUS_UNSPECIFIED {
		statusValue := chatStatusToString(req.GetStatus())
		params.Status = &statusValue
	}
	if req.Summary != nil {
		summaryValue := req.GetSummary()
		if summaryValue == "" {
			params.ClearSummary = true
		} else {
			params.Summary = &summaryValue
		}
	}

	threadsCtx := identity.AppendToOutgoingContext(ctx, id)
	threadsByID, err := s.fetchThreads(threadsCtx, id.IdentityID, []uuid.UUID{threadID})
	if err != nil {
		return nil, mapThreadsError(err)
	}
	thread, ok := threadsByID[threadID]
	if !ok || !threadHasParticipant(thread, id.IdentityID) {
		return nil, status.Error(codes.NotFound, "chat not found")
	}

	updatedChat, err := s.store.UpdateChat(ctx, threadID, params)
	if err != nil {
		return nil, toStatusError(err)
	}

	return &chatv1.UpdateChatResponse{
		Chat: threadToChat(thread, updatedChat.OrganizationID.String(), stringToChatStatus(updatedChat.Status), updatedChat.Summary),
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

	threadsCtx := identity.AppendToOutgoingContext(ctx, id)
	msgResp, err := s.threads.GetMessages(threadsCtx, &threadsv1.GetMessagesRequest{
		ThreadId:  req.GetChatId(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	})
	if err != nil {
		return nil, mapThreadsError(err)
	}

	unreadCount, err := s.countUnread(threadsCtx, id.IdentityID, req.GetChatId())
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

	threadsCtx := identity.AppendToOutgoingContext(ctx, id)
	resp, err := s.threads.SendMessage(threadsCtx, &threadsv1.SendMessageRequest{
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

	threadsCtx := identity.AppendToOutgoingContext(ctx, id)
	messageIDs, err := s.listUnackedMessageIDs(threadsCtx, id.IdentityID, req.GetChatId())
	if err != nil {
		return nil, mapThreadsError(err)
	}
	if len(messageIDs) == 0 {
		return &chatv1.MarkAsReadResponse{ReadCount: 0}, nil
	}
	resp, err := s.threads.AckMessages(threadsCtx, &threadsv1.AckMessagesRequest{
		ParticipantId: id.IdentityID,
		MessageIds:    messageIDs,
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

	threadID := chatID
	for {
		resp, err := s.threads.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{
			ParticipantId: participantID,
			ThreadId:      &threadID,
			PageSize:      unackedPageSize,
			PageToken:     pageToken,
		})
		if err != nil {
			return 0, err
		}

		for range resp.GetMessages() {
			count++
		}

		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	return count, nil
}

func (s *Server) listUnackedMessageIDs(ctx context.Context, participantID, chatID string) ([]string, error) {
	var messageIDs []string
	var pageToken string

	threadID := chatID
	for {
		resp, err := s.threads.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{
			ParticipantId: participantID,
			ThreadId:      &threadID,
			PageSize:      unackedPageSize,
			PageToken:     pageToken,
		})
		if err != nil {
			return nil, err
		}

		for _, message := range resp.GetMessages() {
			messageIDs = append(messageIDs, message.GetId())
		}

		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}

	return messageIDs, nil
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

type workloadSummary struct {
	latestStatus      runnersv1.WorkloadStatus
	hasLatest         bool
	activeWorkloadIDs []string
}

type threadAgentPair struct {
	threadID string
	agentID  string
}

type workloadResult struct {
	threadID string
	agentID  string
	summary  workloadSummary
	err      error
}

type workloadAggregate struct {
	activeWorkloadIDs []string
	running           bool
	pending           bool
}

func (s *Server) fetchChatActivities(ctx context.Context, threads []*threadsv1.Thread, identityTypes map[string]identityv1.IdentityType) map[string]chatActivity {
	activities := make(map[string]chatActivity, len(threads))
	if len(threads) == 0 {
		return activities
	}

	pairs := make([]threadAgentPair, 0)
	for _, thread := range threads {
		threadID := thread.GetId()
		if threadID == "" {
			log.Printf("chat: thread missing id")
			continue
		}
		if thread.GetStatus() == threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED {
			activities[threadID] = chatActivity{status: chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_UNSPECIFIED}
			continue
		}
		agentIDs := agentParticipantIDs(thread, identityTypes)
		if len(agentIDs) == 0 {
			activities[threadID] = chatActivity{status: chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_UNSPECIFIED}
			continue
		}
		for _, agentID := range agentIDs {
			pairs = append(pairs, threadAgentPair{threadID: threadID, agentID: agentID})
		}
	}

	if len(pairs) == 0 {
		return activities
	}

	results := make(chan workloadResult, len(pairs))
	sem := make(chan struct{}, workloadsConcurrencyLimit)
	var wg sync.WaitGroup
	for _, pair := range pairs {
		wg.Add(1)
		go func(pair threadAgentPair) {
			defer wg.Done()
			sem <- struct{}{}
			summary, err := s.workloadSummaryForAgent(ctx, pair.threadID, pair.agentID)
			<-sem
			results <- workloadResult{threadID: pair.threadID, agentID: pair.agentID, summary: summary, err: err}
		}(pair)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	aggregates := make(map[string]*workloadAggregate)
	for result := range results {
		agg, ok := aggregates[result.threadID]
		if !ok {
			agg = &workloadAggregate{}
			aggregates[result.threadID] = agg
		}
		if result.err != nil {
			log.Printf("chat: runners workloads failed: thread_id=%s agent_id=%s err=%v", result.threadID, result.agentID, result.err)
			continue
		}
		agg.activeWorkloadIDs = append(agg.activeWorkloadIDs, result.summary.activeWorkloadIDs...)
		if !result.summary.hasLatest {
			continue
		}
		switch result.summary.latestStatus {
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
			agg.running = true
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
			runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
			agg.pending = true
		case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
			// finished
		}
	}

	for _, thread := range threads {
		threadID := thread.GetId()
		if threadID == "" {
			continue
		}
		if _, ok := activities[threadID]; ok {
			continue
		}
		agg := aggregates[threadID]
		if agg == nil {
			activities[threadID] = chatActivity{status: chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_FINISHED}
			continue
		}
		status := chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_FINISHED
		if agg.running {
			status = chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_RUNNING
		} else if agg.pending {
			status = chatv1.ChatActivityStatus_CHAT_ACTIVITY_STATUS_PENDING
		}
		activities[threadID] = chatActivity{status: status, activeWorkloadIDs: agg.activeWorkloadIDs}
	}

	return activities
}

func agentParticipantIDs(thread *threadsv1.Thread, identityTypes map[string]identityv1.IdentityType) []string {
	agentIDs := make([]string, 0)
	for _, participant := range thread.GetParticipants() {
		if participant.GetPassive() {
			continue
		}
		participantID := participant.GetId()
		if participantID == "" {
			continue
		}
		if identityTypes[participantID] != identityv1.IdentityType_IDENTITY_TYPE_AGENT {
			continue
		}
		agentIDs = append(agentIDs, participantID)
	}
	return agentIDs
}

func (s *Server) workloadSummaryForAgent(ctx context.Context, threadID, agentID string) (workloadSummary, error) {
	summary := workloadSummary{}
	resp, err := s.runners.ListWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{
		ThreadId: threadID,
		AgentId:  &agentID,
		PageSize: latestWorkloadPageSize,
	})
	if err != nil {
		return summary, err
	}
	workloads := resp.GetWorkloads()
	if len(workloads) == 0 {
		return summary, nil
	}
	workload := workloads[0]
	if workload == nil {
		return summary, fmt.Errorf("workload missing")
	}
	status := workload.GetStatus()
	if err := validateWorkloadStatus(status); err != nil {
		return summary, err
	}
	summary.latestStatus = status
	summary.hasLatest = true
	if isActiveWorkloadStatus(status) {
		workloadID, err := workloadID(workload)
		if err != nil {
			return summary, err
		}
		summary.activeWorkloadIDs = append(summary.activeWorkloadIDs, workloadID)
	}
	return summary, nil
}

func validateWorkloadStatus(status runnersv1.WorkloadStatus) error {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return nil
	default:
		return fmt.Errorf("unsupported workload status %s", status)
	}
}

func isActiveWorkloadStatus(status runnersv1.WorkloadStatus) bool {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return true
	default:
		return false
	}
}

func workloadID(workload *runnersv1.Workload) (string, error) {
	meta := workload.GetMeta()
	if meta == nil {
		return "", fmt.Errorf("workload metadata missing")
	}
	if meta.GetId() == "" {
		return "", fmt.Errorf("workload id missing")
	}
	return meta.GetId(), nil
}

func (s *Server) fetchIdentityTypes(ctx context.Context, ids []string) (map[string]identityv1.IdentityType, error) {
	resp, err := s.identity.BatchGetIdentityTypes(ctx, &identityv1.BatchGetIdentityTypesRequest{
		IdentityIds: ids,
	})
	if err != nil {
		return nil, err
	}

	identityTypes := make(map[string]identityv1.IdentityType, len(resp.GetEntries()))
	for _, entry := range resp.GetEntries() {
		if entry == nil {
			log.Printf("chat: identity type entry missing")
			continue
		}
		identityID := entry.GetIdentityId()
		if identityID == "" {
			log.Printf("chat: identity type entry missing identity_id")
			continue
		}
		identityTypes[identityID] = entry.GetIdentityType()
	}
	return identityTypes, nil
}

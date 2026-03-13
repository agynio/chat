package e2e

import (
	"context"
	"testing"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	userID   = "aaaaaaaa-0000-0000-0000-000000000001"
	otherID  = "aaaaaaaa-0000-0000-0000-000000000002"
	thirdID  = "aaaaaaaa-0000-0000-0000-000000000003"
	userType = "user"
	tenantID = "tttttttt-0000-0000-0000-000000000001"
)

func TestCreateChat(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{otherID},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetChat())
	require.NotEmpty(t, resp.GetChat().GetId())

	participantIDs := participantIDsFromChat(resp.GetChat())
	assert.Contains(t, participantIDs, userID)
	assert.Contains(t, participantIDs, otherID)
	assert.Len(t, participantIDs, 2)
}

func TestCreateChat_CallerDeduplication(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{userID, otherID},
	})
	require.NoError(t, err)

	participantIDs := participantIDsFromChat(resp.GetChat())
	assert.Contains(t, participantIDs, userID)
	assert.Contains(t, participantIDs, otherID)
	assert.Len(t, participantIDs, 2)
}

func TestCreateChat_EmptyParticipants(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChat_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.CreateChat(context.Background(), &chatv1.CreateChatRequest{
		ParticipantIds: []string{otherID},
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestGetChats(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{otherID}})
	require.NoError(t, err)
	_, err = env.client.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{thirdID}})
	require.NoError(t, err)

	resp, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.GetChats(), 2)
}

func TestGetChats_Pagination(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	for _, pid := range []string{otherID, thirdID, "aaaaaaaa-0000-0000-0000-000000000004"} {
		_, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{ParticipantIds: []string{pid}})
		require.NoError(t, err)
	}

	page1, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{PageSize: 2})
	require.NoError(t, err)
	assert.Len(t, page1.GetChats(), 2)
	assert.NotEmpty(t, page1.GetNextPageToken())

	page2, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{
		PageSize:  2,
		PageToken: page1.GetNextPageToken(),
	})
	require.NoError(t, err)
	assert.Len(t, page2.GetChats(), 1)
	assert.Empty(t, page2.GetNextPageToken())
}

func TestGetChats_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.GetChats(context.Background(), &chatv1.GetChatsRequest{})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestGetMessages(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, ctx, otherID)

	_, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "hello"})
	require.NoError(t, err)
	_, err = env.client.SendMessage(ctx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "world"})
	require.NoError(t, err)

	resp, err := env.client.GetMessages(ctx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Len(t, resp.GetMessages(), 2)
	assert.Equal(t, "hello", resp.GetMessages()[0].GetBody())
	assert.Equal(t, "world", resp.GetMessages()[1].GetBody())
}

func TestGetMessages_UnreadCount(t *testing.T) {
	env := setupEnv(t)
	senderCtx := ctxWithIdentity(otherID, userType, tenantID)
	readerCtx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, readerCtx, otherID)

	_, err := env.client.SendMessage(senderCtx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "msg1"})
	require.NoError(t, err)
	_, err = env.client.SendMessage(senderCtx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "msg2"})
	require.NoError(t, err)

	resp, err := env.client.GetMessages(readerCtx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.GetUnreadCount())
}

func TestGetMessages_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.GetMessages(ctx, &chatv1.GetMessagesRequest{})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestGetMessages_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.GetMessages(context.Background(), &chatv1.GetMessagesRequest{ChatId: "some-id"})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestSendMessage(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, ctx, otherID)

	resp, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{
		ChatId: chat.GetId(),
		Body:   "hello",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetMessage())
	assert.Equal(t, chat.GetId(), resp.GetMessage().GetChatId())
	assert.Equal(t, userID, resp.GetMessage().GetSenderId())
	assert.Equal(t, "hello", resp.GetMessage().GetBody())
	assert.NotEmpty(t, resp.GetMessage().GetId())
}

func TestSendMessage_WithFileIDs(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, ctx, otherID)

	resp, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{
		ChatId:  chat.GetId(),
		FileIds: []string{"file-uuid-1", "file-uuid-2"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"file-uuid-1", "file-uuid-2"}, resp.GetMessage().GetFileIds())
}

func TestSendMessage_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{Body: "hello"})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestSendMessage_MissingBodyAndFiles(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, ctx, otherID)

	_, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{ChatId: chat.GetId()})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestSendMessage_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.SendMessage(context.Background(), &chatv1.SendMessageRequest{
		ChatId: "some-id",
		Body:   "hello",
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func TestMarkAsRead(t *testing.T) {
	env := setupEnv(t)
	senderCtx := ctxWithIdentity(otherID, userType, tenantID)
	readerCtx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, readerCtx, otherID)

	msg1, err := env.client.SendMessage(senderCtx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "msg1"})
	require.NoError(t, err)
	msg2, err := env.client.SendMessage(senderCtx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "msg2"})
	require.NoError(t, err)

	resp, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg1.GetMessage().GetId(), msg2.GetMessage().GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.GetReadCount())

	msgResp, err := env.client.GetMessages(readerCtx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Equal(t, int32(0), msgResp.GetUnreadCount())
}

func TestMarkAsRead_Idempotent(t *testing.T) {
	env := setupEnv(t)
	senderCtx := ctxWithIdentity(otherID, userType, tenantID)
	readerCtx := ctxWithIdentity(userID, userType, tenantID)

	chat := createChat(t, env, readerCtx, otherID)

	msg, err := env.client.SendMessage(senderCtx, &chatv1.SendMessageRequest{ChatId: chat.GetId(), Body: "msg"})
	require.NoError(t, err)

	resp1, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg.GetMessage().GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp1.GetReadCount())

	resp2, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg.GetMessage().GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp2.GetReadCount())
}

func TestMarkAsRead_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.MarkAsRead(ctx, &chatv1.MarkAsReadRequest{
		MessageIds: []string{"some-msg-id"},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestMarkAsRead_EmptyMessageIDs(t *testing.T) {
	env := setupEnv(t)
	ctx := ctxWithIdentity(userID, userType, tenantID)

	_, err := env.client.MarkAsRead(ctx, &chatv1.MarkAsReadRequest{
		ChatId:     "some-chat-id",
		MessageIds: []string{},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestMarkAsRead_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.MarkAsRead(context.Background(), &chatv1.MarkAsReadRequest{
		ChatId:     "some-chat-id",
		MessageIds: []string{"some-msg-id"},
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

func createChat(t *testing.T, env *testEnv, ctx context.Context, otherParticipantID string) *chatv1.Chat {
	t.Helper()
	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{otherParticipantID},
	})
	require.NoError(t, err)
	return resp.GetChat()
}

func participantIDsFromChat(chat *chatv1.Chat) []string {
	ids := make([]string, len(chat.GetParticipants()))
	for i, p := range chat.GetParticipants() {
		ids[i] = p.GetId()
	}
	return ids
}

func requireStatusCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error, got: %v", err)
	assert.Equal(t, code, st.Code(), "expected code %s, got %s: %s", code, st.Code(), st.Message())
}

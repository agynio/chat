package e2e

import (
	"context"
	"testing"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// ---------------------------------------------------------------------------
// CreateChat
// ---------------------------------------------------------------------------

func TestCreateChat(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()
	otherID := uniqueID()

	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{otherID},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetChat())
	assert.NotEmpty(t, resp.GetChat().GetId())
	assert.NotNil(t, resp.GetChat().GetCreatedAt())
	assert.NotNil(t, resp.GetChat().GetUpdatedAt())
	assert.Len(t, resp.GetChat().GetParticipants(), 2)
}

func TestCreateChat_CallerAutoAdded(t *testing.T) {
	env := setupEnv(t)
	callerID, ctx := testIdentity()
	otherID := uniqueID()

	chat := createChat(t, env, ctx, otherID)
	pids := participantIDsFromChat(chat)
	assert.Contains(t, pids, callerID)
	assert.Contains(t, pids, otherID)
}

func TestCreateChat_CallerDeduplication(t *testing.T) {
	env := setupEnv(t)
	callerID, ctx := testIdentity()
	otherID := uniqueID()

	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{callerID, otherID},
	})
	require.NoError(t, err)

	pids := participantIDsFromChat(resp.GetChat())
	assert.Contains(t, pids, callerID)
	assert.Contains(t, pids, otherID)
	assert.Len(t, pids, 2)
}

func TestCreateChat_EmptyParticipants(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	_, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: []string{},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestCreateChat_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.CreateChat(context.Background(), &chatv1.CreateChatRequest{
		ParticipantIds: []string{uniqueID()},
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

// ---------------------------------------------------------------------------
// GetChats
// ---------------------------------------------------------------------------

func TestGetChats(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	createChat(t, env, ctx, uniqueID())
	createChat(t, env, ctx, uniqueID())

	resp, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(resp.GetChats()), 2)
}

func TestGetChats_Pagination(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	createChat(t, env, ctx, uniqueID())
	createChat(t, env, ctx, uniqueID())
	createChat(t, env, ctx, uniqueID())

	page1, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{PageSize: 2})
	require.NoError(t, err)
	assert.Len(t, page1.GetChats(), 2)
	assert.NotEmpty(t, page1.GetNextPageToken())

	page2, err := env.client.GetChats(ctx, &chatv1.GetChatsRequest{
		PageSize:  2,
		PageToken: page1.GetNextPageToken(),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(page2.GetChats()), 1)
}

func TestGetChats_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.GetChats(context.Background(), &chatv1.GetChatsRequest{})
	requireStatusCode(t, err, codes.Unauthenticated)
}

// ---------------------------------------------------------------------------
// GetMessages
// ---------------------------------------------------------------------------

func TestGetMessages(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	chat := createChat(t, env, ctx, uniqueID())
	sendMessage(t, env, ctx, chat.GetId(), "hello")
	sendMessage(t, env, ctx, chat.GetId(), "world")

	resp, err := env.client.GetMessages(ctx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Len(t, resp.GetMessages(), 2)
	assert.Equal(t, "hello", resp.GetMessages()[0].GetBody())
	assert.Equal(t, "world", resp.GetMessages()[1].GetBody())
}

func TestGetMessages_UnreadCount(t *testing.T) {
	env := setupEnv(t)
	_, readerCtx := testIdentity()
	otherID := uniqueID()
	tenantID := uniqueID()
	senderCtx := ctxWithIdentity(otherID, "user", tenantID)

	chat := createChat(t, env, readerCtx, otherID)

	sendMessage(t, env, senderCtx, chat.GetId(), "msg1")
	sendMessage(t, env, senderCtx, chat.GetId(), "msg2")

	resp, err := env.client.GetMessages(readerCtx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.GetUnreadCount())
}

func TestGetMessages_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	_, err := env.client.GetMessages(ctx, &chatv1.GetMessagesRequest{})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestGetMessages_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.GetMessages(context.Background(), &chatv1.GetMessagesRequest{ChatId: uniqueID()})
	requireStatusCode(t, err, codes.Unauthenticated)
}

// ---------------------------------------------------------------------------
// SendMessage
// ---------------------------------------------------------------------------

func TestSendMessage(t *testing.T) {
	env := setupEnv(t)
	callerID, ctx := testIdentity()

	chat := createChat(t, env, ctx, uniqueID())

	resp, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{
		ChatId: chat.GetId(),
		Body:   "hello",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetMessage())
	assert.Equal(t, chat.GetId(), resp.GetMessage().GetChatId())
	assert.Equal(t, callerID, resp.GetMessage().GetSenderId())
	assert.Equal(t, "hello", resp.GetMessage().GetBody())
	assert.NotEmpty(t, resp.GetMessage().GetId())
	assert.NotNil(t, resp.GetMessage().GetCreatedAt())
}

func TestSendMessage_WithFileIDs(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	chat := createChat(t, env, ctx, uniqueID())

	resp, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{
		ChatId:  chat.GetId(),
		FileIds: []string{uniqueID(), uniqueID()},
	})
	require.NoError(t, err)
	assert.Len(t, resp.GetMessage().GetFileIds(), 2)
}

func TestSendMessage_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	_, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{Body: "hello"})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestSendMessage_MissingBodyAndFiles(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	chat := createChat(t, env, ctx, uniqueID())

	_, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{ChatId: chat.GetId()})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestSendMessage_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.SendMessage(context.Background(), &chatv1.SendMessageRequest{
		ChatId: uniqueID(),
		Body:   "hello",
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

// ---------------------------------------------------------------------------
// MarkAsRead
// ---------------------------------------------------------------------------

func TestMarkAsRead(t *testing.T) {
	env := setupEnv(t)
	_, readerCtx := testIdentity()
	otherID := uniqueID()
	tenantID := uniqueID()
	senderCtx := ctxWithIdentity(otherID, "user", tenantID)

	chat := createChat(t, env, readerCtx, otherID)

	msg1 := sendMessage(t, env, senderCtx, chat.GetId(), "msg1")
	msg2 := sendMessage(t, env, senderCtx, chat.GetId(), "msg2")

	resp, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg1.GetId(), msg2.GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.GetReadCount())

	msgResp, err := env.client.GetMessages(readerCtx, &chatv1.GetMessagesRequest{ChatId: chat.GetId()})
	require.NoError(t, err)
	assert.Equal(t, int32(0), msgResp.GetUnreadCount())
}

func TestMarkAsRead_Idempotent(t *testing.T) {
	env := setupEnv(t)
	_, readerCtx := testIdentity()
	otherID := uniqueID()
	tenantID := uniqueID()
	senderCtx := ctxWithIdentity(otherID, "user", tenantID)

	chat := createChat(t, env, readerCtx, otherID)
	msg := sendMessage(t, env, senderCtx, chat.GetId(), "msg")

	resp1, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg.GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp1.GetReadCount())

	resp2, err := env.client.MarkAsRead(readerCtx, &chatv1.MarkAsReadRequest{
		ChatId:     chat.GetId(),
		MessageIds: []string{msg.GetId()},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp2.GetReadCount())
}

func TestMarkAsRead_MissingChatID(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	_, err := env.client.MarkAsRead(ctx, &chatv1.MarkAsReadRequest{
		MessageIds: []string{uniqueID()},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestMarkAsRead_EmptyMessageIDs(t *testing.T) {
	env := setupEnv(t)
	_, ctx := testIdentity()

	_, err := env.client.MarkAsRead(ctx, &chatv1.MarkAsReadRequest{
		ChatId:     uniqueID(),
		MessageIds: []string{},
	})
	requireStatusCode(t, err, codes.InvalidArgument)
}

func TestMarkAsRead_MissingIdentity(t *testing.T) {
	env := setupEnv(t)

	_, err := env.client.MarkAsRead(context.Background(), &chatv1.MarkAsReadRequest{
		ChatId:     uniqueID(),
		MessageIds: []string{uniqueID()},
	})
	requireStatusCode(t, err, codes.Unauthenticated)
}

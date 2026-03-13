package e2e

import (
	"context"
	"os"
	"testing"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testEnv struct {
	client chatv1.ChatServiceClient
	conn   *grpc.ClientConn
}

func setupEnv(t *testing.T) *testEnv {
	t.Helper()

	addr := os.Getenv("CHAT_ADDRESS")
	if addr == "" {
		addr = "localhost:50051"
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial chat: %v", err)
	}

	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		client: chatv1.NewChatServiceClient(conn),
		conn:   conn,
	}
}

// uniqueID returns a fresh UUID string for test isolation.
func uniqueID() string {
	return uuid.NewString()
}

// testIdentity returns a unique identity ID and a context with identity metadata.
func testIdentity() (string, context.Context) {
	id := uniqueID()
	return id, ctxWithIdentity(id, "user", uniqueID())
}

func ctxWithIdentity(identityID, identityType, tenantID string) context.Context {
	md := metadata.Pairs(
		"x-identity-id", identityID,
		"x-identity-type", identityType,
		"x-tenant-id", tenantID,
	)
	return metadata.NewOutgoingContext(context.Background(), md)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func createChat(t *testing.T, env *testEnv, ctx context.Context, participantIDs ...string) *chatv1.Chat {
	t.Helper()
	resp, err := env.client.CreateChat(ctx, &chatv1.CreateChatRequest{
		ParticipantIds: participantIDs,
	})
	require.NoError(t, err)
	return resp.GetChat()
}

func sendMessage(t *testing.T, env *testEnv, ctx context.Context, chatID, body string) *chatv1.ChatMessage {
	t.Helper()
	resp, err := env.client.SendMessage(ctx, &chatv1.SendMessageRequest{
		ChatId: chatID,
		Body:   body,
	})
	require.NoError(t, err)
	return resp.GetMessage()
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

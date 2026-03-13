package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/agynio/chat/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type testEnv struct {
	client      chatv1.ChatServiceClient
	fakeThreads *fakeThreadsServer

	chatConn    *grpc.ClientConn
	chatGRPC    *grpc.Server
	threadsGRPC *grpc.Server
}

func setupEnv(t *testing.T) *testEnv {
	t.Helper()

	fake := newFakeThreadsServer()
	threadsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen threads: %v", err)
	}
	threadsGRPC := grpc.NewServer()
	threadsv1.RegisterThreadsServiceServer(threadsGRPC, fake)
	go func() { _ = threadsGRPC.Serve(threadsLis) }()

	threadsConn, err := grpc.NewClient(
		threadsLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		threadsGRPC.Stop()
		t.Fatalf("dial threads: %v", err)
	}
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)

	chatLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		threadsConn.Close()
		threadsGRPC.Stop()
		t.Fatalf("listen chat: %v", err)
	}
	chatGRPC := grpc.NewServer()
	chatv1.RegisterChatServiceServer(chatGRPC, server.New(threadsClient))
	go func() { _ = chatGRPC.Serve(chatLis) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	chatConn, err := grpc.DialContext(
		ctx,
		chatLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		chatGRPC.Stop()
		threadsConn.Close()
		threadsGRPC.Stop()
		t.Fatalf("dial chat: %v", err)
	}

	env := &testEnv{
		client:      chatv1.NewChatServiceClient(chatConn),
		fakeThreads: fake,
		chatConn:    chatConn,
		chatGRPC:    chatGRPC,
		threadsGRPC: threadsGRPC,
	}

	t.Cleanup(func() {
		chatConn.Close()
		chatGRPC.GracefulStop()
		threadsConn.Close()
		threadsGRPC.GracefulStop()
	})

	return env
}

func ctxWithIdentity(identityID, identityType, tenantID string) context.Context {
	md := metadata.Pairs(
		"x-identity-id", identityID,
		"x-identity-type", identityType,
		"x-tenant-id", tenantID,
	)
	return metadata.NewOutgoingContext(context.Background(), md)
}

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	chatv1 "github.com/agynio/chat/gen/go/agynio/api/chat/v1"
	identityv1 "github.com/agynio/chat/gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/chat/gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/chat/gen/go/agynio/api/threads/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agynio/chat/internal/config"
	"github.com/agynio/chat/internal/db"
	"github.com/agynio/chat/internal/server"
	"github.com/agynio/chat/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("chat: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	threadsConn, err := grpc.NewClient(cfg.ThreadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial threads: %w", err)
	}
	defer threadsConn.Close()

	runnersConn, err := grpc.NewClient(cfg.RunnersAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial runners: %w", err)
	}
	defer runnersConn.Close()

	identityConn, err := grpc.NewClient(cfg.IdentityAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial identity: %w", err)
	}
	defer identityConn.Close()

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)
	identityClient := identityv1.NewIdentityServiceClient(identityConn)
	chatStore := store.New(pool)

	grpcServer := grpc.NewServer()
	chatv1.RegisterChatServiceServer(grpcServer, server.New(threadsClient, runnersClient, identityClient, chatStore))

	lis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddress, err)
	}

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("ChatService listening on %s", cfg.GRPCAddress)
	if err := grpcServer.Serve(lis); err != nil {
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

package config

import (
	"fmt"
	"os"
)

type Config struct {
	GRPCAddress     string
	ThreadsAddress  string
	RunnersAddress  string
	IdentityAddress string
	DatabaseURL     string
}

func FromEnv() (Config, error) {
	cfg := Config{}

	cfg.GRPCAddress = os.Getenv("GRPC_ADDRESS")
	if cfg.GRPCAddress == "" {
		cfg.GRPCAddress = ":50051"
	}

	cfg.ThreadsAddress = os.Getenv("THREADS_ADDRESS")
	if cfg.ThreadsAddress == "" {
		return Config{}, fmt.Errorf("THREADS_ADDRESS must be set")
	}

	cfg.RunnersAddress = os.Getenv("RUNNERS_ADDRESS")
	if cfg.RunnersAddress == "" {
		return Config{}, fmt.Errorf("RUNNERS_ADDRESS must be set")
	}

	cfg.IdentityAddress = os.Getenv("IDENTITY_ADDRESS")
	if cfg.IdentityAddress == "" {
		return Config{}, fmt.Errorf("IDENTITY_ADDRESS must be set")
	}

	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}

	return cfg, nil
}

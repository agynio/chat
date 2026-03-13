package config

import (
	"fmt"
	"os"
)

type Config struct {
	GRPCAddress    string
	ThreadsAddress string
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

	return cfg, nil
}

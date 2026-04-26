#!/usr/bin/env bash
set -eu

echo "=== DevSpace startup ==="

echo "Generating protobuf types..."
buf generate buf.build/agynio/api --path agynio/api/chat/v1 --path agynio/api/threads/v1 --path agynio/api/runners/v1 --path agynio/api/runner/v1 --path agynio/api/identity/v1

echo "Downloading Go modules..."
go mod download

echo "Starting dev server (air)..."
exec air

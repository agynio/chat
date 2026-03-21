package identity

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const (
	MetadataKeyIdentityID   = "x-identity-id"
	MetadataKeyIdentityType = "x-identity-type"
)

type Identity struct {
	IdentityID   string
	IdentityType string
}

func FromContext(ctx context.Context) (Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Identity{}, fmt.Errorf("missing gRPC metadata")
	}

	identityID, err := singleValue(md, MetadataKeyIdentityID)
	if err != nil {
		return Identity{}, err
	}

	identityType, err := singleValue(md, MetadataKeyIdentityType)
	if err != nil {
		return Identity{}, err
	}

	return Identity{
		IdentityID:   identityID,
		IdentityType: identityType,
	}, nil
}

func singleValue(md metadata.MD, key string) (string, error) {
	values := md.Get(key)
	if len(values) == 0 {
		return "", fmt.Errorf("missing required metadata key %q", key)
	}
	return values[0], nil
}

package identity

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

const (
	MetadataKeyIdentityID   = "x-identity-id"
	MetadataKeyIdentityType = "x-identity-type"
	MetadataKeyWorkloadID   = "x-workload-id"
)

type Identity struct {
	IdentityID   string
	IdentityType string
	WorkloadID   *string
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

	workloadID := optionalValue(md, MetadataKeyWorkloadID)

	return Identity{
		IdentityID:   identityID,
		IdentityType: identityType,
		WorkloadID:   workloadID,
	}, nil
}

func AppendToOutgoingContext(ctx context.Context, identity Identity) context.Context {
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		MetadataKeyIdentityID,
		identity.IdentityID,
		MetadataKeyIdentityType,
		identity.IdentityType,
	)
	if identity.WorkloadID == nil {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, MetadataKeyWorkloadID, *identity.WorkloadID)
}

func singleValue(md metadata.MD, key string) (string, error) {
	values := md.Get(key)
	if len(values) == 0 {
		return "", fmt.Errorf("missing required metadata key %q", key)
	}
	return values[0], nil
}

func optionalValue(md metadata.MD, key string) *string {
	values := md.Get(key)
	if len(values) == 0 {
		return nil
	}
	value := values[0]
	return &value
}

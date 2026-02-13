package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

func TestNewLandController(t *testing.T) {
	controller := NewLandController(zap.NewNop(), tally.NoopScope)
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	controller := NewLandController(zap.NewNop(), tally.NoopScope)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	require.NotEmpty(t, resp.Sqid)
}

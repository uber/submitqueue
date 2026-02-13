package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/gateway/protopb"
)

func TestNewLandController(t *testing.T) {
	controller := NewLandController(nil, nil)
	require.NotNil(t, controller)
}

func TestLand_ReturnsSqid(t *testing.T) {
	controller := NewLandController(nil, nil)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	resp, err := controller.Land(ctx, req)

	require.NoError(t, err)
	require.NotEmpty(t, resp.Sqid)
}

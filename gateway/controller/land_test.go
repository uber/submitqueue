package controller

import (
	"context"
	"testing"

	pb "github.com/uber/submitqueue/gateway/protopb"
)

func TestNewLandController(t *testing.T) {
	controller := NewLandController(nil, nil)
	if controller == nil {
		t.Fatal("NewLandController() returned nil")
	}
}

func TestLand_ReturnsSqid(t *testing.T) {
	controller := NewLandController(nil, nil)
	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:  "test-queue",
		Change: &pb.Change{Source: "github", Ids: []string{"123"}},
	}
	resp, err := controller.Land(ctx, req)

	if err != nil {
		t.Fatalf("Land() returned error: %v", err)
	}

	if resp.Sqid == "" {
		t.Fatal("Expected sqid to be set, got empty string")
	}
}

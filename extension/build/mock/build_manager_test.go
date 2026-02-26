package mock

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"
	"github.com/uber/submitqueue/entity"
)

// TestMockBuildManager_Compilation verifies the mock compiles and basic setup works.
func TestMockBuildManager_Compilation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBuildMgr := NewMockBuildManager(ctrl)

	// Test Schedule
	expectedBuildID := "mock://test-build-123"
	mockBuildMgr.EXPECT().
		Schedule(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(expectedBuildID, nil)

	queueName := "test-queue"
	changes := []entity.BuildChange{
		{ChangeID: "D12345", Action: entity.ChangeActionApply},
		{ChangeID: "D12346", Action: entity.ChangeActionValidate},
	}

	buildID, err := mockBuildMgr.Schedule(
		context.Background(), queueName, changes,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if buildID != expectedBuildID {
		t.Fatalf("expected build ID %v, got %v", expectedBuildID, buildID)
	}

	// Test Poll
	expectedMetadata := entity.BuildMetadata{
		"build_url": "https://ci.example.com/builds/123",
		"commit_sha": "abc123",
	}
	mockBuildMgr.EXPECT().
		Poll(gomock.Any(), gomock.Any()).
		Return(entity.BuildStatusSucceeded, expectedMetadata, nil)

	status, metadata, err := mockBuildMgr.Poll(context.Background(), buildID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != entity.BuildStatusSucceeded {
		t.Fatalf("expected %v, got %v", entity.BuildStatusSucceeded, status)
	}
	if metadata["build_url"] != expectedMetadata["build_url"] {
		t.Fatalf("expected metadata %v, got %v", expectedMetadata, metadata)
	}
}

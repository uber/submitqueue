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
	mockBuildMgr.EXPECT().
		Schedule(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	queueName := "test-queue"
	changes := []entity.BuildChange{
		{ChangeID: "D12345", Action: entity.BuildActionValidate},
		{ChangeID: "D12346", Action: entity.BuildActionApply},
	}

	err := mockBuildMgr.Schedule(
		context.Background(), queueName, changes,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test Poll
	buildID := "mock://1"
	mockBuildMgr.EXPECT().
		Poll(gomock.Any(), gomock.Any()).
		Return(entity.BuildStatusPassed, nil)

	status, err := mockBuildMgr.Poll(context.Background(), buildID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != entity.BuildStatusPassed {
		t.Fatalf("expected %v, got %v", entity.BuildStatusPassed, status)
	}
}

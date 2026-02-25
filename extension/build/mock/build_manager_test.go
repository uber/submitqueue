package mock

import (
	"context"
	"testing"

	"github.com/uber/submitqueue/entity"
	"go.uber.org/mock/gomock"
)

// TestMockBuildManager_Compilation verifies the mock compiles and basic setup works.
func TestMockBuildManager_Compilation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBuildMgr := NewMockBuildManager(ctrl)

	// Test ScheduleBuild
	buildID := "mock://1"
	mockBuildMgr.EXPECT().
		ScheduleBuild(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(buildID, nil)

	head := "queue-1/batch/5"
	base := []string{"queue-1/batch/1", "queue-1/batch/2"}
	jobName := "test-pipeline"

	result, err := mockBuildMgr.ScheduleBuild(
		context.Background(), head, base, jobName,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != buildID {
		t.Fatalf("expected %v, got %v", buildID, result)
	}

	// Test Poll
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

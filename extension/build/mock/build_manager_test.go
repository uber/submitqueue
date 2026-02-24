package mock

import (
	"context"
	"testing"

	"github.com/uber/submitqueue/entity"
	entitybuild "github.com/uber/submitqueue/entity/build"
	"go.uber.org/mock/gomock"
)

// TestMockBuildManager_Compilation verifies the mock compiles and basic setup works.
func TestMockBuildManager_Compilation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBuildMgr := NewMockBuildManager(ctrl)

	// Verify mock implements the interface by calling a method with expectations
	buildID := entitybuild.NewBuildID("mock", "1")
	mockBuildMgr.EXPECT().
		ScheduleBuild(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any()).
		Return(buildID, nil)

	testBatch := entity.Batch{ID: "test"}
	result, err := mockBuildMgr.ScheduleBuild(
		context.Background(), "sha", nil, testBatch,
		"repo", "main", "pipeline", "sqid", nil, "msg",
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != buildID {
		t.Fatalf("expected %v, got %v", buildID, result)
	}
}

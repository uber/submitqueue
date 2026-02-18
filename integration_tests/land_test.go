package integration_tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"github.com/uber/submitqueue/extensions/storage/mysql"
	"github.com/uber/submitqueue/gateway/controller"
	pb "github.com/uber/submitqueue/gateway/protopb"
	"go.uber.org/zap"
)

func TestLandController_LandRequest(t *testing.T) {
	log := newTestLogger(t)

	params := setupMySQL(t, log)

	factory, err := mysql.NewFactory(params)
	require.NoError(t, err, "failed to create MySQL factory")
	defer factory.Close()

	ctrl := controller.NewLandController(zap.NewNop(), tally.NoopScope, factory)

	ctx := context.Background()

	req := &pb.LandRequest{
		Queue:    "integration-test-queue",
		Change:   &pb.Change{Source: "github", Ids: []string{"pr-100", "pr-101"}},
		Strategy: pb.Strategy_STRATEGY_REBASE,
	}

	log.logf("Sending Land request for queue=%s", req.Queue)
	resp, err := ctrl.Land(ctx, req)
	require.NoError(t, err, "Land request failed")
	require.NotEmpty(t, resp.Sqid, "SQID should not be empty")
	log.logf("Land request succeeded: sqid=%s", resp.Sqid)
}

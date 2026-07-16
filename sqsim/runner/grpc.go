// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"context"
	"fmt"

	changepb "github.com/uber/submitqueue/api/base/change/protopb"
	mergestrategypb "github.com/uber/submitqueue/api/base/mergestrategy/protopb"
	gatewaypb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCGateway calls the public Gateway gRPC API.
type GRPCGateway struct {
	conn   *grpc.ClientConn
	client gatewaypb.SubmitQueueGatewayClient
}

// NewGRPCGateway connects to a Gateway address.
func NewGRPCGateway(address string) (*GRPCGateway, error) {
	if address == "" {
		return nil, fmt.Errorf("gateway address is required")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to gateway: %w", err)
	}
	return &GRPCGateway{conn: conn, client: gatewaypb.NewSubmitQueueGatewayClient(conn)}, nil
}

// Close closes the Gateway connection.
func (g *GRPCGateway) Close() error {
	return g.conn.Close()
}

// Ping verifies that the Gateway is serving requests.
func (g *GRPCGateway) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &gatewaypb.PingRequest{Message: "sqsim"})
	return err
}

// Land submits one synthetic change.
func (g *GRPCGateway) Land(ctx context.Context, queue, changeURI string) (string, error) {
	response, err := g.client.Land(ctx, &gatewaypb.LandRequest{
		Queue:    queue,
		Change:   &changepb.Change{Uris: []string{changeURI}},
		Strategy: mergestrategypb.Strategy_REBASE,
	})
	if err != nil {
		return "", err
	}
	return response.GetSqid(), nil
}

// List returns one page of queue request summaries.
func (g *GRPCGateway) List(ctx context.Context, queue string, receivedAtOrAfterMs, receivedBeforeMs int64, pageToken string) ([]Summary, string, error) {
	response, err := g.client.List(ctx, &gatewaypb.ListRequest{
		Queue:               queue,
		ReceivedAtOrAfterMs: receivedAtOrAfterMs,
		ReceivedBeforeMs:    receivedBeforeMs,
		PageSize:            100,
		PageToken:           pageToken,
	})
	if err != nil {
		return nil, "", err
	}
	summaries := make([]Summary, len(response.GetRequests()))
	for i, summary := range response.GetRequests() {
		summaries[i] = fromProtoSummary(summary)
	}
	return summaries, response.GetNextPageToken(), nil
}

// Summary returns the current public request summary.
func (g *GRPCGateway) Summary(ctx context.Context, sqid string) (Summary, error) {
	response, err := g.client.GetRequestSummaryByID(ctx, &gatewaypb.GetRequestSummaryByIDRequest{Sqid: sqid})
	if err != nil {
		return Summary{}, err
	}
	if response.GetRequest() == nil {
		return Summary{}, fmt.Errorf("request summary %q is empty", sqid)
	}
	return fromProtoSummary(response.GetRequest()), nil
}

// History returns retained public lifecycle events.
func (g *GRPCGateway) History(ctx context.Context, sqid string) ([]HistoryEvent, error) {
	response, err := g.client.GetRequestHistoryByID(ctx, &gatewaypb.GetRequestHistoryByIDRequest{Sqid: sqid})
	if err != nil {
		return nil, err
	}
	events := make([]HistoryEvent, len(response.GetEvents()))
	for i, event := range response.GetEvents() {
		events[i] = HistoryEvent{
			TimestampMs: event.GetTimestampMs(),
			Status:      event.GetStatus(),
			LastError:   event.GetLastError(),
			Metadata:    cloneMetadata(event.GetMetadata()),
		}
	}
	return events, nil
}

func fromProtoSummary(summary *gatewaypb.RequestSummary) Summary {
	return Summary{
		SQID:      summary.GetSqid(),
		Status:    summary.GetStatus(),
		LastError: summary.GetLastError(),
		Metadata:  cloneMetadata(summary.GetMetadata()),
	}
}

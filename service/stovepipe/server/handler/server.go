// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package handler implements Stovepipe's gRPC service handlers.
package handler

import (
	"context"

	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/service/stovepipe/server/mapper"
	"github.com/uber/submitqueue/stovepipe/controller"
)

// StovepipeServer wraps the controllers and implements the gRPC service interface.
type StovepipeServer struct {
	pb.UnimplementedStovepipeServer
	pingController           *controller.PingController
	ingestController         *controller.IngestController
	requestHistoryController controller.RequestHistoryController
	projectStatusController  *controller.GetProjectStatusByURIController
}

// NewStovepipeServer creates a gRPC service handler from Stovepipe controllers.
func NewStovepipeServer(
	pingController *controller.PingController,
	ingestController *controller.IngestController,
	requestHistoryController controller.RequestHistoryController,
	projectStatusController *controller.GetProjectStatusByURIController,
) *StovepipeServer {
	return &StovepipeServer{
		pingController:           pingController,
		ingestController:         ingestController,
		requestHistoryController: requestHistoryController,
		projectStatusController:  projectStatusController,
	}
}

// Ping delegates to the controller.
func (s *StovepipeServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.pingController.Ping(ctx, req)
}

// Ingest maps the wire request to an entity, delegates to the controller, and maps
// the result back to the wire response.
func (s *StovepipeServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	result, err := s.ingestController.Ingest(ctx, mapper.ProtoToIngestRequest(req))
	if err != nil {
		return nil, err
	}
	return mapper.IngestResultToProto(result), nil
}

// GetRequestHistoryByID returns retained history for one request ID.
func (s *StovepipeServer) GetRequestHistoryByID(ctx context.Context, req *pb.GetRequestHistoryByIDRequest) (*pb.GetRequestHistoryByIDResponse, error) {
	events, err := s.requestHistoryController.GetRequestHistoryByID(ctx, mapper.ProtoToGetRequestHistoryByIDRequest(req))
	if err != nil {
		return nil, err
	}
	return &pb.GetRequestHistoryByIDResponse{Events: mapper.HistoryEventsToProto(events)}, nil
}

// GetRequestHistoryByURI returns retained histories for one commit URI.
func (s *StovepipeServer) GetRequestHistoryByURI(ctx context.Context, req *pb.GetRequestHistoryByURIRequest) (*pb.GetRequestHistoryByURIResponse, error) {
	histories, err := s.requestHistoryController.GetRequestHistoryByURI(ctx, mapper.ProtoToGetRequestHistoryByURIRequest(req))
	if err != nil {
		return nil, err
	}
	return &pb.GetRequestHistoryByURIResponse{Histories: mapper.RequestHistoriesToProto(histories)}, nil
}

// GetProjectStatusByURI returns the current repository validation status for a commit.
func (s *StovepipeServer) GetProjectStatusByURI(ctx context.Context, req *pb.GetProjectStatusByURIRequest) (*pb.GetProjectStatusByURIResponse, error) {
	result, err := s.projectStatusController.GetProjectStatusByURI(ctx, mapper.ProtoToGetProjectStatusByURIRequest(req))
	if err != nil {
		return nil, err
	}
	return mapper.GetProjectStatusByURIResultToProto(result), nil
}

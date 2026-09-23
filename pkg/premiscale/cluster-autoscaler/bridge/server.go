// Package bridge exposes the upstream autoscaler contract over a Python backend.
package bridge

import (
	"context"
	pb "github.com/premiscale/premiscale/cluster-autoscaler/protocol"
	"google.golang.org/grpc"
	"time"
)

// Server forwards cloud-provider calls with bounded deadlines and cancellation.
type Server struct {
	pb.UnimplementedCloudProviderServer
	backend   pb.CloudProviderClient
	templates pb.TemplatesClient
	timeout   time.Duration
}

// New constructs the public service over a private backend connection.
func New(connection grpc.ClientConnInterface, timeout time.Duration) *Server {
	return &Server{backend: pb.NewCloudProviderClient(connection), templates: pb.NewTemplatesClient(connection), timeout: timeout}
}

func (s *Server) NodeGroups(ctx context.Context, request *pb.NodeGroupsRequest) (*pb.NodeGroupsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroups(ctx, request)
}

func (s *Server) NodeGroupForNode(ctx context.Context, request *pb.NodeGroupForNodeRequest) (*pb.NodeGroupForNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupForNode(ctx, request)
}

func (s *Server) PricingNodePrice(ctx context.Context, request *pb.PricingNodePriceRequest) (*pb.PricingNodePriceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.PricingNodePrice(ctx, request)
}

func (s *Server) PricingPodPrice(ctx context.Context, request *pb.PricingPodPriceRequest) (*pb.PricingPodPriceResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.PricingPodPrice(ctx, request)
}

func (s *Server) GPULabel(ctx context.Context, request *pb.GPULabelRequest) (*pb.GPULabelResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.GPULabel(ctx, request)
}

func (s *Server) GetAvailableGPUTypes(ctx context.Context, request *pb.GetAvailableGPUTypesRequest) (*pb.GetAvailableGPUTypesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.GetAvailableGPUTypes(ctx, request)
}

func (s *Server) Cleanup(ctx context.Context, request *pb.CleanupRequest) (*pb.CleanupResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.Cleanup(ctx, request)
}

func (s *Server) Refresh(ctx context.Context, request *pb.RefreshRequest) (*pb.RefreshResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.Refresh(ctx, request)
}

func (s *Server) NodeGroupTargetSize(ctx context.Context, request *pb.NodeGroupTargetSizeRequest) (*pb.NodeGroupTargetSizeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupTargetSize(ctx, request)
}

func (s *Server) NodeGroupIncreaseSize(ctx context.Context, request *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupIncreaseSize(ctx, request)
}

func (s *Server) NodeGroupDeleteNodes(ctx context.Context, request *pb.NodeGroupDeleteNodesRequest) (*pb.NodeGroupDeleteNodesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupDeleteNodes(ctx, request)
}

func (s *Server) NodeGroupDecreaseTargetSize(ctx context.Context, request *pb.NodeGroupDecreaseTargetSizeRequest) (*pb.NodeGroupDecreaseTargetSizeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupDecreaseTargetSize(ctx, request)
}

func (s *Server) NodeGroupNodes(ctx context.Context, request *pb.NodeGroupNodesRequest) (*pb.NodeGroupNodesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupNodes(ctx, request)
}

func (s *Server) NodeGroupGetOptions(ctx context.Context, request *pb.NodeGroupAutoscalingOptionsRequest) (*pb.NodeGroupAutoscalingOptionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.backend.NodeGroupGetOptions(ctx, request)
}

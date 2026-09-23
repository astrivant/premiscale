package bridge

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/premiscale/premiscale/cluster-autoscaler/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	core "k8s.io/api/core/v1"
)

type backend struct {
	pb.UnimplementedCloudProviderServer
	pb.UnimplementedTemplatesServer
	wait bool
}

func (b *backend) NodeGroups(ctx context.Context, _ *pb.NodeGroupsRequest) (*pb.NodeGroupsResponse, error) {
	if b.wait {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &pb.NodeGroupsResponse{NodeGroups: []*pb.NodeGroup{{Id: "workers", MinSize: 1, MaxSize: 5}}}, nil
}

func (b *backend) NodeGroupIncreaseSize(_ context.Context, r *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	if r.Id != "workers" {
		return nil, status.Error(codes.NotFound, "unknown group")
	}
	if r.Delta <= 0 {
		return nil, status.Error(codes.InvalidArgument, "positive delta required")
	}
	return &pb.NodeGroupIncreaseSizeResponse{}, nil
}

func (b *backend) Get(_ context.Context, r *pb.TemplateRequest) (*pb.TemplateResponse, error) {
	if r.Id != "workers" {
		return nil, status.Error(codes.NotFound, "unknown group")
	}
	return &pb.TemplateResponse{Labels: map[string]string{"kubernetes.io/os": "linux"},
		Capacity:    map[string]string{"cpu": "2", "memory": "4Gi", "pods": "110"},
		Allocatable: map[string]string{"cpu": "1800m", "memory": "3Gi", "pods": "110"},
		Taints:      []*pb.Taint{{Key: "dedicated", Value: "workers", Effect: "NoSchedule"}}}, nil
}

func connect(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close(); server.Stop(); listener.Close() })
	return connection
}

func client(t *testing.T, wait bool) pb.CloudProviderClient {
	t.Helper()
	origin := connect(t, func(server *grpc.Server) {
		implementation := &backend{wait: wait}
		pb.RegisterCloudProviderServer(server, implementation)
		pb.RegisterTemplatesServer(server, implementation)
	})
	public := connect(t, func(server *grpc.Server) { pb.RegisterCloudProviderServer(server, New(origin, 100*time.Millisecond)) })
	return pb.NewCloudProviderClient(public)
}

func TestForwardingAndErrorCodes(t *testing.T) {
	provider := client(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groups, err := provider.NodeGroups(ctx, &pb.NodeGroupsRequest{})
	if err != nil || len(groups.NodeGroups) != 1 || groups.NodeGroups[0].Id != "workers" {
		t.Fatalf("discovery: %v %v", groups, err)
	}
	_, err = provider.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "workers", Delta: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "workers", Delta: 0})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("lost backend status: %v", err)
	}
	_, err = provider.NodeGroupIncreaseSize(ctx, &pb.NodeGroupIncreaseSizeRequest{Id: "missing", Delta: 1})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("lost backend status: %v", err)
	}
	_, err = provider.PricingNodePrice(ctx, &pb.PricingNodePriceRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("optional pricing: %v", err)
	}
}

func TestBackendDeadline(t *testing.T) {
	provider := client(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := provider.NodeGroups(ctx, &pb.NodeGroupsRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline: %v", err)
	}
}

func TestTemplateIsKubernetesProtobuf(t *testing.T) {
	provider := client(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := provider.NodeGroupTemplateNodeInfo(ctx, &pb.NodeGroupTemplateNodeInfoRequest{Id: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	node := &core.Node{}
	if err := node.Unmarshal(response.NodeBytes); err != nil {
		t.Fatal(err)
	}
	if node.Status.Capacity.Cpu().Value() != 2 || node.Status.Allocatable.Cpu().MilliValue() != 1800 {
		t.Fatalf("capacity: %+v", node.Status)
	}
	if node.Labels["kubernetes.io/os"] != "linux" || len(node.Spec.Taints) != 1 {
		t.Fatalf("node: %+v", node)
	}
}

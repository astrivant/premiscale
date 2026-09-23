package bridge

import (
	"context"
	"fmt"

	pb "github.com/premiscale/premiscale/cluster-autoscaler/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NodeGroupTemplateNodeInfo produces the actual Kubernetes protobuf expected by CA.
func (s *Server) NodeGroupTemplateNodeInfo(ctx context.Context, request *pb.NodeGroupTemplateNodeInfoRequest) (*pb.NodeGroupTemplateNodeInfoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	template, err := s.templates.Get(ctx, &pb.TemplateRequest{Id: request.Id})
	if err != nil {
		return nil, err
	}
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "premiscale-template", Labels: template.Labels}}
	node.Status.Capacity, err = quantities(template.Capacity)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	node.Status.Allocatable, err = quantities(template.Allocatable)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for _, taint := range template.Taints {
		node.Spec.Taints = append(node.Spec.Taints, core.Taint{Key: taint.Key, Value: taint.Value, Effect: core.TaintEffect(taint.Effect)})
	}
	node.Status.Conditions = []core.NodeCondition{{Type: core.NodeReady, Status: core.ConditionTrue}}
	data, err := node.Marshal()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.NodeGroupTemplateNodeInfoResponse{NodeBytes: data}, nil
}

func quantities(values map[string]string) (core.ResourceList, error) {
	result := core.ResourceList{}
	for name, value := range values {
		quantity, err := resource.ParseQuantity(value)
		if err != nil || quantity.Sign() < 0 {
			return nil, fmt.Errorf("invalid capacity for %s: %q", name, value)
		}
		result[core.ResourceName(name)] = quantity
	}
	return result, nil
}

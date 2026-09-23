"""
Adapt the durable provider to the upstream gRPC service.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import logging
from typing import TYPE_CHECKING

import grpc
import libvirt

from .provider import DELETING
from .protos import externalgrpc_pb2 as pb
from .protos import externalgrpc_pb2_grpc as rpc
from .protos import templates_pb2 as templates
from .protos import templates_pb2_grpc as template_rpc

if TYPE_CHECKING:
    from typing import Any, Callable
    from .provider import Provider


log = logging.getLogger(__name__)


def errors(function: Callable) -> Callable:
    """
    Translate backend failures into stable gRPC status codes.

    Args:
        function (Callable): Callable whose failures should be translated to gRPC status codes.

    Returns:
        Callable: Wrapped RPC handler that reports backend failures through gRPC.
    """
    @wraps(function)
    def call(self: Any, request: Any, context: grpc.ServicerContext) -> Any:
        """
        Invoke the wrapped operation and handle its failure policy.

        Args:
            self (Any): Service instance whose RPC handler is being invoked.
            request (Any): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            Any: Protobuf response produced by the wrapped handler.
        """
        try:
            return function(self, request, context)
        except KeyError as error:
            context.abort(grpc.StatusCode.NOT_FOUND, f'Unknown node group: {error}')
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except (ConnectionError, libvirt.libvirtError) as error:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(error))
        except Exception:
            log.exception('Provider RPC failed')
            context.abort(grpc.StatusCode.INTERNAL, 'Provider operation failed; see controller logs')
    return call


class CloudProvider(rpc.CloudProviderServicer):
    """
    Serve discovery, target changes, and VM lifecycle requests.
    """

    def __init__(self, provider: Provider) -> None:
        """
        Initialize CloudProvider with the supplied settings.

        Args:
            provider (Provider): Provider that owns node groups and durable lifecycle operations.
        """
        self.provider = provider

    def _group(self, name: str) -> pb.NodeGroup:
        """
        Build the upstream node-group descriptor for a configured group.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            pb.NodeGroup: The upstream node-group descriptor for a configured group.
        """
        group = self.provider.group(name)
        return pb.NodeGroup(id=name, minSize=group.scaling.minNodes, maxSize=group.scaling.maxNodes,
                            debug=f'PremiScale node group {name}')

    @errors
    def NodeGroups(self, request: pb.NodeGroupsRequest, context: grpc.ServicerContext) -> pb.NodeGroupsResponse:
        """
        List the configured autoscaling node groups.

        Args:
            request (pb.NodeGroupsRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupsResponse: The configured autoscaling node groups.
        """
        return pb.NodeGroupsResponse(nodeGroups=[self._group(name) for name in sorted(self.provider.groups)])

    @errors
    def NodeGroupForNode(self, request: pb.NodeGroupForNodeRequest, context: grpc.ServicerContext) -> pb.NodeGroupForNodeResponse:
        """
        Resolve the node group that owns the supplied node.

        Args:
            request (pb.NodeGroupForNodeRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupForNodeResponse: The node group that owns the supplied node.
        """
        name = self.provider.group_for_node(request.node.providerID, request.node.name)
        return pb.NodeGroupForNodeResponse(nodeGroup=self._group(name) if name else pb.NodeGroup())

    @errors
    def NodeGroupTargetSize(self, request: pb.NodeGroupTargetSizeRequest, context: grpc.ServicerContext) -> pb.NodeGroupTargetSizeResponse:
        """
        Report the accepted target size of a node group.

        Args:
            request (pb.NodeGroupTargetSizeRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupTargetSizeResponse: The accepted target size of a node group.
        """
        return pb.NodeGroupTargetSizeResponse(targetSize=self.provider.target_size(request.id))

    @errors
    def NodeGroupIncreaseSize(self, request: pb.NodeGroupIncreaseSizeRequest, context: grpc.ServicerContext) -> pb.NodeGroupIncreaseSizeResponse:
        """
        Reserve additional instances for a node group.

        Args:
            request (pb.NodeGroupIncreaseSizeRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupIncreaseSizeResponse: Empty response acknowledging the accepted scale-up reservation.
        """
        self.provider.increase(request.id, request.delta)
        return pb.NodeGroupIncreaseSizeResponse()

    @errors
    def NodeGroupDecreaseTargetSize(self, request: pb.NodeGroupDecreaseTargetSizeRequest, context: grpc.ServicerContext) -> pb.NodeGroupDecreaseTargetSizeResponse:
        """
        Cancel queued reservations without deleting running VMs.

        Args:
            request (pb.NodeGroupDecreaseTargetSizeRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupDecreaseTargetSizeResponse: Empty response acknowledging cancellation of queued reservations.
        """
        self.provider.decrease_target(request.id, request.delta)
        return pb.NodeGroupDecreaseTargetSizeResponse()

    @errors
    def NodeGroupDeleteNodes(self, request: pb.NodeGroupDeleteNodesRequest, context: grpc.ServicerContext) -> pb.NodeGroupDeleteNodesResponse:
        """
        Queue deletion of the explicitly selected managed nodes.

        Args:
            request (pb.NodeGroupDeleteNodesRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupDeleteNodesResponse: Empty response acknowledging the requested deletions.
        """
        self.provider.delete_nodes(request.id, [(node.providerID, node.name) for node in request.nodes])
        return pb.NodeGroupDeleteNodesResponse()

    @errors
    def NodeGroupNodes(self, request: pb.NodeGroupNodesRequest, context: grpc.ServicerContext) -> pb.NodeGroupNodesResponse:
        """
        Report managed instances and their lifecycle states.

        Args:
            request (pb.NodeGroupNodesRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupNodesResponse: Managed instances and their lifecycle states.
        """
        instances = []
        for node in self.provider.nodes(request.id):
            state = pb.InstanceStatus.instanceDeleting if node.phase in DELETING else (
                pb.InstanceStatus.instanceRunning if node.phase == 'running' else pb.InstanceStatus.instanceCreating)
            status = pb.InstanceStatus(instanceState=state)
            if node.error:
                status.errorInfo.CopyFrom(pb.InstanceErrorInfo(errorCode='ProvisioningFailed', errorMessage=node.error, instanceErrorClass=1))
            instances.append(pb.Instance(id=node.provider_id, status=status))
        return pb.NodeGroupNodesResponse(instances=instances)

    @errors
    def Refresh(self, request: pb.RefreshRequest, context: grpc.ServicerContext) -> pb.RefreshResponse:
        """
        Reconcile the durable journal with discovered hypervisor state.

        Args:
            request (pb.RefreshRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.RefreshResponse: Empty response after reconciliation completes.
        """
        self.provider.refresh()
        return pb.RefreshResponse()

    def Cleanup(self, request: pb.CleanupRequest, context: grpc.ServicerContext) -> pb.CleanupResponse:
        """
        Acknowledge the stateless upstream cleanup request.

        Args:
            request (pb.CleanupRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.CleanupResponse: Empty response; this RPC owns no additional resources to release.
        """
        return pb.CleanupResponse()

    def GPULabel(self, request: pb.GPULabelRequest, context: grpc.ServicerContext) -> pb.GPULabelResponse:
        """
        Report that no GPU resource label is configured.

        Args:
            request (pb.GPULabelRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.GPULabelResponse: Response containing an empty GPU label.
        """
        return pb.GPULabelResponse()

    def GetAvailableGPUTypes(self, request: pb.GetAvailableGPUTypesRequest, context: grpc.ServicerContext) -> pb.GetAvailableGPUTypesResponse:
        """
        Report an empty set of supported GPU types.

        Args:
            request (pb.GetAvailableGPUTypesRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.GetAvailableGPUTypesResponse: An empty set of supported GPU types.
        """
        return pb.GetAvailableGPUTypesResponse()

    @errors
    def NodeGroupGetOptions(self, request: pb.NodeGroupAutoscalingOptionsRequest, context: grpc.ServicerContext) -> pb.NodeGroupAutoscalingOptionsResponse:
        """
        Validate the group and preserve the autoscaler defaults.

        Args:
            request (pb.NodeGroupAutoscalingOptionsRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            pb.NodeGroupAutoscalingOptionsResponse: Response containing the caller-supplied autoscaling defaults.
        """
        self.provider.group(request.id)
        return pb.NodeGroupAutoscalingOptionsResponse(nodeGroupAutoscalingOptions=request.defaults)


class Templates(template_rpc.TemplatesServicer):
    """
    Supply structured capacities for native Kubernetes Node serialization in Go.
    """

    def __init__(self, provider: Provider) -> None:
        """
        Initialize Templates with the supplied settings.

        Args:
            provider (Provider): Provider that owns node groups and durable lifecycle operations.
        """
        self.provider = provider

    @errors
    def Get(self, request: templates.TemplateRequest, context: grpc.ServicerContext) -> templates.TemplateResponse:
        """
        Return the resource capacity and metadata of a group template.

        Args:
            request (templates.TemplateRequest): Incoming protobuf request.
            context (grpc.ServicerContext): Execution context for the operation.

        Returns:
            templates.TemplateResponse: The resource capacity and metadata of a group template.
        """
        self.provider.group(request.id)
        return templates.TemplateResponse(**self.provider.driver.template(request.id))


def create_server(provider: Provider, target: str) -> Any:
    """
    Build the private Python service on a supervisor-owned Unix socket.

    Args:
        provider (Provider): Provider that owns node groups and durable lifecycle operations.
        target (str): Address on which to bind the private gRPC service.

    Returns:
        Any: The private Python service on a supervisor-owned Unix socket.

    Raises:
        RuntimeError: If the private gRPC backend cannot bind the requested address.
    """
    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    rpc.add_CloudProviderServicer_to_server(CloudProvider(provider), server)
    template_rpc.add_TemplatesServicer_to_server(Templates(provider), server)
    if not server.add_insecure_port(target):
        raise RuntimeError(f'Could not bind Python gRPC backend: {target}')
    return server

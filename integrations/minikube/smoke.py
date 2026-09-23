"""
Exercise in-cluster HTTP, gRPC, and Dragonfly using the installed controller image.
"""

from urllib.request import urlopen
from uuid import uuid4

import grpc

from premiscale.config.v1alpha1 import Broker
from premiscale.messaging.channels import platform_queue
from premiscale_cluster_autoscaler.protos import externalgrpc_pb2 as pb
from premiscale_cluster_autoscaler.protos import externalgrpc_pb2_grpc as rpc


def main() -> None:
    """
    Check real Service endpoints and use a private queue namespace for the round trip.

    Returns:
        None: No value is returned.
    """
    for port, path in ((8085, 'healthz'), (9090, 'ready'), (9090, 'metrics')):
        with urlopen(f'http://premiscale:{port}/{path}', timeout=10) as response:
            assert response.status == 200
    with grpc.insecure_channel('premiscale:50051') as channel:
        grpc.channel_ready_future(channel).result(timeout=30)
        groups = rpc.CloudProviderStub(channel).NodeGroups(pb.NodeGroupsRequest(), timeout=10)
        print(f'Provider responded with {len(groups.nodeGroups)} node groups')
    with platform_queue(Broker(namespace=f'minikube-smoke-{uuid4()}')) as queue:
        try:
            queue.put('minikube smoke test')
            with queue.delivery() as message:
                assert message.payload == 'minikube smoke test'
            assert queue.client.xpending(queue.key, queue.group)['pending'] == 0
        finally:
            queue.client.delete(queue.key, queue.dead_letters)
    print('HTTP, gRPC, and Dragonfly smoke checks passed')


if __name__ == '__main__':
    main()

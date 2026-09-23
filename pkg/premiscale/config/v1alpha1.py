"""
Parse v1alpha1 configuration files into a Config object with attrs and cattrs.
"""


from __future__ import annotations

import logging
from math import isfinite
import os
import re
import sys

from pathlib import Path
from urllib.parse import urlsplit
from attrs import define
from attr import ib
from cattrs import Converter
from cattrs.gen import make_dict_structure_fn, override

# In this particular module, cattrs requires these types during runtime to unpack,
# so we skip the TYPE_CHECKING check wrapping these imports.
from typing import Dict, List

from .databases import Databases, SQLiteState, MySQLState, LocalMetrics, PostgreSQLMetrics, InfluxDBMetrics


log = logging.getLogger(__name__)


def _expand_environment(value: str) -> str:
    """
    Expand environment references while preserving a concrete string constructor type.

    Args:
        value (str): Configuration string containing optional environment references.

    Returns:
        str: Expanded configuration value.
    """
    return os.path.expandvars(value)


@define
class Certificates:
    """
    Certificate configuration options.

    Attributes:
        path (str): Path.
    """
    path: str

    def __attrs_post_init__(self) -> None:
        """
        Post-initialization method to expand environment variables.

        Returns:
            None: No value is returned.
        """
        self.expand()

        if not Path(self.path).is_file():
            log.error(f'Certificate file at path "{self.path}" does not exist')
            sys.exit(1)

    def expand(self) -> None:
        """
        Expand environment variables in the certificate configuration.

        Returns:
            None: No value is returned.
        """
        self.path = os.path.expandvars(self.path)


@define
class Platform:
    """
    Platform configuration options.

    Attributes:
        domain (str): Platform service hostname.
        token (str): Platform registration token.
        certificates (Certificates): CA certificate settings for platform requests.
        actionsQueueMaxSize (int): Configured platform action queue capacity.
    """
    domain: str
    token: str
    certificates: Certificates
    actionsQueueMaxSize: int

    def __attrs_post_init__(self) -> None:
        """
        Post-initialization method to expand environment variables.

        Returns:
            None: No value is returned.
        """
        self.expand()

    def expand(self) -> None:
        """
        Expand environment variables in the platform configuration.

        Returns:
            None: No value is returned.
        """
        self.domain = os.path.expandvars(self.domain)
        self.token = os.path.expandvars(self.token)


@define
class CollectionControl:
    """
    Tune each reconciliation worker's throughput-driven thread-pool controller.

    Attributes:
        minThreads (int): Minimum concurrency for a nonempty host partition.
        initialThreads (int): Concurrency used before the first throughput observation.
        targetThroughput (float | None): Successful hosts per second per worker; None derives the target from the collection interval.
        proportionalGain (float): Threads per unit of relative throughput error.
        integralGain (float): Threads per second of accumulated relative error.
        derivativeGain (float): Threads per unit of relative error change per second.
        smoothing (float): Weight of the latest throughput observation in the moving average.
        deadband (float): Relative throughput error that does not require a concurrency change.
        maxStep (int): Largest thread-count change between collection passes.
    """

    minThreads: int = 1
    initialThreads: int = 1
    targetThroughput: float | None = None
    proportionalGain: float = 1.0
    integralGain: float = 0.1
    derivativeGain: float = 0.05
    smoothing: float = 0.3
    deadband: float = 0.1
    maxStep: int = 2

    def __attrs_post_init__(self) -> None:
        """
        Reject invalid controller gains, concurrency bounds, and measurement settings.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If settings cannot define a bounded PID controller.
        """
        if self.minThreads < 1 or self.initialThreads < self.minThreads or self.maxStep < 1:
            raise ValueError('Invalid collection thread limits')
        gains = (self.proportionalGain, self.integralGain, self.derivativeGain)
        if any(not isfinite(gain) or gain < 0 for gain in gains):
            raise ValueError('Collection PID gains must be finite and nonnegative')
        if not 0 < self.smoothing <= 1 or not 0 <= self.deadband < 1:
            raise ValueError('Invalid collection smoothing or deadband')
        if self.targetThroughput is not None and (not isfinite(self.targetThroughput) or self.targetThroughput <= 0):
            raise ValueError('Collection throughput target must be finite and positive')


@define
class Reconciliation:
    """
    Reconciliation configuration options.

    Attributes:
        interval (int): Seconds between reconciliation passes.
        collection (CollectionControl): PID settings for the per-core collection workers.
    """
    interval: int
    collection: CollectionControl = ib(factory=CollectionControl)


@define
class Resources:
    """
    Resource configuration options.

    Attributes:
        cpu (int): CPU resource target.
        memory (int): Memory resource target.
        storage (int): Storage resource target.
    """
    cpu: int
    memory: int
    storage: int


@define
class Host:
    """
    Host configuration options.

    Attributes:
        name (str): Configured resource name.
        address (str): Host or service address.
        protocol (str): Connection transport, such as SSH or TLS.
        port (int): Service port number.
        hypervisor (str): Hypervisor implementation selected for this host.
        sshKey (str | None): Private SSH key contents or an environment-variable reference.
        timeout (int): Connection timeout in seconds.
        user (str | None): Optional connection username.
        resources (Resources | None): Optional host resource limits.
    """
    name: str
    address: str
    protocol: str
    port: int
    hypervisor: str
    sshKey: str | None = ib(default=None, repr=False)  # Private key contents or an environment-variable reference.
    timeout: int = ib(default=45)
    user: str | None = ib(default=None)
    resources: Resources | None = ib(default=None)

    def __attrs_post_init__(self) -> None:
        """
        Post-initialization method to expand environment variables.

        Returns:
            None: No value is returned.
        """
        self.expand()

    def expand(self) -> None:
        """
        Expand environment variables in the host configuration.

        Returns:
            None: No value is returned.
        """
        if self.user:
            self.user = os.path.expandvars(self.user)

        if self.sshKey:
            self.sshKey = os.path.expandvars(self.sshKey)

    # Minor helper functions to reduce code duplication.
    def state(self) -> Dict:
        """
        Convert the host configuration to a flat database record-format.

        Returns:
            Dict: This instance as a database record.
        """
        host_dict = {
            'name': self.name,
            'address': self.address,
            'protocol': self.protocol,
            'port': self.port,
            'hypervisor': self.hypervisor,
            'cpu': self.resources.cpu if self.resources is not None and self.resources.cpu is not None else 0,
            'memory': self.resources.memory if self.resources is not None and self.resources.memory is not None else 0,
            'storage': self.resources.storage if self.resources is not None and self.resources.storage is not None else 0
        }

        return host_dict


@define
class CloudInit:
    """
    Cloud-init configuration options.

    Attributes:
        inline (str): Inline cloud-init user-data.
        file (str): Path to a cloud-init user-data file.
    """
    inline: str = ''
    file: str = ''


@define
class HostReplacementStrategy:
    """
    Host replacement strategy configuration options.

    Attributes:
        strategy (str): Host replacement strategy.
        maxUnavailable (int): Maximum unavailable hosts during replacement.
        maxSurge (int): Maximum extra hosts permitted during replacement.
    """
    strategy: str
    maxUnavailable: int
    maxSurge: int


@define
class Network:
    """
    Network configuration options.

    Attributes:
        type (str): Configured backend or networking mode.
        addresses (List[str]): Reserved static worker IP addresses.
        gateway (str): Network gateway address.
        subnet (str): Worker subnet in CIDR notation.
    """
    type: str
    addresses: List[str]
    gateway: str  # 192.168.1.1
    subnet: str   # 192.168.1.0


@define
class ScaleStrategy:
    """
    Scale strategy configuration options.

    Attributes:
        minNodes (int): Minimum desired node count.
        maxNodes (int): Maximum desired node count.
        increment (int): Standalone scaling step size.
        cooldown (int): Seconds between standalone scaling operations.
        method (str): Host selection method.
        resourceTarget (Resources): Resource utilization targets for scaling.
    """
    minNodes: int
    maxNodes: int
    increment: int
    cooldown: int
    method: str
    resourceTarget: Resources


@define
class AutoscalingGroup:
    """
    Autoscale group configuration options.

    Attributes:
        image (str): Source template disk path.
        name (str): Configured resource name.
        imageMigration (str): Template image availability or migration policy.
        cloudInit (CloudInit): Worker bootstrap user-data settings.
        hosts (List[Host | str]): Configured hosts or references by name.
        replacement (HostReplacementStrategy): Host replacement settings.
        networking (Network): Worker network settings.
        scaling (ScaleStrategy): Node count limits and host selection settings.
        nodeLabels (Dict[str, str]): Labels advertised on the autoscaler template node.
        nodeTaints (List[Dict[str, str]]): Taints advertised on the autoscaler template node.
        maxPods (int): Pod capacity advertised on each template node.
    """
    image: str
    name: str
    imageMigration: str
    cloudInit: CloudInit
    hosts: List[Host | str]
    replacement: HostReplacementStrategy
    networking: Network
    scaling: ScaleStrategy
    nodeLabels: Dict[str, str] = ib(factory=dict)
    nodeTaints: List[Dict[str, str]] = ib(factory=list)
    maxPods: int = 110


AutoscalingGroups = Dict[str, AutoscalingGroup]


@define
class Autoscale:
    """
    Autoscale configuration options.

    Attributes:
        hosts (List[Host]): Configured hosts or references by name.
        groups (AutoscalingGroups): Autoscaling groups indexed by name.
    """
    hosts: List[Host]
    groups: AutoscalingGroups


@define
class Healthcheck:
    """
    Healthcheck configuration options.

    Attributes:
        host (str): Configured host name or bind address.
        port (int): Service port number.
        apiPort (int): Separate Flask metrics and readiness port in Kubernetes modes.
        stateDirectory (str): Parent directory for private shared runtime snapshots.
    """
    host: str
    port: int
    apiPort: int = 9090
    stateDirectory: str = ib(factory=lambda: os.getenv('PREMISCALE_STATE_DIRECTORY', '/opt/premiscale/status'))


@define
class Kubernetes:
    """
    Kubernetes autoscaler connection options.

    Attributes:
        autoscalerPort (int): Legacy autoscaler service port.
        autoscalerHost (str): Legacy autoscaler service hostname.
        providerHost (str): Bind address for the public external gRPC provider.
        providerPort (int): Port for the public external gRPC provider.
        clusterName (str): Persistent cluster ownership identity.
        stateFile (str): Path to the autoscaler operation journal.
        stateDsn (str): PostgreSQL journal DSN; required for HA and overrides stateFile.
        namespace (str): Namespace containing this controller's custom resources.
        providerCert (str): Optional server TLS certificate path.
        providerKey (str): Optional server TLS private key path.
        clientCA (str): Optional CA for authenticating gRPC clients.
    """
    autoscalerPort: int
    autoscalerHost: str
    providerHost: str = '127.0.0.1'
    providerPort: int = 50051
    clusterName: str = 'premiscale'
    stateFile: str = '/opt/premiscale/autoscaler.db'
    stateDsn: str = ib(default='', converter=_expand_environment, repr=False)
    namespace: str = ib(factory=lambda: os.getenv('PREMISCALE_NAMESPACE', 'default'))
    providerCert: str = ''
    providerKey: str = ''
    clientCA: str = ''


@define
class Broker:
    """
    Redis-compatible queue service shared by independently deployed workers.

    Attributes:
        url (str): Backend connection URL.
        namespace (str): Namespace shared by a controller and its queue consumers.
        leaseSeconds (int): Delivery ownership lease duration in seconds.
        blockMilliseconds (int): Maximum blocking stream-read duration in milliseconds.
        connectTimeout (float): Broker connection timeout in seconds.
        socketTimeout (float): Broker socket timeout in seconds.
        caFile (str): Optional CA certificate path for TLS broker connections.
    """

    url: str = ib(factory=lambda: os.getenv('PREMISCALE_REDIS_URL', 'redis://dragonfly:6379/0'), repr=False)
    namespace: str = ib(factory=lambda: os.getenv('PREMISCALE_QUEUE_NAMESPACE', 'premiscale'))
    leaseSeconds: int = 60
    blockMilliseconds: int = 1000
    connectTimeout: float = 5
    socketTimeout: float = 5
    caFile: str = ''

    def __attrs_post_init__(self) -> None:
        """
        Normalize and validate the initialized configuration.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the URL scheme, namespace, lease, or connection and socket timeouts are invalid.
        """
        self.url = os.path.expandvars(self.url)
        self.namespace = os.path.expandvars(self.namespace)
        self.caFile = os.path.expandvars(os.path.expanduser(self.caFile))
        if urlsplit(self.url).scheme not in {'redis', 'rediss'}:
            raise ValueError('Broker URL must use redis:// or rediss://')
        if not self.namespace or any(character in self.namespace for character in '{}'):
            raise ValueError('Broker namespace must be nonempty and cannot contain braces')
        if self.leaseSeconds < 3 or self.blockMilliseconds < 1 or self.connectTimeout <= 0:
            raise ValueError('Broker lease and connection timeouts must be positive')
        if self.socketTimeout <= self.blockMilliseconds / 1000:
            raise ValueError('Broker socketTimeout must exceed blockMilliseconds')


@define
class Kafka:
    """
    Configure the durable metrics topic and per-database consumer groups.

    Attributes:
        enabled (bool): Use Kafka instead of Redis streams for metrics batches.
        bootstrapServers (str): Comma-separated broker addresses.
        topic (str): Metrics topic shared by all database subscribers.
        groupPrefix (str): Stable namespace for independent subscriber consumer groups.
        options (dict[str, str]): Additional librdkafka TLS or SASL connection properties.
    """

    enabled: bool = False
    bootstrapServers: str = ib(default='', converter=_expand_environment)
    topic: str = 'premiscale.metrics'
    groupPrefix: str = ib(factory=lambda: os.getenv('PREMISCALE_KAFKA_GROUP_PREFIX', 'premiscale'))
    options: dict[str, str] = ib(factory=dict, repr=False)

    def __attrs_post_init__(self) -> None:
        """
        Validate broker identity and expand secret-backed connection options.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If enabled Kafka lacks bootstrap servers or has invalid topic/group names.
        """
        if self.enabled and not self.bootstrapServers:
            raise ValueError('Kafka requires bootstrapServers')
        for value in (self.topic, self.groupPrefix):
            if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,180}', value):
                raise ValueError('Invalid Kafka topic or consumer group prefix')
        self.options = {key: os.path.expandvars(value) for key, value in self.options.items()}


@define
class Controller:
    """
    Controller configuration options.

    Attributes:
        mode (str): Controller mode selecting the required service processes.
        pidFile (str): Configured controller PID file path.
        databases (Databases): State and metrics backend settings.
        platform (Platform): Platform registration settings.
        reconciliation (Reconciliation): Reconciliation scheduling and collection control settings.
        autoscale (Autoscale): Hosts and autoscaling groups.
        healthcheck (Healthcheck): HTTP API listener settings.
        kubernetes (Kubernetes): Kubernetes provider settings.
        broker (Broker): Shared work-queue connection settings.
        kafka (Kafka): Optional Kafka metrics transport, required in HA modes.
    """
    mode: str
    pidFile: str
    databases: Databases
    platform: Platform
    reconciliation: Reconciliation
    autoscale: Autoscale
    healthcheck: Healthcheck
    kubernetes: Kubernetes
    broker: Broker = ib(factory=Broker)
    kafka: Kafka = ib(factory=Kafka)

    def __attrs_post_init__(self) -> None:
        """
        Validate concurrency settings against the per-process connection limit.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If collection concurrency exceeds its maximum or Kopf and Flask listener ports conflict.
        """
        if self.reconciliation.collection.initialThreads > self.databases.maxHostConnectionThreads:
            raise ValueError('Collection initialThreads must not exceed maxHostConnectionThreads')
        if self.mode.startswith('kubernetes') and self.healthcheck.port == self.healthcheck.apiPort:
            raise ValueError('Kopf and Flask must use distinct listener ports')


@define
class Config:
    """
    Parse config files of version v1alpha1.

    Attributes:
        version (str): Configuration schema version.
        controller (Controller): Controller configuration.
    """
    version: str
    controller: Controller

    @classmethod
    def from_dict(cls, config: dict) -> Config:
        """
        Create a Config object from a dictionary.

        Args:
            config (dict): The config dictionary.

        Returns:
            Config: The Config object.
        """
        return _converter.structure(
            config,
            cls
        )


_converter = Converter()
for _backend in (SQLiteState, MySQLState, LocalMetrics, PostgreSQLMetrics, InfluxDBMetrics):
    _converter.register_structure_hook(
        _backend, make_dict_structure_fn(_backend, _converter, _cattrs_forbid_extra_keys=True)
    )
_converter.register_structure_hook(
    Host | str,
    lambda value, _: value if isinstance(value, str) else _converter.structure(value, Host)
)
_converter.register_structure_hook(
    AutoscalingGroup,
    make_dict_structure_fn(
        AutoscalingGroup,
        _converter,
        cloudInit=override(rename='cloud-init')
    )
)

"""
Project a selected ControllerConfig and its dependencies into a watched ConfigMap.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
import json
import os
from typing import TYPE_CHECKING
from urllib.parse import quote

from attrs import define, field
import kopf
from requests import RequestException
from ruamel.yaml import YAML, YAMLError

from premiscale.config.projection import project
from premiscale.config.v1alpha1 import Config
from premiscale.connections.kubernetes import KubernetesClient
from premiscale.daemon.settings import Execution

if TYPE_CHECKING:
    from typing import Any


@define
class Projector:
    """
    Reconcile one pre-existing ConfigMap while retaining the last valid configuration.

    Attributes:
        config (Config): Running controller's identity and service contract.
        source (str): Selected ControllerConfig name in the operator namespace.
        target (str): Existing runtime ConfigMap owned by this configuration source.
        execution (Execution): Current workload composition and publisher delegation.
        client (KubernetesClient): Bounded Kubernetes API transport owned by the operator process.
    """

    config: Config
    source: str
    target: str
    execution: Execution
    client: KubernetesClient = field(factory=KubernetesClient)

    def read(self, path: str) -> dict[str, Any]:
        """
        Fetch one Kubernetes resource or list through the authenticated transport.

        Args:
            path (str): Namespaced API path.

        Returns:
            dict[str, Any]: Decoded resource after checking its HTTP status.
        """
        response = self.client.request('GET', path)
        response.raise_for_status()
        return response.json()

    def validate(self, data: dict[str, Any]) -> None:
        """
        Check runtime invariants without publishing expanded secrets or opening databases.

        Args:
            data (dict[str, Any]): Raw projected runtime configuration.

        Returns:
            None: No value is returned when the projection can be rolled safely.

        Raises:
            ValueError: If configuration is invalid or requires a coordinated Helm update.
        """
        try:
            proposed = Config.from_dict(data)
            self.execution.validate(proposed)
        except (Exception, SystemExit) as error:
            raise ValueError('Projected configuration failed runtime validation') from error
        before, after = self.config.controller, proposed.controller
        fixed = [
            (before.mode, after.mode),
            (before.healthcheck.host, after.healthcheck.host),
            (before.healthcheck.port, after.healthcheck.port),
            (before.healthcheck.apiPort, after.healthcheck.apiPort),
            (before.healthcheck.stateDirectory, after.healthcheck.stateDirectory),
            (before.kubernetes.namespace, after.kubernetes.namespace),
            (before.kubernetes.clusterName, after.kubernetes.clusterName),
            (before.kubernetes.providerHost, after.kubernetes.providerHost),
            (before.kubernetes.providerPort, after.kubernetes.providerPort),
            (before.kubernetes.stateDsn, after.kubernetes.stateDsn),
            (before.kubernetes.stateFile, after.kubernetes.stateFile),
            (before.broker.url, after.broker.url),
            (before.broker.namespace, after.broker.namespace),
            (before.kafka.bootstrapServers, after.kafka.bootstrapServers),
            (before.kafka.enabled, after.kafka.enabled),
            (before.kafka.topic, after.kafka.topic),
            (before.kafka.groupPrefix, after.kafka.groupPrefix),
        ]
        if any(old != new for old, new in fixed):
            raise ValueError('Service listeners, cluster/journal identity, and transport destinations require a coordinated Helm update')

    def reconcile(self) -> tuple[str, int]:
        """
        Update only changed valid data with a ConfigMap resource-version precondition.

        Returns:
            tuple[str, int]: Configuration digest and source generation after successful reconciliation.

        Raises:
            ValueError: If references, configuration, or ConfigMap ownership are invalid.
        """
        namespace = quote(self.config.controller.kubernetes.namespace, safe='')
        base = f'/apis/premiscale.com/v1alpha1/namespaces/{namespace}'
        source = self.read(f'{base}/controllerconfigs/{quote(self.source, safe="")}')
        hosts = {item['metadata']['name']: item['spec'] for item in self.read(f'{base}/hosts')['items']}
        groups = {item['metadata']['name']: item['spec'] for item in self.read(f'{base}/autoscalinggroups')['items']}
        data = project(source['spec'], hosts, groups)
        self.validate(data)
        digest = sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        path = f'/api/v1/namespaces/{namespace}/configmaps/{quote(self.target, safe="")}'
        current = self.read(path)
        annotations = current['metadata'].get('annotations', {})
        if current.get('immutable') or annotations.get('premiscale.com/controller-config') != self.source:
            raise ValueError('Projection requires a mutable ConfigMap assigned to this ControllerConfig')
        try:
            previous = YAML(typ='safe').load(current.get('data', {}).get('config.yaml', ''))
        except YAMLError:
            previous = None
        if previous != data:
            output = StringIO()
            YAML().dump(data, output)
            response = self.client.request('PATCH', path, {
                'metadata': {'resourceVersion': current['metadata']['resourceVersion'],
                             'annotations': {'premiscale.com/configuration-digest': digest}},
                'data': {'config.yaml': output.getvalue()},
            })
            response.raise_for_status()
        return digest, int(source['metadata'].get('generation', 1))


def register_projection(registry: kopf.OperatorRegistry, config: Config) -> None:
    """
    Install periodic reconciliation only for charts selecting a ControllerConfig source.

    Args:
        registry (kopf.OperatorRegistry): Process-local Kopf registry.
        config (Config): Running controller configuration used to guard deployment-coupled settings.

    Returns:
        None: No value is returned.
    """
    source = os.getenv('PREMISCALE_CONTROLLER_CONFIG', '')
    target = os.getenv('PREMISCALE_PROJECTED_CONFIGMAP', '')
    if not source or not target:
        return
    execution = Execution(mode=os.getenv('PREMISCALE_DEPLOYMENT_MODE', 'singular'),
                          connections=int(os.getenv('PREMISCALE_PUBLISHER_CONNECTIONS', '2')),
                          publishers=tuple(name for name in os.getenv('PREMISCALE_REMOTE_PUBLISHERS', '').split(',') if name),
                          publisher_limits=json.loads(os.getenv('PREMISCALE_PUBLISHER_LIMITS', '{}')))
    projector = Projector(config, source, target, execution)

    @kopf.on.cleanup(id='configuration-client', registry=registry)
    async def cleanup(**kwargs: Any) -> None:
        """
        Close the projection HTTP session when the elected operator stops.

        Args:
            **kwargs (Any): Additional Kopf lifecycle context.

        Returns:
            None: No value is returned.
        """
        projector.client.close()

    @kopf.timer('premiscale.com', 'v1alpha1', 'controllerconfigs', id='project-configuration', interval=5, registry=registry,
                when=lambda name, **_: name == source)
    async def reconcile(name: str, meta: kopf.Meta, status: kopf.Status, patch: kopf.Patch, **kwargs: Any) -> None:
        """
        Refresh the selected resource and dependencies, preserving valid data on errors.

        Args:
            name (str): ControllerConfig metadata name.
            meta (kopf.Meta): Resource generation tracked by Kopf.
            status (kopf.Status): Previously published projection status.
            patch (kopf.Patch): Status changes submitted through the status subresource.
            **kwargs (Any): Additional Kopf timer context.

        Returns:
            None: No value is returned.
        """
        if name != source:
            return
        generation = int(meta.get('generation', 1))
        condition: dict[str, Any]
        try:
            digest, generation = await asyncio.to_thread(projector.reconcile)
            condition = {'type': 'Ready', 'status': 'True', 'reason': 'ConfigurationProjected',
                         'message': 'Runtime ConfigMap contains the validated configuration.'}
            if status.get('configurationDigest') != digest:
                patch.status['configurationDigest'] = digest
        except ValueError as error:
            condition = {'type': 'Ready', 'status': 'False', 'reason': 'InvalidConfiguration', 'message': str(error)}
        except (RequestException, OSError):
            condition = {'type': 'Ready', 'status': 'False', 'reason': 'ProjectionUnavailable',
                         'message': 'Kubernetes API resources are unavailable; retaining the previous configuration.'}
        condition['observedGeneration'] = generation
        prior: dict[str, Any] = next((item for item in status.get('conditions', []) if item.get('type') == 'Ready'), {})
        transition = prior.get('lastTransitionTime') if prior.get('status') == condition['status'] else None
        condition['lastTransitionTime'] = transition or datetime.now(timezone.utc).isoformat()
        desired = {'conditions': [condition], 'observedGeneration': generation,
                   'configMapRef': {'name': target, 'key': 'config.yaml'}}
        for key, value in desired.items():
            if status.get(key) != value:
                patch.status[key] = value

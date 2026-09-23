"""
Exercise Kopf itself against a local Kubernetes API and its HTTP probe listener.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
import json
import os
from pathlib import Path
import signal
import socket
from types import SimpleNamespace
from typing import TYPE_CHECKING

import aiohttp
from aiohttp import web
import kopf
import pytest
from jsonschema import Draft7Validator
from ruamel.yaml import YAML

from premiscale.operator.handlers import build_registry
from premiscale.status.groups import configuration_digest
from premiscale.status.store import StatusStore
from .test_configuration_projection import projection

if TYPE_CHECKING:
    from typing import Any
    from premiscale.operator.configuration import Projector


@pytest.mark.parametrize('project_configuration', [False, True])
def test_kopf_patches_status_subresource_and_serves_shared_health(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                              projection: tuple[Projector, dict[str, Any], list[dict[str, Any]]],
                                                              project_configuration: bool) -> None:
    """
    Use real Kopf discovery, watches, patch routing, probing, and graceful stop.

    Args:
        tmp_path (Path): Shared snapshot directory.
        monkeypatch (pytest.MonkeyPatch): Projection mode and API transport overrides.
        projection (tuple[Projector, dict[str, Any], list[dict[str, Any]]]): Real projection logic with a disposable API transport.
        project_configuration (bool): Enable live ControllerConfig reconciliation alongside ASG status.

    Returns:
        None: No value is returned.
    """
    projector, objects, config_patches = projection
    monkeypatch.delenv('PREMISCALE_CONTROLLER_CONFIG', raising=False)
    monkeypatch.delenv('PREMISCALE_PROJECTED_CONFIGMAP', raising=False)
    if project_configuration:
        monkeypatch.setenv('PREMISCALE_CONTROLLER_CONFIG', 'primary')
        monkeypatch.setenv('PREMISCALE_PROJECTED_CONFIGMAP', 'runtime')
        monkeypatch.setattr('premiscale.operator.configuration.Projector', lambda *_args: projector)
        objects['controllerconfigs/primary']['spec']['reconciliation']['interval'] = 120

    async def exercise() -> None:
        """
        Start a mock API and operator on local sockets, then verify their contract.

        Returns:
            None: No value is returned.

        Raises:
            AssertionError: If the operator exits before publishing its resource status.
        """
        root = Path(__file__).resolve().parents[3]
        values = YAML(typ='safe').load(root / 'charts/premiscale-crds/examples/multiple-resources.yaml')
        spec = values['autoscalingGroups']['workers-primary']['spec']
        body = {'apiVersion': 'premiscale.com/v1alpha1', 'kind': 'AutoscalingGroup', 'spec': spec,
                'metadata': {'name': 'workers-primary', 'namespace': 'test', 'uid': 'group-uid',
                             'generation': 1, 'resourceVersion': '1', 'labels': {'premiscale.com/cluster': 'test'}}}
        controller = {'apiVersion': 'premiscale.com/v1alpha1', 'kind': 'ControllerConfig',
                      'spec': objects['controllerconfigs/primary']['spec'],
                      'metadata': {'name': 'primary', 'namespace': 'test', 'uid': 'controller-uid',
                                   'generation': 1, 'resourceVersion': '1'}}
        patches: list[tuple[str, dict[str, Any]]] = []
        store = StatusStore(tmp_path, cache_seconds=0)
        store.write('autoscaler', {'groups': {'workers-primary': {
            'configurationDigest': configuration_digest(spec), 'targetReplicas': 2, 'runningReplicas': 2,
            'pendingReplicas': 0, 'deletingReplicas': 0, 'failedReplicas': 0,
        }}})
        stopped = asyncio.Event()
        release_watch = asyncio.Event()

        async def api(request: web.Request) -> web.Response | web.StreamResponse:
            """
            Serve discovery and one namespaced resource without a real cluster.

            Args:
                request (web.Request): Incoming request from Kopf's Kubernetes client.

            Returns:
                web.Response | web.StreamResponse: Discovery, resource, patch, or watch response.
            """
            path = request.path
            if request.method == 'PATCH':
                data = await request.json()
                patches.append((path, data))
                current = deepcopy(controller if '/controllerconfigs/' in path else body)
                current.update(data)
                return web.json_response(current)
            if path == '/api':
                return web.json_response({'kind': 'APIVersions', 'versions': ['v1']})
            if path == '/apis':
                return web.json_response({'kind': 'APIGroupList', 'groups': [{
                    'name': 'premiscale.com', 'versions': [{'groupVersion': 'premiscale.com/v1alpha1', 'version': 'v1alpha1'}],
                    'preferredVersion': {'groupVersion': 'premiscale.com/v1alpha1', 'version': 'v1alpha1'},
                }]})
            if path == '/api/v1':
                return web.json_response({'kind': 'APIResourceList', 'groupVersion': 'v1', 'resources': [
                    {'name': 'namespaces', 'singularName': 'namespace', 'namespaced': False,
                     'kind': 'Namespace', 'verbs': ['get', 'list', 'watch']},
                ]})
            if path == '/apis/premiscale.com/v1alpha1':
                resources = [{'name': 'autoscalinggroups', 'singularName': 'autoscalinggroup', 'namespaced': True,
                              'kind': 'AutoscalingGroup', 'verbs': ['get', 'list', 'watch', 'patch']},
                             {'name': 'autoscalinggroups/status', 'namespaced': True, 'kind': 'AutoscalingGroup',
                              'verbs': ['get', 'patch', 'update']}]
                if project_configuration:
                    resources.extend([
                        {'name': 'controllerconfigs', 'singularName': 'controllerconfig', 'namespaced': True,
                         'kind': 'ControllerConfig', 'verbs': ['get', 'list', 'watch', 'patch']},
                        {'name': 'controllerconfigs/status', 'namespaced': True, 'kind': 'ControllerConfig',
                         'verbs': ['get', 'patch', 'update']},
                    ])
                return web.json_response({'kind': 'APIResourceList', 'groupVersion': 'premiscale.com/v1alpha1', 'resources': resources})
            if path in {'/apis/premiscale.com/v1alpha1/namespaces/test/autoscalinggroups',
                        '/apis/premiscale.com/v1alpha1/namespaces/test/controllerconfigs'}:
                if request.query.get('watch') == 'true':
                    response = web.StreamResponse(headers={'Content-Type': 'application/json'})
                    await response.prepare(request)
                    await release_watch.wait()
                    return response
                item = controller if path.endswith('/controllerconfigs') else body
                return web.json_response({'apiVersion': 'premiscale.com/v1alpha1', 'kind': f'{item["kind"]}List',
                                          'metadata': {'resourceVersion': '1'}, 'items': [item]})
            return web.json_response({'kind': 'Status', 'code': 404}, status=404)

        application = web.Application()
        application.router.add_route('*', '/{tail:.*}', api)
        runner = web.AppRunner(application, shutdown_timeout=1)
        await runner.setup()
        api_socket = socket.socket()
        api_socket.bind(('127.0.0.1', 0))
        api_socket.listen()
        api_port = api_socket.getsockname()[1]
        await web.SockSite(runner, api_socket).start()
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            probe_port = reservation.getsockname()[1]
        config: Any = SimpleNamespace(controller=SimpleNamespace(kubernetes=SimpleNamespace(clusterName='test')))
        if project_configuration:
            config = projector.config
        registry = build_registry(config, store)

        @kopf.on.login(registry=registry)
        async def login(**kwargs: Any) -> kopf.ConnectionInfo:
            """
            Authenticate solely against the disposable test API.

            Args:
                **kwargs (Any): Additional framework context.

            Returns:
                kopf.ConnectionInfo: Credentials for the local mock API.
            """
            return kopf.ConnectionInfo(server=f'http://127.0.0.1:{api_port}', token='test-only')

        task = asyncio.create_task(kopf.operator(registry=registry, standalone=True, namespaces=['test'],
                                                stop_flag=stopped, liveness_endpoint=f'http://127.0.0.1:{probe_port}/healthz'))
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
                async with asyncio.timeout(15):
                    while True:
                        if task.done():
                            await task
                            raise AssertionError('Operator exited before publishing status')
                        try:
                            async with client.get(f'http://127.0.0.1:{probe_port}/healthz') as response:
                                assert response.status == 500
                                break
                        except aiohttp.ClientConnectorError:
                            await asyncio.sleep(0.05)
                    store.write('supervisor', {'phase': 'running', 'services': {'operator': {'pid': os.getpid(), 'required': True}}})
                    store.write('process-operator', {'phase': 'ready'})
                    async with client.get(f'http://127.0.0.1:{probe_port}/healthz') as response:
                        assert response.status == 200
                        assert (await response.json())['controller']['status'] == 'OK'
                    while not any('/autoscalinggroups/' in path and path.endswith('/status') for path, _ in patches):
                        await asyncio.sleep(0.05)
                    if project_configuration:
                        while not any('/controllerconfigs/' in path and path.endswith('/status') for path, _ in patches):
                            await asyncio.sleep(0.05)
                        assert len(config_patches) == 1
                        objects['hosts']['items'][0]['spec']['timeout'] = 42
                        while len(config_patches) < 2:
                            await asyncio.sleep(0.05)
            path, patch = next((path, patch) for path, patch in patches if '/autoscalinggroups/' in path and path.endswith('/status'))
            assert path.endswith('/namespaces/test/autoscalinggroups/workers-primary/status')
            assert patch['status']['runningReplicas'] == 2
            assert patch['status']['observedGeneration'] == 1
            schema = json.loads((root / 'charts/premiscale-crds/schemas/AutoscalingGroup.json').read_text())
            Draft7Validator(schema['properties']['status']).validate(patch['status'])
            if project_configuration:
                change = next(patch for path, patch in patches if '/controllerconfigs/' in path and path.endswith('/status'))
                schema = json.loads((root / 'charts/premiscale-crds/schemas/ControllerConfig.json').read_text())
                Draft7Validator(schema['properties']['status']).validate(change['status'])
                assert change['status']['conditions'][0]['status'] == 'True'
                latest = YAML(typ='safe').load(config_patches[-1]['data']['config.yaml'])
                assert latest['controller']['autoscale']['hosts'][0]['timeout'] == 42
        finally:
            stopped.set()
            release_watch.set()
            try:
                await asyncio.wait_for(task, timeout=5)
            finally:
                await runner.cleanup()

    handlers = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        asyncio.run(exercise())
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)

"""
Verify libvirt ownership, cloning and deletion without touching real machines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from pathlib import Path

from unittest.mock import Mock
from xml.etree import ElementTree as ET

import libvirt
import pytest

from premiscale_cluster_autoscaler.libvirt import LibvirtDriver, NAMESPACE
from premiscale.connections.ssh import configure_ssh
from premiscale_cluster_autoscaler.state import Instance
from tests.unit.test_cluster_autoscaler import config as config

if TYPE_CHECKING:
    from typing import Any


DATA = Path(__file__).resolve().parents[1] / 'data/libvirt'
TEMPLATE = (DATA / 'domain.xml').read_text(encoding='utf-8')


class Missing(libvirt.libvirtError):
    """
    Represent a specific libvirt lookup error.
    """

    def __init__(self, code: int) -> None:
        """
        Initialize Missing with the supplied settings.

        Args:
            code (int): Exit status or libvirt error code to simulate.
        """
        super().__init__('not found')
        self.code = code

    def get_error_code(self) -> int:
        """
        Return the simulated libvirt lookup error code.

        Returns:
            int: The simulated libvirt lookup error code.
        """
        return self.code


@pytest.fixture
def infrastructure(config: Any, monkeypatch: pytest.MonkeyPatch) -> tuple[LibvirtDriver, Mock, dict[str, Mock], dict[str, Mock], Mock]:
    """
    Model libvirt objects closely enough to verify cross-object ownership.

    Args:
        config (Any): Parsed controller configuration.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for restoring patched attributes and environment variables.

    Returns:
        tuple[LibvirtDriver, Mock, dict[str, Mock], dict[str, Mock], Mock]: Driver and fake libvirt objects used to inspect volume and domain mutations.
    """
    connection = Mock()
    pool = Mock()
    volumes: dict[str, Mock] = {}
    domains: dict[str, Mock] = {}
    pool.XMLDesc.return_value = (DATA / 'pool.xml').read_text(encoding='utf-8')
    pool.info.return_value = [2, 100 * 1024**3, 0, 100 * 1024**3]
    connection.getFreeMemory.return_value = 8 * 1024**3
    template = Mock()
    template.XMLDesc.return_value = TEMPLATE
    template.isActive.return_value = False
    connection.lookupByName.return_value = template

    def create_volume(xml: str, *_args: Any) -> Mock:
        """
        Create a fake storage volume from its XML definition.

        Args:
            xml (str): XML representation of the fake libvirt object.
            *_args (Any): Unused positional arguments supplied by the caller.

        Returns:
            Mock: A fake storage volume from its XML definition.
        """
        root = ET.fromstring(xml)
        name = root.findtext('name')
        capacity = root.findtext('capacity')
        assert name is not None and capacity is not None
        path = '/images/' + name
        volume = Mock()
        volume.XMLDesc.return_value = xml
        volume.path.return_value = path
        volume.info.return_value = [0, int(capacity), 0]
        volume.storagePoolLookupByVolume.return_value = pool
        volume.delete.side_effect = lambda _: volumes.pop(path)
        volumes[path] = volume
        return volume

    create_volume((DATA / 'volume.xml').read_text(encoding='utf-8'))
    pool.createXML.side_effect = create_volume
    pool.createXMLFrom.side_effect = create_volume
    pool.listAllVolumes.side_effect = lambda _: list(volumes.values())
    connection.listAllStoragePools.return_value = [pool]

    def find_volume(path: str) -> Mock:
        """
        Find a fake volume or raise the libvirt missing-volume error.

        Args:
            path (str): Filesystem path to the requested resource.

        Returns:
            Mock: A fake volume or raise the libvirt missing-volume error.

        Raises:
            Missing: If the path does not identify a fake storage volume.
        """
        if path not in volumes:
            raise Missing(libvirt.VIR_ERR_NO_STORAGE_VOL)
        return volumes[path]

    def find_domain(identity: str) -> Mock:
        """
        Find a fake domain or raise the libvirt missing-domain error.

        Args:
            identity (str): Stable identifier of the managed resource.

        Returns:
            Mock: A fake domain or raise the libvirt missing-domain error.

        Raises:
            Missing: If the UUID does not identify a fake domain.
        """
        if identity not in domains:
            raise Missing(libvirt.VIR_ERR_NO_DOMAIN)
        return domains[identity]

    def define(xml: str) -> Mock:
        """
        Define a fake domain with mutable lifecycle state.

        Args:
            xml (str): XML representation of the fake libvirt object.

        Returns:
            Mock: Define a fake domain with mutable lifecycle state.
        """
        root = ET.fromstring(xml)
        identity = root.findtext('uuid')
        assert identity is not None
        domain = Mock()
        domain.XMLDesc.return_value = xml
        domain.UUIDString.return_value = identity
        domain.name.return_value = root.findtext('name')
        domain.isActive.return_value = False
        domain.create.side_effect = lambda: setattr(domain.isActive, 'return_value', True)
        domain.destroy.side_effect = lambda: setattr(domain.isActive, 'return_value', False)
        domain.undefineFlags.side_effect = lambda _: domains.pop(identity)
        domains[identity] = domain
        return domain

    connection.lookupByUUIDString.side_effect = find_domain
    connection.storageVolLookupByPath.side_effect = find_volume
    connection.defineXML.side_effect = define
    connection.listAllDomains.side_effect = lambda _: list(domains.values())
    monkeypatch.setattr(libvirt, 'open', lambda _: connection)
    monkeypatch.setattr('premiscale_cluster_autoscaler.libvirt.configure_ssh',
                        lambda host: configure_ssh(host, Path(config.controller.kubernetes.stateFile).parent / '.ssh'))
    return LibvirtDriver(config), connection, volumes, domains, template


def test_clone_has_private_identity_disks_and_seed_and_delete_preserves_template(config: Any, infrastructure: Any) -> None:
    """
    Verify clone has private identity disks and seed and delete preserves template.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    node = Instance('3b06395a-b015-4fb2-86fb-b6c82f5c501c', 'worker-1', 'workers', 'host-1')
    driver.provision(node, config.controller.autoscale.groups['workers'])
    domain = domains[node.id]
    root = ET.fromstring(domain.XMLDesc(0))
    assert root.findtext('name') == node.name
    assert root.findtext('uuid') == node.id
    interface_mac = root.find('devices/interface/mac')
    assert interface_mac is not None
    assert interface_mac.get('address') != '52:54:00:00:00:01'
    assert root.find('devices/interface/target') is None
    ownership = root.find(f'metadata/{{{NAMESPACE}}}instance')
    assert ownership is not None
    assert ownership.get('group') == 'workers'
    assert len(volumes) == 3
    assert all(path == '/images/template.qcow2' or node.id in path for path in volumes)
    assert template.XMLDesc(0) == TEMPLATE
    assert driver.discover()[0].id == node.id
    driver.delete(node)
    assert not domains
    assert list(volumes) == ['/images/template.qcow2']
    template.destroy.assert_not_called()


def test_deletion_refuses_an_unmarked_vm(config: Any, infrastructure: Any) -> None:
    """
    Verify deletion refuses an unmarked vm.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    node = Instance('foreign-id', 'foreign', 'workers', 'host-1')
    domains[node.id] = template
    with pytest.raises(ValueError, match='ownership'):
        driver.delete(node)
    template.destroy.assert_not_called()
    assert list(volumes) == ['/images/template.qcow2']


def test_retry_reuses_a_defined_owned_domain(config: Any, infrastructure: Any) -> None:
    """
    Verify retry reuses a defined owned domain.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    node = Instance('3b06395a-b015-4fb2-86fb-b6c82f5c501c', 'worker-1', 'workers', 'host-1')
    driver.provision(node, config.controller.autoscale.groups['workers'])
    domains[node.id].isActive.return_value = False
    driver.provision(node, config.controller.autoscale.groups['workers'])
    assert connection.defineXML.call_count == 1
    assert domains[node.id].create.call_count == 2
    assert len(volumes) == 3


def test_delete_removes_owned_orphan_volumes_after_a_crash(config: Any, infrastructure: Any) -> None:
    """
    Verify delete removes owned orphan volumes after a crash.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    node = Instance('3b06395a-b015-4fb2-86fb-b6c82f5c501c', 'worker-1', 'workers', 'host-1')
    driver.provision(node, config.controller.autoscale.groups['workers'])
    domains.clear()
    driver.delete(node)
    assert list(volumes) == ['/images/template.qcow2']


def test_active_template_is_not_cloned(config: Any, infrastructure: Any) -> None:
    """
    Verify active template is not cloned.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    template.isActive.return_value = True
    node = Instance('3b06395a-b015-4fb2-86fb-b6c82f5c501c', 'worker-1', 'workers', 'host-1')
    with pytest.raises(ValueError, match='shut off'):
        driver.provision(node, config.controller.autoscale.groups['workers'])
    assert not domains
    assert list(volumes) == ['/images/template.qcow2']


def test_volume_definition_is_accepted_by_libvirt_test_driver() -> None:
    """
    Verify volume definition is accepted by libvirt test driver.

    Returns:
        None: No value is returned.
    """
    connection = libvirt.open('test:///default')
    try:
        driver = object.__new__(LibvirtDriver)
        root = driver._volume_xml('premiscale-schema-check.img', 1048576, 'raw')
        pool = connection.listAllStoragePools()[0]
        volume = pool.createXML(ET.tostring(root, encoding='unicode'), 0)
        assert volume.name() == 'premiscale-schema-check.img'
        volume.delete(0)
    finally:
        connection.close()


def test_preexisting_unowned_volume_is_not_overwritten(config: Any, infrastructure: Any) -> None:
    """
    Verify preexisting unowned volume is not overwritten.

    Args:
        config (Any): Parsed controller configuration.
        infrastructure (Any): Fake driver, connection, volumes, domains, and template.

    Returns:
        None: No value is returned.
    """
    driver, connection, volumes, domains, template = infrastructure
    node = Instance('3b06395a-b015-4fb2-86fb-b6c82f5c501c', 'worker-1', 'workers', 'host-1')
    foreign_path = f'/images/premiscale-{node.id}-0.img'
    foreign = Mock()
    volumes[foreign_path] = foreign
    with pytest.raises(ValueError, match='preexisting volume'):
        driver.provision(node, config.controller.autoscale.groups['workers'])
    foreign.delete.assert_not_called()
    assert not domains

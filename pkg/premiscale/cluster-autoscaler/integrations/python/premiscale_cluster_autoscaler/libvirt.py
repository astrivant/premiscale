"""
Clone, discover and delete QEMU VMs owned by this autoscaler.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from io import BytesIO
from pathlib import Path, PurePosixPath
import tempfile
from typing import TYPE_CHECKING
from urllib.parse import quote
from xml.etree import ElementTree as ET

import libvirt

from premiscale.config.v1alpha1 import Host
from premiscale.connections.ssh import configure_ssh
from premiscale.connections.journal import Journal
from premiscale.schemas.qemu import OWNERSHIP_NAMESPACE as NAMESPACE
from premiscale_cluster_autoscaler.cloudinit import seed, mac_address
from premiscale_cluster_autoscaler.state import Instance

if TYPE_CHECKING:
    from typing import Any, Iterator
    from premiscale.config.v1alpha1 import Config, AutoscalingGroup


ET.register_namespace('premiscale', NAMESPACE)


def _xml(element: ET.Element) -> str:
    """
    Serialize libvirt XML as Unicode text.

    Args:
        element (ET.Element): XML element to serialize.

    Returns:
        str: Libvirt XML as Unicode text.
    """
    return ET.tostring(element, encoding='unicode')


class LibvirtDriver:
    """
    Manage only domains and volumes carrying this cluster's ownership mark.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize LibvirtDriver with the supplied settings.

        Args:
            config (Config): Parsed controller configuration.

        Raises:
            ValueError: If a group references an unknown host or conflicting definitions share a host name.
        """
        self.cluster = config.controller.kubernetes.clusterName
        self.journal = Path(config.controller.kubernetes.stateFile).expanduser()
        self.journal_dsn = getattr(config.controller.kubernetes, 'stateDsn', '')
        if not self.journal_dsn:
            self.journal.parent.mkdir(parents=True, exist_ok=True)
        self.groups = config.controller.autoscale.groups
        self.hosts = {host.name: host for host in config.controller.autoscale.hosts}
        self.group_hosts: dict[str, list[str]] = {}
        for name, group in self.groups.items():
            names = []
            for host in group.hosts:
                if isinstance(host, str):
                    if host not in self.hosts:
                        raise ValueError(f'Unknown host {host} for node group {name}')
                    names.append(host)
                else:
                    if host.name in self.hosts and self.hosts[host.name] != host:
                        raise ValueError(f'Conflicting host definitions for {host.name}')
                    self.hosts[host.name] = host
                    names.append(host.name)
            self.group_hosts[name] = names

    @contextmanager
    def connect(self, host: str) -> Iterator[Any]:
        """
        Open an authenticated libvirt connection using the configured host.

        Args:
            host (str): Configured hypervisor host name.

        Yields:
            Any: Resource available for the duration of the context.

        Raises:
            ValueError: Provisioning requires qemu over ssh or tls.
            ConnectionError: If libvirt does not return an open connection.
        """
        config = self.hosts[host]
        if config.hypervisor != 'qemu' or config.protocol.lower() not in {'ssh', 'tls'}:
            raise ValueError('Provisioning requires qemu over ssh or tls')
        configure_ssh(config)
        address = f'[{config.address}]' if ':' in config.address else config.address
        user = quote(config.user or 'root', safe='')
        uri = f'qemu+{config.protocol.lower()}://{user}@{address}:{config.port}/system'
        connection = libvirt.open(uri)
        if connection is None:
            raise ConnectionError(f'Could not connect to libvirt host {host}')
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _volumes(self) -> Iterator[Journal]:
        """
        Journal exact volume paths; libvirt volume XML has no ownership metadata.

        Yields:
            Journal: Resource available for the duration of the context.
        """
        connection = Journal(str(self.journal), self.cluster, self.journal_dsn)
        try:
            with connection.transaction():
                connection.execute('libvirt/create_volumes.sql')
                yield connection
        finally:
            connection.close()

    def _claim(self, instance: Instance, path: str, *, existing: bool, recovered: bool = False) -> None:
        """
        Record a new path before allocation, or recover it from owned domain XML.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.
            path (str): Filesystem path to the requested resource.
            existing (bool): Whether a volume already occupies the requested path.
            recovered (bool): Whether ownership was recovered from a managed domain.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the path identity is invalid, another owner holds it, or an existing volume is unclaimed.
        """
        if not PurePosixPath(path).name.startswith(f'premiscale-{instance.id}-'):
            raise ValueError('Managed volume path does not match the instance identity')
        with self._volumes() as journal:
            row = journal.execute('libvirt/get_volume_owner.sql', (instance.host, path)).fetchone()
            if row is not None and row != (self.cluster, instance.id):
                raise ValueError('Volume is owned by a different cluster or instance')
            if row is None and existing and not recovered:
                raise ValueError('Refusing to reuse a preexisting volume without an ownership record')
            journal.execute('libvirt/claim_volume.sql', (self.cluster, instance.id, instance.host, path))

    def _recover_volumes(self, root: ET.Element, instance: Instance) -> None:
        """
        Recover journaled volume ownership from managed domain metadata.

        Args:
            root (ET.Element): Parsed libvirt domain XML.
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.
        """
        mark = root.find(f'metadata/{{{NAMESPACE}}}instance')
        assert mark is not None
        for entry in mark.findall(f'{{{NAMESPACE}}}volume'):
            self._claim(instance, entry.attrib['path'], existing=True, recovered=True)

    def _domain(self, connection: Any, identity: str) -> Any:
        """
        Look up a domain by UUID, returning None when it does not exist.

        Args:
            connection (Any): Open libvirt connection to the target host.
            identity (str): Stable identifier of the managed resource.

        Returns:
            Any: A domain by UUID, returning None when it does not exist.

        Raises:
            libvirt.libvirtError: If the underlying operation fails after cleanup.
        """
        try:
            return connection.lookupByUUIDString(identity)
        except libvirt.libvirtError as error:
            if error.get_error_code() != libvirt.VIR_ERR_NO_DOMAIN:
                raise
            return None

    def _owned(self, domain: Any, instance: Instance) -> ET.Element:
        """
        Validate domain ownership and return its XML.

        Args:
            domain (Any): Libvirt domain whose ownership must be verified.
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            ET.Element: Parsed domain XML after all ownership fields match the requested instance.

        Raises:
            ValueError: Refusing to mutate a VM without matching ownership metadata.
        """
        root = ET.fromstring(domain.XMLDesc(0))
        mark = root.find(f'metadata/{{{NAMESPACE}}}instance')
        if mark is None or mark.get('cluster') != self.cluster or mark.get('group') != instance.group or mark.get('id') != instance.id:
            raise ValueError('Refusing to mutate a VM without matching ownership metadata')
        return root

    def discover(self) -> list[Instance]:
        """
        Read only marked domains on hosts assigned to configured groups.

        Returns:
            list[Instance]: Only marked domains on hosts assigned to configured groups.

        Raises:
            ValueError: If a managed domain refers to a removed group or has conflicting host or instance ownership.
        """
        instances = []
        for host in sorted({name for names in self.group_hosts.values() for name in names}):
            with self.connect(host) as connection:
                for domain in connection.listAllDomains(0):
                    root = ET.fromstring(domain.XMLDesc(0))
                    mark = root.find(f'metadata/{{{NAMESPACE}}}instance')
                    if mark is None or mark.get('cluster') != self.cluster:
                        continue
                    group = mark.get('group', '')
                    if group not in self.groups:
                        raise ValueError(f'Managed VM {domain.name()} belongs to removed group {group}')
                    if host not in self.group_hosts[group] or mark.get('id') != domain.UUIDString():
                        raise ValueError(f'Conflicting ownership metadata for {domain.name()}')
                    instance = Instance(domain.UUIDString(), domain.name(), group, host,
                                        mark.get('address', ''), 'running' if domain.isActive() else 'creating')
                    self._recover_volumes(root, instance)
                    instances.append(instance)
        return instances

    def _template(self, connection: Any, group: AutoscalingGroup) -> ET.Element:
        """
        Load and validate the shut-off template domain.

        Args:
            connection (Any): Open libvirt connection to the target host.
            group (AutoscalingGroup): Node group configuration or identifier.

        Returns:
            ET.Element: Parsed XML for the validated, inactive template domain.

        Raises:
            ValueError: If the template is running, has unsupported devices, or uses unsupported storage.
        """
        domain = connection.lookupByName(group.name)
        if domain.isActive():
            raise ValueError('The template domain must be shut off before cloning its disks')
        root = ET.fromstring(domain.XMLDesc(libvirt.VIR_DOMAIN_XML_INACTIVE))
        if root.find(f'metadata/{{{NAMESPACE}}}instance') is not None:
            raise ValueError('A managed node cannot also be a clone template')
        devices = root.find('devices')
        if devices is None or not devices.findall("disk[@device='disk']"):
            raise ValueError('Template must contain a managed file-backed disk')
        for tag in ('hostdev', 'filesystem', 'tpm'):
            if devices.find(tag) is not None:
                raise ValueError(f'Template device {tag} cannot be cloned safely')
        return root

    def template(self, name: str) -> dict[str, Any]:
        """
        Describe the template resources used by Kubernetes scheduling.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            dict[str, Any]: The template resources used by Kubernetes scheduling.

        Raises:
            ValueError: Template memory is missing.
        """
        group = self.groups[name]
        with self.connect(self.group_hosts[name][0]) as connection:
            root = self._template(connection, group)
            memory = root.find('memory')
            if memory is None or memory.text is None:
                raise ValueError('Template memory is missing')
            units = {'b': 1, 'bytes': 1, 'kb': 1000, 'kib': 1024, 'mb': 1000**2,
                     'mib': 1024**2, 'gb': 1000**3, 'gib': 1024**3}
            memory_bytes = int(memory.text) * units[memory.get('unit', 'KiB').lower()]
            disk = connection.storageVolLookupByPath(group.image)
            capacity = {'cpu': root.findtext('vcpu', '1'), 'memory': str(memory_bytes),
                        'ephemeral-storage': str(disk.info()[1]), 'pods': str(group.maxPods)}
            architecture = root.find('os/type')
            arch = architecture.get('arch', 'x86_64') if architecture is not None else 'x86_64'
            labels = {'kubernetes.io/os': 'linux', 'kubernetes.io/arch': {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(arch, arch),
                      'premiscale.com/node-group': name, **group.nodeLabels}
            return {'labels': labels, 'capacity': capacity, 'allocatable': capacity,
                    'taints': group.nodeTaints}

    def _volume(self, connection: Any, path: str) -> Any:
        """
        Look up a storage volume, returning None when it does not exist.

        Args:
            connection (Any): Open libvirt connection to the target host.
            path (str): Filesystem path to the requested resource.

        Returns:
            Any: A storage volume, returning None when it does not exist.

        Raises:
            libvirt.libvirtError: If the underlying operation fails after cleanup.
        """
        try:
            return connection.storageVolLookupByPath(path)
        except libvirt.libvirtError as error:
            if error.get_error_code() != libvirt.VIR_ERR_NO_STORAGE_VOL:
                raise
            return None

    def vacancies(self, name: str) -> dict[str, int]:
        """
        Estimate whole-template slots from free host memory and storage.

        Args:
            name (str): Name identifying the requested resource.

        Returns:
            dict[str, int]: Available whole-template VM slots indexed by host name.

        Raises:
            ValueError: Template must use a file-backed disk.
        """
        required = self.template(name)['capacity']
        result = {}
        for host in self.group_hosts[name]:
            with self.connect(host) as connection:
                root = self._template(connection, self.groups[name])
                source = root.find("devices/disk[@device='disk']/source")
                if source is None or not source.get('file'):
                    raise ValueError('Template must use a file-backed disk')
                volume = connection.storageVolLookupByPath(source.attrib['file'])
                pool = volume.storagePoolLookupByVolume()
                memory_slots = connection.getFreeMemory() // int(required['memory'])
                disk_slots = pool.info()[3] // int(required['ephemeral-storage'])
                result[host] = min(memory_slots, disk_slots)
        return result

    def _volume_xml(self, name: str, capacity: int, format_: str) -> ET.Element:
        """
        Build a filesystem-backed libvirt volume definition.

        Args:
            name (str): Name identifying the requested resource.
            capacity (int): Requested volume capacity in bytes.
            format_ (str): Libvirt disk format, such as raw or qcow2.

        Returns:
            ET.Element: A filesystem-backed libvirt volume definition.
        """
        root = ET.Element('volume')
        ET.SubElement(root, 'name').text = name
        ET.SubElement(root, 'capacity', unit='bytes').text = str(capacity)
        target = ET.SubElement(root, 'target')
        ET.SubElement(target, 'format', type=format_)
        return root

    def _upload(self, connection: Any, volume: Any, content: Any, length: int) -> None:
        """
        Stream binary contents into a libvirt storage volume.

        Args:
            connection (Any): Open libvirt connection to the target host.
            volume (Any): Libvirt storage volume receiving the data.
            content (Any): Binary reader containing the volume contents.
            length (int): Number of bytes to transfer.

        Returns:
            None: No value is returned.

        Raises:
            BaseException: If the underlying operation fails after cleanup.
        """
        stream = connection.newStream(0)
        try:
            volume.upload(stream, 0, length, 0)
            stream.sendAll(lambda _stream, size, reader: reader.read(size), content)
            stream.finish()
        except BaseException:
            stream.abort()
            raise

    def _clone(self, connection: Any, pool: Any, source: str, filename: str,
               instance: Instance, group: AutoscalingGroup) -> Any:
        """
        Create an owned private volume from a local or migrated template image.

        Args:
            connection (Any): Open libvirt connection to the target host.
            pool (Any): Destination libvirt storage pool.
            source (str): Path to the source template volume.
            filename (str): Name of the private destination volume.
            instance (Instance): Managed VM identity and requested lifecycle state.
            group (AutoscalingGroup): Node group configuration or identifier.

        Returns:
            Any: An owned private volume from a local or migrated template image.

        Raises:
            ValueError: If storage is unsuitable, a source image is unavailable, or a migrated image has backing storage.
            BaseException: If the underlying operation fails after cleanup.
        """
        pool_path = ET.fromstring(pool.XMLDesc(0)).findtext('target/path')
        if not pool_path:
            raise ValueError('Provisioning requires a filesystem-backed storage pool')
        destination = str(PurePosixPath(pool_path) / filename)
        existing = self._volume(connection, destination)
        self._claim(instance, destination, existing=existing is not None)
        if existing is not None:
            # A previous process may have died during the copy. Recreate until a
            # domain has been defined; provision() handles defined VMs separately.
            existing.delete(0)
        source_volume = self._volume(connection, source)
        if source_volume is not None:
            source_xml = ET.fromstring(source_volume.XMLDesc(0))
            format_ = source_xml.find('target/format')
            root = self._volume_xml(filename, source_volume.info()[1],
                                    format_.get('type', 'raw') if format_ is not None else 'raw')
            return pool.createXMLFrom(_xml(root), source_volume, 0)
        if group.imageMigration != 'migrate':
            raise ValueError(f'Centralized image is unavailable on host {instance.host}: {source}')
        # Transfer via libvirt streams; no SSH shell command or remote path interpolation.
        for host in self.group_hosts[instance.group]:
            if host == instance.host:
                continue
            with self.connect(host) as origin:
                self._template(origin, group)
                source_volume = self._volume(origin, source)
                if source_volume is None:
                    continue
                source_xml = ET.fromstring(source_volume.XMLDesc(0))
                if source_xml.find('backingStore/path') is not None:
                    raise ValueError('Migrated template images must be flattened before transfer')
                format_node = source_xml.find('target/format')
                root = self._volume_xml(filename, source_volume.info()[1],
                                        format_node.get('type', 'raw') if format_node is not None else 'raw')
                target = pool.createXML(_xml(root), 0)
                stream = origin.newStream(0)
                try:
                    with tempfile.TemporaryFile() as image:
                        source_volume.download(stream, 0, 0, 0)
                        stream.recvAll(lambda _stream, data, file: file.write(data), image)
                        stream.finish()
                        size = image.tell()
                        image.seek(0)
                        self._upload(connection, target, image, size)
                    return target
                except BaseException:
                    stream.abort()
                    raise
        raise ValueError(f'Image {source} was not found on the group hosts')

    def provision(self, instance: Instance, group: AutoscalingGroup) -> None:
        """
        Clone disks, attach cloud-init media, define and start a managed VM.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.
            group (AutoscalingGroup): Node group configuration or identifier.

        Returns:
            None: No value is returned.

        Raises:
            ValueError: If the template, storage, available capacity, or reserved cloud-init disk target is invalid.
            RuntimeError: If libvirt fails to define the cloned domain.
        """
        with self.connect(instance.host) as connection:
            existing = self._domain(connection, instance.id)
            if existing is not None:
                self._recover_volumes(self._owned(existing, instance), instance)
                existing.setAutostart(1)
                if not existing.isActive():
                    existing.create()
                return
            root = self._template(connection, group)
            root.attrib.pop('id', None)
            for tag in ('uuid', 'genid', 'metadata'):
                for element in root.findall(tag):
                    root.remove(element)
            domain_name = root.find('name')
            if domain_name is None:
                raise ValueError('Template domain name is missing')
            domain_name.text = instance.name
            ET.SubElement(root, 'uuid').text = instance.id
            metadata = ET.SubElement(root, 'metadata')
            mark = ET.SubElement(metadata, f'{{{NAMESPACE}}}instance', cluster=self.cluster, group=instance.group,
                                 id=instance.id, address=instance.address)
            # Let libvirt allocate private NVRAM instead of inheriting the template's.
            os_config = root.find('os')
            if os_config is not None:
                for nvram in os_config.findall('nvram'):
                    os_config.remove(nvram)
            devices = root.find('devices')
            assert devices is not None
            disks = devices.findall("disk[@device='disk']")
            first_source = disks[0].find('source')
            if first_source is None or not first_source.get('file'):
                raise ValueError('Template disk must be a file in a libvirt storage pool')
            template_volume = connection.storageVolLookupByPath(first_source.get('file'))
            pool = template_volume.storagePoolLookupByVolume()
            # Do not start a clone if the host cannot accommodate its memory.
            memory = root.find('memory')
            if memory is not None and memory.text:
                unit = memory.get('unit', 'KiB').lower()
                factor = {'b': 1, 'bytes': 1, 'kb': 1000, 'kib': 1024, 'mb': 1000**2,
                          'mib': 1024**2, 'gb': 1000**3, 'gib': 1024**3}[unit]
                if connection.getFreeMemory() < int(memory.text) * factor:
                    raise RuntimeError('Host has insufficient free memory for the template')
            for index, disk in enumerate(disks):
                source = disk.find('source')
                if disk.get('type') != 'file' or source is None or not source.get('file'):
                    raise ValueError('All cloned disks must be managed file-backed volumes')
                path = group.image if index == 0 else source.attrib['file']
                volume = self._clone(connection, pool, path, f'premiscale-{instance.id}-{index}.img', instance, group)
                ET.SubElement(mark, f'{{{NAMESPACE}}}volume', path=volume.path())
                source.attrib.clear()
                source.set('file', volume.path())
                volume_format = ET.fromstring(volume.XMLDesc(0)).find('target/format')
                disk_driver = disk.find('driver')
                if disk_driver is None:
                    disk_driver = ET.SubElement(disk, 'driver', name='qemu')
                disk_driver.set('type', volume_format.get('type', 'raw') if volume_format is not None else 'raw')
                for tag in ('serial', 'wwn', 'backingStore'):
                    for element in disk.findall(tag):
                        disk.remove(element)
            interfaces = devices.findall('interface')
            if not interfaces:
                raise ValueError('Template requires a network interface')
            for index, interface in enumerate(interfaces):
                mac = interface.find('mac')
                if mac is None:
                    mac = ET.SubElement(interface, 'mac')
                mac.set('address', mac_address(instance.id, index))
                for target in interface.findall('target'):
                    interface.remove(target)
            for graphics in devices.findall('graphics'):
                if graphics.get('type') in {'vnc', 'spice'}:
                    graphics.set('autoport', 'yes')
                    graphics.set('port', '-1')
                    graphics.attrib.pop('tlsPort', None)
            for cdrom in devices.findall("disk[@device='cdrom']"):
                devices.remove(cdrom)
            content = seed(instance, group, mac_address(instance.id))
            if any(target.get('dev') == 'vdz' for target in devices.findall('disk/target')):
                raise ValueError('The vdz disk target is reserved for cloud-init media')
            seed_name = f'premiscale-{instance.id}-seed.iso'
            pool_path = ET.fromstring(pool.XMLDesc(0)).findtext('target/path')
            if not pool_path:
                raise ValueError('Cloud-init media requires a filesystem-backed storage pool')
            seed_path = str(PurePosixPath(pool_path) / seed_name)
            old_seed = self._volume(connection, seed_path)
            self._claim(instance, seed_path, existing=old_seed is not None)
            if old_seed is not None:
                old_seed.delete(0)
            seed_volume = pool.createXML(_xml(self._volume_xml(seed_name, len(content), 'raw')), 0)
            self._upload(connection, seed_volume, BytesIO(content), len(content))
            ET.SubElement(mark, f'{{{NAMESPACE}}}volume', path=seed_volume.path())
            disk = ET.SubElement(devices, 'disk', type='file', device='disk')
            ET.SubElement(disk, 'driver', name='qemu', type='raw')
            ET.SubElement(disk, 'source', file=seed_volume.path())
            ET.SubElement(disk, 'target', dev='vdz', bus='virtio')
            ET.SubElement(disk, 'readonly')
            domain = connection.defineXML(_xml(root))
            if domain is None:
                raise RuntimeError('libvirt did not define the cloned domain')
            domain.setAutostart(1)
            domain.create()

    def delete(self, instance: Instance) -> None:
        """
        Remove one owned VM and only disks recorded for its exact identity.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.
        """
        with self.connect(instance.host) as connection:
            domain = self._domain(connection, instance.id)
            if domain is not None:
                self._recover_volumes(self._owned(domain, instance), instance)
                if domain.isActive():
                    domain.destroy()
                domain.undefineFlags(libvirt.VIR_DOMAIN_UNDEFINE_NVRAM | libvirt.VIR_DOMAIN_UNDEFINE_MANAGED_SAVE)
            with self._volumes() as journal:
                paths = journal.execute('libvirt/list_instance_volumes.sql',
                                        (self.cluster, instance.id, instance.host)).fetchall()
            for (path,) in paths:
                volume = self._volume(connection, path)
                if volume is not None:
                    volume.delete(0)
                with self._volumes() as journal:
                    journal.execute('libvirt/delete_volume.sql',
                                    (self.cluster, instance.id, instance.host, path))

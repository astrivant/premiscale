"""
Build per-node NoCloud media without invoking shell commands.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from io import BytesIO, StringIO
from ipaddress import ip_network
from pathlib import Path

import pycdlib
from ruamel.yaml import YAML


if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import AutoscalingGroup
    from premiscale_cluster_autoscaler.state import Instance


def _dump_yaml(value: dict) -> str:
    """
    Serialize cloud-init metadata using the shared YAML library.

    Args:
        value (dict): Metadata or network configuration mapping.

    Returns:
        str: YAML document suitable for a NoCloud seed file.
    """
    stream = StringIO()
    yaml = YAML(typ='safe')
    yaml.default_flow_style = False
    yaml.dump(value, stream)
    return stream.getvalue()


def seed(instance: Instance, group: AutoscalingGroup, mac: str) -> bytes:
    """
    Render bootstrap data and optional networking into a CIDATA ISO.

    Args:
        instance (Instance): Managed VM identity and requested lifecycle state.
        group (AutoscalingGroup): Node group configuration or identifier.
        mac (str): MAC address assigned to the primary network interface.

    Returns:
        bytes: CIDATA ISO image containing cloud-init user-data, metadata, and optional networking.
    """
    userdata = group.cloudInit.inline or Path(group.cloudInit.file).expanduser().read_text()
    for key, value in {
        '${PREMISCALE_NODE_NAME}': instance.name,
        '${PREMISCALE_PROVIDER_ID}': instance.provider_id,
        '${PREMISCALE_NODE_GROUP}': instance.group,
    }.items():
        userdata = userdata.replace(key, value)
    files = {
        'user-data': userdata,
        'meta-data': _dump_yaml({'instance-id': instance.id, 'local-hostname': instance.name}),
    }
    network = group.networking
    if network.type != 'ignore':
        interface: dict = {'match': {'macaddress': mac}}
        if network.type == 'dynamic':
            interface['dhcp4'] = True
        else:
            subnet = ip_network(network.subnet, strict=False)
            interface['addresses'] = [f'{instance.address}/{subnet.prefixlen}']
            interface['routes'] = [{'to': 'default', 'via': network.gateway}]
        files['network-config'] = _dump_yaml({'version': 2, 'ethernets': {'primary': interface}})
    iso = pycdlib.PyCdlib()
    iso.new(interchange_level=3, joliet=3, rock_ridge='1.09', vol_ident='cidata')
    streams = []
    try:
        for index, (name, value) in enumerate(files.items()):
            content = value.encode()
            stream = BytesIO(content)
            streams.append(stream)
            iso.add_fp(stream, len(content), iso_path=f'/DATA{index}.;1', rr_name=name, joliet_path=f'/{name}')
        result = BytesIO()
        iso.write_fp(result)
        return result.getvalue()
    finally:
        iso.close()


def mac_address(identity: str, index: int = 0) -> str:
    """
    Derive a stable locally administered MAC for each cloned interface.

    Args:
        identity (str): Stable identifier of the managed resource.
        index (int): Interface index used to derive a distinct MAC address.

    Returns:
        str: Stable locally administered MAC address for this instance and interface index.
    """
    import hashlib

    octets = b'\x02' + hashlib.sha256(f'{identity}/{index}'.encode()).digest()[:5]
    return ':'.join(f'{octet:02x}' for octet in octets)

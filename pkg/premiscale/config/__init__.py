"""
This module contains methods, schemas and classes for parsing and validating configuration files.
"""


from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Type


def build_config_from_version(version: str) -> Type:
    """
    Build a Config object from a given version.

    Args:
        version (str): Controller version advertised during platform registration.

    Returns:
        Type: A Config object from a given version.

    Raises:
        ValueError: If the requested configuration version is unsupported.
    """
    match version:
        case 'v1alpha1':
            from premiscale.config.v1alpha1 import Config as ConfigV1Alpha1

            return ConfigV1Alpha1
        case _:
            raise ValueError(f'Unknown config version: {version}')

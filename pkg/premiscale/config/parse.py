"""
Parse a configuration file, or create a default one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
import logging
import json
import sys

from pathlib import Path
from importlib import resources
from cattrs.errors import BaseValidationError
from ruamel.yaml import YAML, YAMLError
from jsonschema import Draft7Validator
from premiscale.config import build_config_from_version

if TYPE_CHECKING:
    from premiscale.config.v1alpha1 import Config


log = logging.getLogger(__name__)


__all__ = [
    'configParse',
    'validateConfig'
]


def configParse(configPath: str) -> Config:
    """
    Parse a config file and return it as a Config-object. If the file does not exist, create a default one.

    Args:
        configPath (str): path to the config file.

    Returns:
        Config: The parsed config file.

    Raises:
        ValueError: Config file must contain a YAML mapping.
    """
    # Drop a default config for parsing if one was not provided by the user.
    if not Path(configPath).exists():
        makeDefaultConfig(configPath)

    try:
        with open(configPath, 'r', encoding='utf-8') as f:
            _loaded_config = YAML(typ='safe').load(f)

        if not isinstance(_loaded_config, dict):
            raise ValueError('Config file must contain a YAML mapping')

        # Validate the config file against the schema.
        if 'version' not in _loaded_config:
            raise ValueError('Config file is missing a version field')

        if not validateConfig(configPath, version=_loaded_config['version']):
            sys.exit(1)

        # Parse the config into a Config object, now.
        _config = build_config_from_version(_loaded_config['version']).from_dict(_loaded_config)
    except (YAMLError, OSError, KeyError, ValueError, TypeError, BaseValidationError) as e:
        log.error(f'Error parsing config file: {e}')
        sys.exit(1)

    log.debug('Successfully parsed config version %s', _config.version)

    return _config


def validateConfig(configPath: str, version: str = 'v1alpha1', strict: bool = True) -> bool:
    """
    Validate users' config files against our schema.

    Args:
        configPath (str): path to a config file path/name to validate against the schema.
        version (str): the version of the config file to validate. (default: 'v1alpha1')
        strict (bool): Whether to reject fields absent from the generated configuration schema.

    Returns:
        bool: True if the config file is valid.

    Raises:
        ValueError: Config file must contain exactly one YAML mapping.
    """
    if not Path(configPath).exists():
        makeDefaultConfig(configPath)
    else:
        log.debug(f'Found config file at {configPath}')

    try:
        # Only supported versions may select a packaged schema.
        build_config_from_version(version)
        schema_text = resources.files('premiscale.config.schemas').joinpath(
            f'schema.{version}.json'
        ).read_text(encoding='utf-8')
        schema = json.loads(schema_text)
        if not strict:
            _allow_unknown_fields(schema)
        data = YAML(typ='safe').load(Path(configPath))
        if not isinstance(data, dict):
            raise ValueError('Config file must contain exactly one YAML mapping')
        # YAML admits nonfinite numbers that JSON and Kubernetes cannot represent.
        json.dumps(data, allow_nan=False)
        error = next(Draft7Validator(schema).iter_errors(data), None)
        if error is not None:
            log.error('Configuration violates %s at %s', error.validator, error.json_path)
            return False

        log.info(f'Config file at {configPath} is valid')
    except OSError as e:
        log.error(f'Could not read config or schema file: {e}')
        return False
    except (ValueError, TypeError, YAMLError) as e:
        log.error(f'Error validating config file: {e}')
        return False

    return True


def _allow_unknown_fields(schema: dict) -> None:
    """
    Relax closed objects for callers that explicitly disable strict validation.

    Args:
        schema (dict): Freshly loaded schema to adjust recursively in place.

    Returns:
        None: No value is returned.
    """
    if schema.get('additionalProperties') is False:
        schema['additionalProperties'] = True
    for value in schema.values():
        if isinstance(value, dict):
            _allow_unknown_fields(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _allow_unknown_fields(item)


def makeDefaultConfig(path: str | Path, default_config: str | Path = 'default.yaml') -> None:
    """
    Make a default config file if one does not exist.

    Args:
        path (str | Path): The default location to create an autoscale configuration file, if it doesn't exist.
        default_config (str | Path): The default config file to use when creating a new config file. (default: 'default.yaml')

    Returns:
        None: No value is returned.
    """
    try:
        if not Path(path).parent.exists():
            Path(path).parent.mkdir(parents=True)

        if not Path(path).exists():
            log.debug(f'Config file at {path} does not exist. Creating default config file')

            with resources.open_text('premiscale.config', str(default_config)) as default_config_f, open(str(path), 'x', encoding='utf-8') as f:
                f.write(default_config_f.read().strip())

            log.debug(f'Successfully created default config file at \'{str(path)}\'')
    except PermissionError:
        log.error(f'premiscale does not have permission to install to {str(Path(path).parent)}')
        sys.exit(1)

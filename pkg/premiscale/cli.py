"""
The PremiScale autoscaling controller for Kubernetes.
"""


from __future__ import annotations

import sys
import logging
import os
import json

from pathlib import Path
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from premiscale.config.parse import validateConfig, configParse
from premiscale.daemon import start
from premiscale.utils import LogLevel
from premiscale import env, version
from premiscale.daemon.settings import Execution


log = logging.getLogger(__name__)


def main() -> None:
    """
    Set up the CLI for PremiScale autoscaler.

    Returns:
        None: No value is returned.
    """
    parser = ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description=__doc__,
        epilog='For more information, visit https://premiscale.com.\n\n© PremiScale, Inc. 2024.'
    )

    parser.add_argument(
        '--token', type=str, default=env['PREMISCALE_TOKEN'],
        help='Platform registration token. Also available as the environment variable \'PREMISCALE_TOKEN\'. If no token is provided, the controller will not register with the platform and start in standalone mode.'
    )

    parser.add_argument(
        '-c', '--config', type=str, default=env['PREMISCALE_CONFIG_PATH'],
        help='Configuration file path to use. Also available as the environment variable \'PREMISCALE_CONFIG_PATH\'. (default: /opt/premiscale/config.yaml)'
    )

    parser.add_argument(
        '--validate', action='store_true',
        help='Validate the provided configuration file and exit. (default: false)'
    )

    parser.add_argument(
        '--version', action='store_true',
        help='Display controller version.'
    )

    parser.add_argument(
        '--log-level', default=env['PREMISCALE_LOG_LEVEL'], choices=list(LogLevel), type=LogLevel.from_string,
        help='Set the logging level. Also available as the environment variable \'PREMISCALE_LOG_LEVEL\'. (default: info)'
    )

    log_group = parser.add_mutually_exclusive_group()

    log_group.add_argument(
        '--log-file', type=str, default='/opt/premiscale/controller.log',
        help='Specify the file the service logs to if --log-stdout is not set. Also available as the environment variable \'PREMISCALE_LOG_FILE\'. (default: /opt/premiscale/controller.log)'
    )

    log_group.add_argument(
        '--log-stdout', action='store_true',
        help='Log to stdout (for use in containerized deployments). (default: false)'
    )

    parser.add_argument('--role', choices=['controller', 'collector', 'publisher'],
                        default=os.getenv('PREMISCALE_ROLE', 'controller'), help='Deployment process role.')
    parser.add_argument('--deployment-mode', choices=['singular', 'ha', 'hha'],
                        default=os.getenv('PREMISCALE_DEPLOYMENT_MODE', 'singular'),
                        help='Singular container, consolidated HA replicas, or separate HHA workloads.')
    parser.add_argument('--remote-publishers', default=os.getenv('PREMISCALE_REMOTE_PUBLISHERS', ''),
                        help='Comma-separated database subscribers delegated by the controller.')
    parser.add_argument('--publisher', default=os.getenv('PREMISCALE_PUBLISHER', ''),
                        help='Database subscriber consumed by this publisher deployment.')
    parser.add_argument('--publisher-connections', type=int,
                        default=os.getenv('PREMISCALE_PUBLISHER_CONNECTIONS', '2'),
                        help='Maximum database connections in a publisher pod.')
    parser.add_argument('--publisher-limits', type=json.loads,
                        default=os.getenv('PREMISCALE_PUBLISHER_LIMITS', '{}'),
                        help='JSON map of subscriber names to per-pod connection limits.')

    args = parser.parse_args()

    if args.version:
        print(f'premiscale v{version}')
        sys.exit(0)

    # Configure logger
    if args.log_stdout:
        logging.basicConfig(
            stream=sys.stdout,
            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            level=args.log_level.value
        )
    else:
        try:
            # Instantiate log path (when logging locally).
            if not Path(args.log_file).exists():
                Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)

            logging.basicConfig(
                filename=args.log_file,
                format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
                level=args.log_level.value,
                filemode='a'
            )
        except (FileNotFoundError, PermissionError) as msg:
            log.error(f'Failed to configure logging, received: {msg}')
            sys.exit(1)

    if args.validate:
        sys.exit(0 if validateConfig(args.config) else 1)

    log.info(f'Starting PremiScale controller v{version}')

    if (token := args.token) != '':
        log.info('Using provided platform token for registration')
    else:
        token = ''

    # Start the premiscale controller.
    sys.exit(
        start(
            config=configParse(args.config),
            version=version,
            token=token,
            execution=Execution(role=args.role, mode=args.deployment_mode,
                                publishers=tuple(name for name in args.remote_publishers.split(',') if name),
                                publisher=args.publisher, connections=args.publisher_connections,
                                publisher_limits=args.publisher_limits),
        )
    )

"""
Durable node identities and pending lifecycle operations.
"""

from dataclasses import dataclass
from premiscale.connections.journal import Journal


@dataclass(frozen=True)
class Instance:
    """
    One VM and the operation currently requested for it.

    Attributes:
        id (str): Stable instance or delivery identifier.
        name (str): Configured resource name.
        group (str): Owning node-group name.
        host (str): Configured host name or bind address.
        address (str): Host or service address.
        phase (str): Current or requested lifecycle phase.
        error (str): Last lifecycle failure description, or an empty string.
    """

    id: str
    name: str
    group: str
    host: str
    address: str = ''
    phase: str = 'queued'
    error: str = ''

    @property
    def provider_id(self) -> str:
        """
        Return the identity configured on the node's kubelet.

        Returns:
            str: The identity configured on the node's kubelet.
        """
        return f'premiscale://{self.id}'


class State:
    """
    Persist operations; callers serialize access with the provider lock.
    """

    def __init__(self, path: str, cluster: str, dsn: str = '') -> None:
        """
        Initialize State with the supplied settings.

        Args:
            path (str): Filesystem path to the requested resource.
            cluster (str): Cluster identity allowed to own this journal.
            dsn (str): Optional PostgreSQL connection string for shared durable state.

        Raises:
            ValueError: The operation journal belongs to a different clusterName.
        """
        self.connection = Journal(path, cluster, dsn, mappings=True)
        if not dsn:
            self.connection.execute('autoscaler/enable_wal.sql')
            self.connection.execute('autoscaler/enable_full_sync.sql')
        with self.connection.transaction():
            self.connection.execute('autoscaler/create_metadata.sql')
            self.connection.execute('autoscaler/create_instances.sql')
            self.connection.execute('autoscaler/claim_cluster.sql', (cluster,))
        owner = self.connection.execute('autoscaler/get_cluster.sql').fetchone()
        if owner is not None and owner['value'] != cluster:
            self.connection.close()
            raise ValueError('The operation journal belongs to a different clusterName')

    def instances(self, group: str | None = None) -> list[Instance]:
        """
        Read all instances, optionally limiting the result to a node group.

        Args:
            group (str | None): Node group configuration or identifier.

        Returns:
            list[Instance]: All instances, optionally limiting the result to a node group.
        """
        if group is None:
            rows = self.connection.execute('autoscaler/list_instances.sql')
        else:
            rows = self.connection.execute('autoscaler/list_group_instances.sql', (group,))
        return [Instance(**dict(row)) for row in rows]

    def put(self, instance: Instance) -> None:
        """
        Insert or update an instance in the caller's transaction.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.
        """
        self.connection.execute(
            'autoscaler/put_instance.sql',
            (instance.id, instance.name, instance.group, instance.host, instance.address, instance.phase, instance.error),
        )

    def remove(self, instance: Instance) -> None:
        """
        Remove a cancelled reservation or successfully deleted VM.

        Args:
            instance (Instance): Managed VM identity and requested lifecycle state.

        Returns:
            None: No value is returned.
        """
        self.connection.execute('autoscaler/delete_instance.sql', (instance.id,))

    def close(self) -> None:
        """
        Close the operation journal.

        Returns:
            None: No value is returned.
        """
        self.connection.close()

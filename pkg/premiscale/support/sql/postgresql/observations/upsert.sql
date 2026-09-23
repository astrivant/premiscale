INSERT INTO vm_observations (
    cluster, id, name, group_name, host, address, state, reason,
    vcpus, memory_bytes, storage_bytes, observed_at, changed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (cluster, id) DO UPDATE SET
    name = excluded.name,
    group_name = excluded.group_name,
    host = excluded.host,
    address = excluded.address,
    state = COALESCE(excluded.state, vm_observations.state),
    reason = COALESCE(excluded.reason, vm_observations.reason),
    vcpus = COALESCE(excluded.vcpus, vm_observations.vcpus),
    memory_bytes = COALESCE(excluded.memory_bytes, vm_observations.memory_bytes),
    storage_bytes = COALESCE(excluded.storage_bytes, vm_observations.storage_bytes),
    observed_at = excluded.observed_at,
    changed_at = CASE WHEN
        (vm_observations.name, vm_observations.group_name, vm_observations.host,
         vm_observations.address, vm_observations.state, vm_observations.reason,
         vm_observations.vcpus, vm_observations.memory_bytes, vm_observations.storage_bytes)
        IS DISTINCT FROM
        (excluded.name, excluded.group_name, excluded.host, excluded.address,
         COALESCE(excluded.state, vm_observations.state),
         COALESCE(excluded.reason, vm_observations.reason),
         COALESCE(excluded.vcpus, vm_observations.vcpus),
         COALESCE(excluded.memory_bytes, vm_observations.memory_bytes),
         COALESCE(excluded.storage_bytes, vm_observations.storage_bytes))
        THEN excluded.observed_at ELSE vm_observations.changed_at END,
    revision = vm_observations.revision + CASE WHEN
        (vm_observations.name, vm_observations.group_name, vm_observations.host,
         vm_observations.address, vm_observations.state, vm_observations.reason,
         vm_observations.vcpus, vm_observations.memory_bytes, vm_observations.storage_bytes)
        IS DISTINCT FROM
        (excluded.name, excluded.group_name, excluded.host, excluded.address,
         COALESCE(excluded.state, vm_observations.state),
         COALESCE(excluded.reason, vm_observations.reason),
         COALESCE(excluded.vcpus, vm_observations.vcpus),
         COALESCE(excluded.memory_bytes, vm_observations.memory_bytes),
         COALESCE(excluded.storage_bytes, vm_observations.storage_bytes))
        THEN 1 ELSE 0 END
WHERE excluded.observed_at > vm_observations.observed_at

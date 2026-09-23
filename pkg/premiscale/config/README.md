# Database configuration

Database settings are concrete attrs instances from [databases.py](databases.py).
The YAML `type` fields select these classes during parsing through cattrs' literal
discriminators:

| Configuration | YAML type | Python settings |
| --- | --- | --- |
| State | `memory` | `SQLiteState` |
| State | `mysql` | `MySQLState` |
| Primary time series or named publisher | `memory` | `LocalMetrics` |
| Named publisher | `postgresql` | `PostgreSQLMetrics` |
| Named publisher | `influxdb` | `InfluxDBMetrics` |

Python callers can supply settings instances directly:

```python
from premiscale.config.databases import SQLiteState, LocalMetrics, PostgreSQLMetrics

state = SQLiteState(dbfile="/var/lib/premiscale/state.db")
metrics = LocalMetrics(dbfile="/var/lib/premiscale/metrics.csv", retention=3600)
archive = PostgreSQLMetrics(dsn="${PREMISCALE_POSTGRES_DSN}", retention=86400)

with state.adapter() as connection:
    connection.initialize()

publisher = archive.publisher()
```

`adapter()` constructs a fresh unopened adapter; its context manager opens and
closes the connection. `publisher()` constructs a fresh publisher whose first
delivery opens the database. The publishing worker closes it during cleanup.
Settings remain serializable and hold no live database clients, so they can be
passed to spawned subprocesses; create the adapters inside the process that owns
their connections.

`databases.destinations` maps subscriber names to their settings, including the
same `databases.timeseries` instance under `primary`. Collection and publication
therefore share destination configuration without sharing connection objects.
MySQL settings preserve the existing nested `connection` YAML format; the MySQL
state adapter's operations remain unimplemented.

See the [collection and publication pipeline](../metrics/README.md) for verified VM acquisition, independent state observations, and the optional InfluxDB 2.x publisher.

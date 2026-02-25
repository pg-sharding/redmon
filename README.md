# REDMON - SPQR Redistribution Monitor

Automated monitoring and key range redistribution across SPQR shards.

## Features

- Monitor task group status via psql
- Retry failed task groups with specific retryable errors (optional)
- Redistribute key ranges from shard0 to target shards based on UUID prefix
- Background execution for long-running operations with logs in `/var/log/spqr/`
- Rotating file logging (10MB, 5 backups) + console output
- Dry-run mode for testing

## Quick Start

```bash
# Dry-run mode
python3 spqr_monitor.py --dry-run

# Production with error retry enabled
python3 spqr_monitor.py --retry-errors --log-file ~/logs/spqr_monitor.log
```

## Options

```
--db-host HOST                  Database host (default: localhost)
--db-port PORT                  Database port (default: 6432)
--db-name NAME                  Database name (default: spqr-console)
--db-user USER                  Database user (default: spqr-console)
--iteration-timeout SECONDS     Interval between iterations (default: 60)
--log-file PATH                 Log file path (default: ./spqr_monitor.log)
--dry-run                       Print commands without executing
--retry-errors                  Enable retry of failed task groups (disabled by default)
--max-failed-tasks N            Skip iteration if failed tasks > N (default: 10)
--max-running-tasks N           Skip redistribution if running tasks >= N (default: 8)
--max-retries-per-iteration N   Max task groups to retry per iteration (default: 1)
```

## How it works

Each iteration:

1. Skip if database is read-only
2. Skip if failed tasks exceed `--max-failed-tasks` threshold
3. If `--retry-errors`: retry task groups with retryable errors (etcd timeouts, grpc errors)
4. Skip redistribution if running tasks >= `--max-running-tasks`
5. Find random key range on shard0 matching `ds_user_id_kr_*`
6. Determine target shard by UUID first hex digit: 0-1→shard-001, 2-3→shard-002, ..., e-f→shard-008
7. Execute `REDISTRIBUTE KEY RANGE '...' TO '...' BATCH SIZE 300000`

## Testing

```bash
python3 -m unittest discover -s . -p "test_*.py" -v
```

## Requirements

- Python 3.10+
- psql
